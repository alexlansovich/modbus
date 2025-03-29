package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/aldas/go-modbus-client"
	"github.com/aldas/go-modbus-client/packet"
	"github.com/aldas/go-modbus-client/poller"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

type Generator struct {
	GenID           uint16 `json:"genId,string"`
	GenControllerID uint16 `json:"genControllerId,string"`
	GenControllerIP string `json:"genControllerIp"`
}

type Register struct {
	ID            uint16  `json:"id,string"`
	RegisterData  uint16  `json:"register_data,string"`
	RegisterBits  uint16  `json:"register_bits,string"`
	RegisterMulti uint16  `json:"register_multi,string"`
	RegisterName  string  `json:"register_name"`
	RegisterDesc  *string `json:"register_desc"` // used for null valiues
}

var mainLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

var mqttClient mqtt.Client
var pollInterval = 10 * time.Second // interval

func fetchGenerators(apiURL string) ([]Generator, error) {
	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch generators: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var generators []Generator
	if err := json.Unmarshal(body, &generators); err != nil {
		return nil, fmt.Errorf("failed to unmarshal generators: %v", err)
	}

	return generators, nil
}

func filterGeneratorsAndExtractTypes(generators []Generator) ([]Generator, []uint16) {
	filteredGenerators := []Generator{}
	typesMap := make(map[uint16]bool)
	for _, generator := range generators {
		/*if len(filteredGenerators) >= 50 { // limit test for not all gensets
			break
		}*/
		if generator.GenID == 0 || generator.GenControllerID == 0 || generator.GenControllerIP == "" {
			continue
		}
		filteredGenerators = append(filteredGenerators, generator)
		typesMap[generator.GenControllerID] = true
	}

	types := []uint16{}
	for genType := range typesMap {
		types = append(types, genType)
	}

	return filteredGenerators, types
}

func fetchRegistersForTypes(apiURLBase string, types []uint16) (map[uint16]map[uint16]Register, error) {
	// map for register by types. 
	registers := make(map[uint16]map[uint16]Register)

	for _, genType := range types {
		// URL for API
		url := fmt.Sprintf("%s%d", apiURLBase, genType)

		// get answer
		resp, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch registers for type %d: %v", genType, err)
		}
		defer resp.Body.Close()

		// read data
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body for type %d: %v", genType, err)
		}

		// parse JSON to Register
		var typeRegisters []Register
		if err := json.Unmarshal(body, &typeRegisters); err != nil {
			return nil, fmt.Errorf("failed to unmarshal registers for type %d: %v", genType, err)
		}

		// Init internal map
		registers[genType] = make(map[uint16]Register)

		// Add refisters , create index
		for index, reg := range typeRegisters {
			registers[genType][uint16(index)] = reg
		}
	}

	return registers, nil
}

func startPoller(generator Generator, registers map[uint16]Register, wg *sync.WaitGroup) {
	defer wg.Done()
	logger := createLogger(generator.GenID)

	serverAddr := fmt.Sprintf("tcp://%s:8899", generator.GenControllerIP)

	builder := modbus.NewRequestBuilderWithConfig(modbus.BuilderDefaults{
		ServerAddress: serverAddr,
		FunctionCode:  packet.FunctionReadHoldingRegisters,
		UnitID:        1,
		Protocol:      modbus.ProtocolTCP,
		Interval:      modbus.Duration(pollInterval),
	})

	for _, reg := range registers {
		builder.AddField(modbus.Field{
			Name:    reg.RegisterName,
			Address: reg.RegisterData,
			Type:    modbus.FieldTypeUint16,
		})
	}

	batches, err := builder.Split()
	if err != nil {
		logger.Error("Failed to split requests: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	myPoller := poller.NewPollerWithConfig(batches, poller.Config{Logger: logger})

	go func() {
		for {
			select {
			case result := <-myPoller.ResultChan:
				metrics := map[string]interface{}{}
				location, _ := time.LoadLocation("Europe/Kiev")

				allSuccess := true
				for _, v := range result.Values {
					if v.Error != nil {
						logger.Error("Register read error", "field", v.Field.Name, "err", v.Error)
						allSuccess = false
						break
					}
					metrics[v.Field.Name] = v.Value
				}

				if !allSuccess {
					logger.Warn("Skipping MQTT publish due to read errors")
					continue
				}

				metrics["timestamp"] = time.Now().In(location).Format("2006-01-02 15:04:05")

				payload, _ := json.Marshal(metrics)
				topic := fmt.Sprintf("generators/%d/metrics", generator.GenID)

				// add log before MQTT
				logger.Info("Publishing metrics", "generatorID", generator.GenID, "topic", topic)

				if err := publishToMQTT(topic, payload); err != nil {
					logger.Error("MQTT publish error", "generatorID", generator.GenID, "error", err)
				} else {
					logger.Info("Published metrics for generator", "generatorID", generator.GenID)
				}
			case <-ctx.Done():
				logger.Warn("Stopping poller", "generatorID", generator.GenID)
				return
			}
		}
	}()

	logger.Info("Starting poller for generator", "generatorID", generator.GenID)
	if err := myPoller.Poll(ctx); err != nil {
		logger.Error("Poller stopped with error:", "error", err)
	}
}

func initMQTT(broker, clientID, username, password string) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetKeepAlive(30 * time.Second) // timeouts
	opts.SetPingTimeout(10 * time.Second)
	opts.SetAutoReconnect(true) // autoreconnect
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)

	//TLS 
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // test
	}
	opts.SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("MQTT connection error: %v", token.Error())
	}

	return client
}

func publishToMQTT(topic string, payload []byte) error {
	if mqttClient == nil || !mqttClient.IsConnected() {
		log.Println("MQTT client is disconnected, attempting to reconnect...")
		mqttClient = initMQTT("tcp://hidenip:8883", "admin-id", "admin", "password")
	}

	token := mqttClient.Publish(topic, 0, true, payload)
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("MQTT publish error: %v", token.Error())
	}
	return nil
}

// logger
func createLogger(generatorID uint16) *slog.Logger {

	logDir := "/var/log/generators/"

	if err := os.MkdirAll(logDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create log directory %s: %v", logDir, err)
	}
	logFileName := fmt.Sprintf("%sgenerator_%d.log", logDir, generatorID)
	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file for generator %d: %v", generatorID, err)
	}

	handler := slog.NewTextHandler(file, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler)
}

func main() {
	generatorAPI := "https://test.alexlans.com/api/genSets"
	registerAPIBase := "https://test.alexlans.com/api/modbusRegisters/"
  // https://test.alexlans.com/api/modbusRegisters/1 or
  // https://test.alexlans.com/api/modbusRegisters/2

	generators, err := fetchGenerators(generatorAPI)
	if err != nil {
		log.Fatalf("Error fetching generators: %v", err)
	}

	filteredGenerators, types := filterGeneratorsAndExtractTypes(generators)

	registers, err := fetchRegistersForTypes(registerAPIBase, types)
	if err != nil {
		log.Fatalf("Error fetching registers: %v", err)
	}

	//  MQTT
	mqttBroker := "tcp://hiddenip:8883" //  TLS
	clientID := "admin-id"
	username := "admin"
	password := "password"

	// publish
	mqttClient = initMQTT(mqttBroker, clientID, username, password)
	defer mqttClient.Disconnect(250)

	// log
	//fmt.Println("\nAll data ready:")
	mainLogger.Info("Starting Modbus-MQTT service...")
	mainLogger.Info("All Data is ready...")

	var wg sync.WaitGroup

	for _, gen := range filteredGenerators {
		wg.Add(1)
		go startPoller(gen, registers[gen.GenControllerID], &wg)
	}

	wg.Wait() //wait
	//log.Println("All workers stopped.")
	mainLogger.Info("Starting Modbus-MQTT service...")
}
