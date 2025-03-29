package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"github.com/aldas/go-modbus-client"
	"github.com/aldas/go-modbus-client/packet"
	"github.com/aldas/go-modbus-client/poller"

	//"strings"
	//"sync"
	"fmt"
	"time"
)

func main() {
	b := modbus.NewRequestBuilderWithConfig(modbus.BuilderDefaults{
		ServerAddress: "tcp://172.25.28.29:8899?max_quantity_per_request=16",
		FunctionCode:  packet.FunctionReadHoldingRegisters,
		UnitID:        1,
		Protocol:      modbus.ProtocolTCP,
		Interval:      modbus.Duration(5 * time.Second),
	})

	type Register struct {
		Name    string
		Type    modbus.FieldType
		Address uint16
	}
	regs := []Register{
		{"Main_l1_voltage", modbus.FieldTypeInt32, 10240},
		{"Main_l2_voltage", modbus.FieldTypeInt32, 10242},
		{"Main_l3_voltage", modbus.FieldTypeInt32, 10244},
		{"Genset_l1_voltage", modbus.FieldTypeInt32, 10246},
		{"Genset_l2_voltage", modbus.FieldTypeInt32, 10248},
		{"Genset_l3_voltage", modbus.FieldTypeInt32, 10250},
		{"Genset_l1_current", modbus.FieldTypeInt32, 10270},
		{"Genset_l2_current", modbus.FieldTypeInt32, 10272},
		{"Genset_l3_current", modbus.FieldTypeInt32, 10274},
		{"Genset_total_current", modbus.FieldTypeInt32, 10278},
		{"Genset_l1_active_power", modbus.FieldTypeInt32, 10286},
		{"Genset_l2_active_power", modbus.FieldTypeInt32, 10288},
		{"Genset_l3_active_power", modbus.FieldTypeInt32, 10290},
		{"Genset_total_active_power", modbus.FieldTypeInt32, 10294},
		{"Genset_l1_apparent_power", modbus.FieldTypeInt32, 10318},
		{"Genset_l2_apparent_power", modbus.FieldTypeInt32, 10320},
		{"Genset_l3_apparent_power", modbus.FieldTypeInt32, 10322},
		{"Genset_total_apparent_power", modbus.FieldTypeInt32, 10326},
		{"Main_frequency", modbus.FieldTypeUint16, 10338},
		{"Genset_frequency", modbus.FieldTypeUint16, 10339},
		{"Charge_input_voltage", modbus.FieldTypeUint16, 10340},
		{"Battery_voltage", modbus.FieldTypeUint16, 10341},
		{"Oil_presure", modbus.FieldTypeUint16, 10361},
		{"Engine_temp", modbus.FieldTypeUint16, 10362},
		{"Engine_rpm", modbus.FieldTypeUint16, 10376},
		{"Unit_oper_status", modbus.FieldTypeUint16, 10604},
		{"Unit_mode", modbus.FieldTypeUint16, 10605},
		{"Count_genset_cranks", modbus.FieldTypeInt32, 10618},
		{"Count_engine_hours_run", modbus.FieldTypeInt32, 10622},
	}
	for _, reg := range regs {
		b.AddField(modbus.Field{
			Name:    reg.Name,
			Address: reg.Address,
			Type:    reg.Type,
		})
	}
	batches, _ := b.Split()

	for i, batch := range batches {
		slog.Info("Batch",
			"index", i,
			"StartAddress", batch.StartAddress,
			"Quantity", batch.Quantity,
		)
		for j, field := range batch.Fields {
			slog.Info("--- field",
				"field_index", j,
				"Name", field.Name,
				"Address", field.Address,
			)
		}
	}

	doPolling(batches)
}

func doPolling(batches []modbus.BuilderRequest) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	config := poller.Config{
		ConnectFunc: poller.NewSingleConnectionPerAddressClientFunc(poller.DefaultConnectClient),
	}
	p := poller.NewPollerWithConfig(batches, config)

	go func() {
		for {
			select {
			case result := <-p.ResultChan:
				//slog.Info("polled values", "values", result)
				for _, val := range result.Values {
					if val.Error != nil {
						slog.Info(fmt.Sprintf("%s = %v [error: %v]", val.Field.Name, val.Value, val.Error))
					} else {
						slog.Info(fmt.Sprintf("%s = %v", val.Field.Name, val.Value))
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		tickerHealth := time.NewTicker(60 * time.Second)
		defer tickerHealth.Stop()
		for {
			select {
			case <-tickerHealth.C:
				for _, s := range p.BatchStatistics() {
					b := batches[s.BatchIndex]
					slog.Info(
						"batch health check",
						"index", s.BatchIndex,
						"address", s.ServerAddress,
						"function_code", s.FunctionCode,
						"start_address", b.StartAddress,
						"quantity", b.Quantity,
						"request_interval", b.RequestInterval.String(),
						"is_polling", s.IsPolling,
						"start_count", s.StartCount,
						"request_ok_count", s.RequestOKCount,
						"request_err_count", s.RequestErrCount,
						"request_modbus_err_count", s.RequestModbusErrCount,
						"send_skip_count", s.SendSkipCount,
					)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	if err := p.Poll(ctx); err != nil {
		slog.Error("polling ended with failure", "err", err)
		return
	}
}
