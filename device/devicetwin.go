package device

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	dmiapi "github.com/kubeedge/api/apis/dmi/v1beta1"
	"github.com/kubeedge/mapper-framework/pkg/common"
	"github.com/kubeedge/mapper-framework/pkg/grpcclient"
	"github.com/kubeedge/mapper-framework/pkg/util/parse"
	"github.com/kubeedge/mqtt-iot-mapper/driver"
)

// TwinData holds per-property reporting context.
type TwinData struct {
	DeviceName      string
	DeviceNamespace string
	Client          *driver.CustomizedClient
	Name            string
	Type            string
	ObservedDesired common.TwinProperty
	VisitorConfig   *driver.VisitorConfig
	Topic           string
	Results         interface{}
	CollectCycle    time.Duration
	ReportToCloud   bool
}

// GetPayLoad reads the property value from the driver and serialises it into a twin-update payload.
func (td *TwinData) GetPayLoad() ([]byte, error) {
	var err error
	td.Results, err = td.Client.GetDeviceData(td.VisitorConfig)
	if err != nil {
		return nil, fmt.Errorf("get device data failed: %v", err)
	}
	sData, err := common.ConvertToString(td.Results)
	if err != nil {
		klog.Errorf("Failed to convert %s %s value as string : %v", td.DeviceName, td.Name, err)
		return nil, err
	}
	if len(sData) > 30 {
		klog.V(4).Infof("Get %s : %s ,value is %s......", td.DeviceName, td.Name, sData[:30])
	} else {
		klog.V(4).Infof("Get %s : %s ,value is %s", td.DeviceName, td.Name, sData)
	}
	var payload []byte
	if strings.Contains(td.Topic, "$hw") {
		if payload, err = common.CreateMessageTwinUpdate(td.Name, td.Type, sData, td.ObservedDesired.Value); err != nil {
			return nil, fmt.Errorf("create message twin update failed: %v", err)
		}
	} else {
		if payload, err = common.CreateMessageData(td.Name, td.Type, sData); err != nil {
			return nil, fmt.Errorf("create message data failed: %v", err)
		}
	}
	return payload, nil
}

// PushToEdgeCore reads the property and reports it to EdgeCore via DMI gRPC.
func (td *TwinData) PushToEdgeCore() {
	payload, err := td.GetPayLoad()
	if err != nil {
		klog.Errorf("twindata %s unmarshal failed, err: %s", td.Name, err)
		return
	}

	var msg common.DeviceTwinUpdate
	if err = json.Unmarshal(payload, &msg); err != nil {
		klog.Errorf("twindata %s unmarshal failed, err: %s", td.Name, err)
		return
	}

	twins := parse.ConvMsgTwinToGrpc(msg.Twin)

	rdsr := &dmiapi.ReportDeviceStatusRequest{
		DeviceName:      td.DeviceName,
		DeviceNamespace: td.DeviceNamespace,
		ReportedDevice: &dmiapi.DeviceStatus{
			Twins: twins,
		},
	}

	if err := grpcclient.ReportDeviceStatus(rdsr); err != nil {
		klog.Errorf("fail to report device status of %s with err: %+v", rdsr.DeviceName, err)
	}
}

// Run starts the collect-and-report loop for this twin property.
// Deprecated: use DeviceReporter.Run instead, which batches all twin updates
// for a device into a single ReportDeviceStatus gRPC call.
func (td *TwinData) Run(ctx context.Context) {
	if !td.ReportToCloud {
		return
	}
	if td.CollectCycle == 0 {
		td.CollectCycle = common.DefaultCollectCycle
	}
	ticker := time.NewTicker(td.CollectCycle)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			td.PushToEdgeCore()
		case <-ctx.Done():
			return
		}
	}
}

// TwinReporter is the per-twin metadata needed by DeviceReporter.
type TwinReporter struct {
	Name            string
	Type            string
	ObservedDesired common.TwinProperty
	VisitorConfig   *driver.VisitorConfig
	Topic           string
}

// DeviceReporter batches all twin status reports for a single device into one
// ReportDeviceStatus gRPC call, avoiding "too many request" rate limiting from EdgeCore.
type DeviceReporter struct {
	DeviceName      string
	DeviceNamespace string
	Client          *driver.CustomizedClient
	Twins           []TwinReporter
	CollectCycle    time.Duration
}

// NewDeviceReporter creates a DeviceReporter. If all twins share the same CollectCycle
// the first non-zero cycle is used; otherwise DefaultCollectCycle is used.
func NewDeviceReporter(dev *driver.CustomizedDev, twins []TwinReporter, collectCycle time.Duration) *DeviceReporter {
	return &DeviceReporter{
		DeviceName:      dev.Instance.Name,
		DeviceNamespace: dev.Instance.Namespace,
		Client:          dev.CustomizedClient,
		Twins:           twins,
		CollectCycle:    collectCycle,
	}
}

// Run starts the batched collect-and-report loop.
func (dr *DeviceReporter) Run(ctx context.Context) {
	if len(dr.Twins) == 0 {
		return
	}
	if dr.CollectCycle == 0 {
		dr.CollectCycle = common.DefaultCollectCycle
	}
	ticker := time.NewTicker(dr.CollectCycle)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			dr.reportAll()
		case <-ctx.Done():
			return
		}
	}
}

// reportAll reads every twin from the device driver and sends them in a single
// ReportDeviceStatus gRPC call.
func (dr *DeviceReporter) reportAll() {
	allTwins := make([]*dmiapi.Twin, 0, len(dr.Twins))

	for _, tr := range dr.Twins {
		td := &TwinData{
			DeviceName:      dr.DeviceName,
			Client:          dr.Client,
			Name:            tr.Name,
			Type:            tr.Type,
			VisitorConfig:   tr.VisitorConfig,
			ObservedDesired: tr.ObservedDesired,
			Topic:           tr.Topic,
		}
		payload, err := td.GetPayLoad()
		if err != nil {
			klog.Errorf("DeviceReporter GetPayLoad %s/%s: %v", dr.DeviceName, tr.Name, err)
			continue
		}
		var msg common.DeviceTwinUpdate
		if err = json.Unmarshal(payload, &msg); err != nil {
			klog.Errorf("DeviceReporter unmarshal %s/%s: %v", dr.DeviceName, tr.Name, err)
			continue
		}
		grpcTwins := parse.ConvMsgTwinToGrpc(msg.Twin)
		allTwins = append(allTwins, grpcTwins...)
	}

	if len(allTwins) == 0 {
		return
	}

	rdsr := &dmiapi.ReportDeviceStatusRequest{
		DeviceName:      dr.DeviceName,
		DeviceNamespace: dr.DeviceNamespace,
		ReportedDevice: &dmiapi.DeviceStatus{
			Twins: allTwins,
		},
	}
	if err := grpcclient.ReportDeviceStatus(rdsr); err != nil {
		klog.Errorf("fail to report device status of %s with err: %+v", dr.DeviceName, err)
	}
}
