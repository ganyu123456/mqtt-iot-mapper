package driver

import (
	"fmt"
	"sync"

	"github.com/spf13/cast"
	"k8s.io/klog/v2"

	"github.com/kubeedge/mapper-framework/pkg/common"
)

// NewClient creates a CustomizedClient from a protocol config; called by device/device.go.
func NewClient(protocol ProtocolConfig) (*CustomizedClient, error) {
	return &CustomizedClient{
		ProtocolConfig: protocol,
		deviceMutex:    sync.Mutex{},
	}, nil
}

// InitDevice initialises the IoTGateway and connects to the MQTT broker.
func (c *CustomizedClient) InitDevice() error {
	cfg := c.ProtocolConfig.ConfigData

	if cfg.MQTTBroker == "" {
		cfg.MQTTBroker = "tcp://192.168.122.212:1884"
	}
	if cfg.DeviceID == "" {
		return fmt.Errorf("configData.deviceId is required")
	}

	state := NewIoTDeviceState()
	c.Gateway = NewIoTGateway(cfg, state)
	if err := c.Gateway.Start(); err != nil {
		return fmt.Errorf("start IoTGateway[%s]: %w", cfg.DeviceID, err)
	}

	klog.Infof("mqtt-iot-mapper device initialised: deviceId=%s broker=%s",
		cfg.DeviceID, cfg.MQTTBroker)
	return nil
}

// GetDeviceData returns the current reported value for the requested property field.
// The value is the string-encoded latest value received from the device's status topic.
func (c *CustomizedClient) GetDeviceData(visitor *VisitorConfig) (interface{}, error) {
	c.deviceMutex.Lock()
	defer c.deviceMutex.Unlock()

	if c.Gateway == nil {
		return nil, fmt.Errorf("gateway not initialised")
	}

	fieldName := visitor.VisitorConfigData.FieldName
	if fieldName == "" {
		return nil, fmt.Errorf("visitor configData.fieldName is empty")
	}

	val := c.Gateway.GetProperty(fieldName)
	klog.V(4).Infof("GetDeviceData[%s]: field=%s value=%s",
		c.ProtocolConfig.ConfigData.DeviceID, fieldName, val)
	return val, nil
}

// SetDeviceData is called when the cloud updates a writable device twin property.
// It generates and publishes a cmd message to device/{deviceId}/cmd.
func (c *CustomizedClient) SetDeviceData(data interface{}, visitor *VisitorConfig) error {
	c.deviceMutex.Lock()
	defer c.deviceMutex.Unlock()

	if c.Gateway == nil {
		return fmt.Errorf("gateway not initialised")
	}

	fieldName := visitor.VisitorConfigData.FieldName
	if fieldName == "" {
		return fmt.Errorf("visitor configData.fieldName is empty")
	}

	klog.Infof("SetDeviceData[%s]: field=%s value=%v",
		c.ProtocolConfig.ConfigData.DeviceID, fieldName, data)

	return c.Gateway.SendCmd(fieldName, cast.ToString(data))
}

// StopDevice cleanly disconnects the gateway.
func (c *CustomizedClient) StopDevice() error {
	c.deviceMutex.Lock()
	defer c.deviceMutex.Unlock()

	if c.Gateway != nil {
		c.Gateway.Stop()
		c.Gateway = nil
		klog.Infof("mqtt-iot-mapper device stopped: deviceId=%s",
			c.ProtocolConfig.ConfigData.DeviceID)
	}
	return nil
}

// GetDeviceStates returns the KubeEdge device health state.
func (c *CustomizedClient) GetDeviceStates() (string, error) {
	c.deviceMutex.Lock()
	defer c.deviceMutex.Unlock()

	if c.Gateway == nil {
		return common.DeviceStatusUnknown, nil
	}
	return common.DeviceStatusOK, nil
}
