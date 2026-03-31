package driver

import (
	"sync"

	"github.com/kubeedge/mapper-framework/pkg/common"
)

// CustomizedDev holds a device instance and its protocol client.
type CustomizedDev struct {
	Instance         common.DeviceInstance
	CustomizedClient *CustomizedClient
}

// CustomizedClient is the protocol client for the universal IoT MQTT mapper.
type CustomizedClient struct {
	deviceMutex sync.Mutex
	ProtocolConfig
	Gateway *IoTGateway
}

// ProtocolConfig is deserialized from Device CRD spec.protocol.
type ProtocolConfig struct {
	ProtocolName string     `json:"protocolName"`
	ConfigData   ConfigData `json:"configData"`
}

// ConfigData holds MQTT connection parameters for a single IoT device.
// Populated from Device CRD spec.protocol.configData.
type ConfigData struct {
	// MQTTBroker is the edge-local MQTT broker, e.g. "tcp://192.168.122.212:1884".
	MQTTBroker   string `json:"mqttBroker"`
	MQTTUsername string `json:"mqttUsername"`
	MQTTPassword string `json:"mqttPassword"`
	// MQTTClientId is the MQTT client ID for this mapper↔device connection.
	MQTTClientId string `json:"mqttClientId"`
	// DeviceID is the unique device identifier; determines MQTT topic paths:
	//   Subscribe: device/{deviceId}/status
	//   Publish:   device/{deviceId}/cmd
	//   Subscribe: device/{deviceId}/data
	DeviceID string `json:"deviceId"`
	// DataForwardBroker (optional): if set, data topic messages are forwarded here.
	DataForwardBroker string `json:"dataForwardBroker,omitempty"`
	// DataForwardTopic (optional): target topic for data forwarding.
	DataForwardTopic string `json:"dataForwardTopic,omitempty"`
}

// VisitorConfig is deserialized from Device CRD spec.properties[n].visitors.
type VisitorConfig struct {
	ProtocolName      string            `json:"protocolName"`
	VisitorConfigData VisitorConfigData `json:"configData"`
}

// VisitorConfigData maps a KubeEdge Device Twin property to a status message field.
type VisitorConfigData struct {
	// FieldName is the JSON key inside status.{fieldName} in the device status message.
	// E.g. "taskControl", "collectInterval", "door1Control1", "captureInterval".
	FieldName string `json:"fieldName"`
}

// IoTDeviceState holds the latest reported property values for one device.
// Updated on every device/{deviceId}/status message received.
type IoTDeviceState struct {
	mu     sync.RWMutex
	values map[string]string // fieldName → string-encoded value
}

// NewIoTDeviceState creates an empty state store.
func NewIoTDeviceState() *IoTDeviceState {
	return &IoTDeviceState{values: make(map[string]string)}
}

// Set stores a field value (thread-safe).
func (s *IoTDeviceState) Set(field, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[field] = value
}

// Get retrieves a field value; returns "" if not yet received.
func (s *IoTDeviceState) Get(field string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.values[field]
}
