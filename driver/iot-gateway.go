package driver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"k8s.io/klog/v2"

	"github.com/kubeedge/mqtt-iot-mapper/util"
)

// gatewayLogger writes persistent MQTT connection events to a hostPath file.
var gatewayLogger *util.FileLogger

func getGatewayLogger() *util.FileLogger {
	if gatewayLogger != nil {
		return gatewayLogger
	}
	cfg := util.FileLoggerConfig{
		Path:       util.EnvOrDefault("GATEWAY_LOG_PATH", "/etc/kubeedge/mapper-gateway.log"),
		MaxSizeMB:  util.EnvIntOrDefault("LOG_MAX_SIZE_MB", 10),
		MaxBackups: util.EnvIntOrDefault("LOG_MAX_BACKUPS", 30),
	}
	l, err := util.NewFileLogger(cfg)
	if err != nil {
		klog.Warningf("cannot create gateway logger: %v", err)
		return nil
	}
	gatewayLogger = l
	klog.Infof("gateway logger: %s (maxSize=%dMB maxBackups=%d)",
		cfg.Path, cfg.MaxSizeMB, cfg.MaxBackups)
	return l
}

func gatewayPersistLog(format string, args ...interface{}) {
	if l := getGatewayLogger(); l != nil {
		l.Write(format, args...)
	}
}

// statusMessage matches the JSON published by devices to device/{deviceId}/status.
type statusMessage struct {
	Timestamp int64                  `json:"timestamp"`
	DeviceID  string                 `json:"deviceId"`
	Status    map[string]interface{} `json:"status"`
}

// cmdMessage is the command payload published to device/{deviceId}/cmd.
type cmdMessage struct {
	RequestID string                 `json:"requestId"`
	DeviceID  string                 `json:"deviceId"`
	Params    map[string]interface{} `json:"params"`
	Timestamp int64                  `json:"timestamp"`
}

// IoTGateway manages a single MQTT connection for one IoT device.
//
// Responsibilities:
//   - Subscribe device/{deviceId}/status → parse fields → update IoTDeviceState
//   - Publish  device/{deviceId}/cmd    → when SetProperty is called (desired value change)
//
// The device/{deviceId}/data topic carries high-volume batch data and is handled
// directly by the edge EMQX broker; this mapper does not subscribe to it.
type IoTGateway struct {
	cfg        ConfigData
	state      *IoTDeviceState
	mqttClient mqtt.Client
	doneChan   chan struct{}
}

// NewIoTGateway creates a gateway; call Start() to connect.
func NewIoTGateway(cfg ConfigData, state *IoTDeviceState) *IoTGateway {
	return &IoTGateway{
		cfg:      cfg,
		state:    state,
		doneChan: make(chan struct{}),
	}
}

// Start connects to the MQTT broker and subscribes to the status topic.
func (g *IoTGateway) Start() error {
if err := g.connect(); err != nil {
		errMsg := fmt.Sprintf("IoTGateway[%s] connect failed: %v", g.cfg.DeviceID, err)
		klog.Error(errMsg)
		gatewayPersistLog(errMsg)
		return fmt.Errorf("connect to MQTT broker %s: %w", g.cfg.MQTTBroker, err)
	}

	if err := g.subscribeStatus(); err != nil {
		return fmt.Errorf("subscribe status topic: %w", err)
	}

	startMsg := fmt.Sprintf("IoTGateway[%s] started | broker=%s", g.cfg.DeviceID, g.cfg.MQTTBroker)
	klog.Info(startMsg)
	gatewayPersistLog(startMsg)
	return nil
}

// Stop disconnects from the MQTT broker.
func (g *IoTGateway) Stop() {
	close(g.doneChan)
	if g.mqttClient != nil && g.mqttClient.IsConnected() {
		g.mqttClient.Disconnect(500)
	}
	stopMsg := fmt.Sprintf("IoTGateway[%s] stopped", g.cfg.DeviceID)
	klog.Info(stopMsg)
	gatewayPersistLog(stopMsg)
}

// GetProperty returns the latest reported value for a device property field.
func (g *IoTGateway) GetProperty(fieldName string) string {
	return g.state.Get(fieldName)
}

// IsConnected returns true when the MQTT client is connected to the broker.
func (g *IoTGateway) IsConnected() bool {
	return g.mqttClient != nil && g.mqttClient.IsConnected()
}

// SendCmd generates a cmd message and publishes it to device/{deviceId}/cmd.
// Called when the cloud updates a writable device twin property.
func (g *IoTGateway) SendCmd(fieldName string, value interface{}) error {
	if g.mqttClient == nil || !g.mqttClient.IsConnected() {
		return fmt.Errorf("MQTT client not connected")
	}

	requestID := fmt.Sprintf("CMD-%d-%04d", time.Now().UnixMilli(), rand.Intn(10000))
	cmd := cmdMessage{
		RequestID: requestID,
		DeviceID:  g.cfg.DeviceID,
		Params:    map[string]interface{}{fieldName: value},
		Timestamp: time.Now().UnixMilli(),
	}

	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal cmd: %w", err)
	}

	topic := fmt.Sprintf("device/%s/cmd", g.cfg.DeviceID)
	token := g.mqttClient.Publish(topic, 1, false, payload)
	if !token.WaitTimeout(3 * time.Second) {
		return fmt.Errorf("publish cmd timeout: topic=%s", topic)
	}
	if token.Error() != nil {
		return token.Error()
	}

	cmdMsg := fmt.Sprintf("IoTGateway[%s]: sent cmd | requestId=%s field=%s value=%v",
		g.cfg.DeviceID, requestID, fieldName, value)
	klog.Info(cmdMsg)
	gatewayPersistLog(cmdMsg)
	return nil
}

// ─────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────

func (g *IoTGateway) clientID() string {
	if g.cfg.MQTTClientId != "" {
		return g.cfg.MQTTClientId
	}
	return fmt.Sprintf("mqtt-iot-mapper-%s", g.cfg.DeviceID)
}

func (g *IoTGateway) connect() error {
	opts := mqtt.NewClientOptions().
		AddBroker(g.cfg.MQTTBroker).
		SetClientID(g.clientID()).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(c mqtt.Client) {
			klog.Infof("IoTGateway[%s]: MQTT reconnected to %s", g.cfg.DeviceID, g.cfg.MQTTBroker)
			_ = g.subscribeStatus()
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			klog.Warningf("IoTGateway[%s]: MQTT connection lost: %v", g.cfg.DeviceID, err)
		})

	if g.cfg.MQTTUsername != "" {
		opts.SetUsername(g.cfg.MQTTUsername).SetPassword(g.cfg.MQTTPassword)
	}

	g.mqttClient = mqtt.NewClient(opts)

	const (
		maxAttempts = 10
		maxBackoff  = 30 * time.Second
	)
	backoff := 1 * time.Second

	for attempt := 0; attempt < maxAttempts; attempt++ {
		token := g.mqttClient.Connect()
		if token.WaitTimeout(10*time.Second) && token.Error() == nil {
			return nil
		}
		if attempt < maxAttempts-1 {
			klog.Warningf("IoTGateway[%s]: MQTT connect attempt %d/%d failed, retrying in %v",
				g.cfg.DeviceID, attempt+1, maxAttempts, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	return fmt.Errorf("connect timeout after %d attempts", maxAttempts)
}


func (g *IoTGateway) subscribeStatus() error {
	topic := fmt.Sprintf("device/%s/status", g.cfg.DeviceID)
	token := g.mqttClient.Subscribe(topic, 1, g.handleStatus)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("subscribe timeout: %s", topic)
	}
	if token.Error() != nil {
		return token.Error()
	}
	subMsg := fmt.Sprintf("IoTGateway[%s]: subscribed status → %s", g.cfg.DeviceID, topic)
	klog.Info(subMsg)
	gatewayPersistLog(subMsg)
	return nil
}

// handleStatus is called on every device/{deviceId}/status message.
// Parses status fields and updates IoTDeviceState so GetProperty can serve them.
func (g *IoTGateway) handleStatus(_ mqtt.Client, msg mqtt.Message) {
	var sm statusMessage
	dec := json.NewDecoder(bytes.NewReader(msg.Payload()))
	dec.UseNumber()
	if err := dec.Decode(&sm); err != nil {
		parseErr := fmt.Sprintf("IoTGateway[%s]: unmarshal status failed: %v", g.cfg.DeviceID, err)
		klog.Error(parseErr)
		gatewayPersistLog(parseErr)
		return
	}

	for k, v := range sm.Status {
		var strVal string
		if n, ok := v.(json.Number); ok {
			strVal = n.String()
		} else {
			strVal = fmt.Sprintf("%v", v)
		}
		g.state.Set(k, strVal)
	}

	klog.V(4).Infof("IoTGateway[%s]: status updated | %d fields | ts=%d",
		g.cfg.DeviceID, len(sm.Status), sm.Timestamp)
}

