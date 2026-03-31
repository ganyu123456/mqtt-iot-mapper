package mqtt

import (
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"k8s.io/klog/v2"

	"github.com/kubeedge/mapper-framework/pkg/common"
	"github.com/kubeedge/mapper-framework/pkg/global"
)

// PushMethod implements global.DataPanel for MQTT push.
type PushMethod struct {
	MQTT   *MQTTConfig
	client mqtt.Client
}

// MQTTConfig is deserialized from Device CRD spec.properties[n].pushMethod.methodConfig.
type MQTTConfig struct {
	Address  string `json:"address,omitempty"`
	Topic    string `json:"topic,omitempty"`
	QoS      int    `json:"qos,omitempty"`
	Retained bool   `json:"retained,omitempty"`
}

// NewDataPanel parses the JSON push-method config and returns a DataPanel.
func NewDataPanel(config json.RawMessage) (global.DataPanel, error) {
	mqttConfig := new(MQTTConfig)
	if err := json.Unmarshal(config, mqttConfig); err != nil {
		return nil, err
	}
	return &PushMethod{MQTT: mqttConfig}, nil
}

// InitPushMethod establishes a persistent MQTT connection.
func (pm *PushMethod) InitPushMethod() error {
	klog.V(1).Infof("Init MQTT push method: broker=%s topic=%s", pm.MQTT.Address, pm.MQTT.Topic)
	opts := mqtt.NewClientOptions().
		AddBroker(pm.MQTT.Address).
		SetAutoReconnect(true)

	pm.client = mqtt.NewClient(opts)
	token := pm.client.Connect()
	if token.WaitTimeout(10*time.Second) && token.Error() != nil {
		return fmt.Errorf("MQTT connect to %s failed: %w", pm.MQTT.Address, token.Error())
	}
	return nil
}

// Push serialises the DataModel and publishes it to the configured topic.
func (pm *PushMethod) Push(data *common.DataModel) {
	if pm.client == nil || !pm.client.IsConnected() {
		klog.Warning("MQTT push method client is not connected, skipping publish")
		return
	}

	payload := fmt.Sprintf(`{"name":%q,"value":%s,"timestamp":%d}`,
		data.DeviceName+"/"+data.PropertyName,
		data.Value,
		data.TimeStamp,
	)

	klog.V(4).Infof("Publish to %s topic %s: %s", pm.MQTT.Address, pm.MQTT.Topic, payload)
	token := pm.client.Publish(pm.MQTT.Topic, byte(pm.MQTT.QoS), pm.MQTT.Retained, payload)
	if !token.WaitTimeout(500 * time.Millisecond) {
		klog.Warningf("MQTT publish timeout for topic %s", pm.MQTT.Topic)
	}
}
