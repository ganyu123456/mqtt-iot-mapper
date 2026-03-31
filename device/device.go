package device

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	dmiapi "github.com/kubeedge/api/apis/dmi/v1beta1"
	"github.com/kubeedge/mapper-framework/pkg/common"
	"github.com/kubeedge/mapper-framework/pkg/global"
	"github.com/kubeedge/mapper-framework/pkg/util/parse"
	mqttMethod "github.com/kubeedge/mqtt-iot-mapper/data/publish/mqtt"
	"github.com/kubeedge/mqtt-iot-mapper/driver"
)

// DevPanel is the central device management structure.
type DevPanel struct {
	deviceMuxs   map[string]context.CancelFunc
	devices      map[string]*driver.CustomizedDev
	models       map[string]common.DeviceModel
	wg           sync.WaitGroup
	serviceMutex sync.Mutex
	quitChan     chan os.Signal
}

var (
	devPanel *DevPanel
	once     sync.Once
)

// ErrEmptyData is returned when EdgeCore provides no devices or models.
var ErrEmptyData = errors.New("device or device model list is empty")

// NewDevPanel returns the singleton DevPanel instance.
func NewDevPanel() *DevPanel {
	once.Do(func() {
		devPanel = &DevPanel{
			deviceMuxs:   make(map[string]context.CancelFunc),
			devices:      make(map[string]*driver.CustomizedDev),
			models:       make(map[string]common.DeviceModel),
			wg:           sync.WaitGroup{},
			serviceMutex: sync.Mutex{},
			quitChan:     make(chan os.Signal),
		}
	})
	return devPanel
}

// DevStart starts all devices and blocks until they exit.
func (d *DevPanel) DevStart() {
	for id, dev := range d.devices {
		klog.V(4).Info("Dev: ", id, dev)
		ctx, cancel := context.WithCancel(context.Background())
		d.deviceMuxs[id] = cancel
		d.wg.Add(1)
		go d.start(ctx, dev)
	}
	signal.Notify(d.quitChan, os.Interrupt)
	go func() {
		<-d.quitChan
		for id, device := range d.devices {
			err := device.CustomizedClient.StopDevice()
			if err != nil {
				klog.Errorf("Service has stopped but failed to stop %s: %v", id, err)
			}
		}
		klog.V(1).Info("Exit mapper")
		os.Exit(1)
	}()
	d.wg.Wait()
}

// start initialises the protocol client and launches the data handler for a single device.
func (d *DevPanel) start(ctx context.Context, dev *driver.CustomizedDev) {
	defer d.wg.Done()

	var protocolConfig driver.ProtocolConfig
	if err := json.Unmarshal(dev.Instance.PProtocol.ConfigData, &protocolConfig); err != nil {
		klog.Errorf("Unmarshal ProtocolConfigs error: %v", err)
		return
	}
	client, err := driver.NewClient(protocolConfig)
	if err != nil {
		klog.Errorf("Init dev %s error: %v", dev.Instance.Name, err)
		return
	}
	dev.CustomizedClient = client
	if err = dev.CustomizedClient.InitDevice(); err != nil {
		klog.Errorf("Init device %s error: %v", dev.Instance.ID, err)
		return
	}
	go dataHandler(ctx, dev)
	<-ctx.Done()
}

// dataHandler sets up TwinData and optional push-method goroutines for each device property.
func dataHandler(ctx context.Context, dev *driver.CustomizedDev) {
	getStates := &DeviceStates{
		Client:          dev.CustomizedClient,
		DeviceName:      dev.Instance.Name,
		DeviceNamespace: dev.Instance.Namespace,
		ReportToCloud:   dev.Instance.Status.ReportToCloud,
		ReportCycle:     time.Millisecond * time.Duration(dev.Instance.Status.ReportCycle),
	}
	go getStates.Run(ctx)

	for _, twin := range dev.Instance.Twins {
		twin.Property.PProperty.DataType = strings.ToLower(twin.Property.PProperty.DataType)
		var visitorConfig driver.VisitorConfig

		if err := json.Unmarshal(twin.Property.Visitors, &visitorConfig); err != nil {
			klog.Errorf("Unmarshal VisitorConfig error: %v", err)
			continue
		}

		if err := setVisitor(&visitorConfig, &twin, dev); err != nil {
			klog.Error(err)
			continue
		}

		twinData := &TwinData{
			DeviceName:      dev.Instance.Name,
			DeviceNamespace: dev.Instance.Namespace,
			Client:          dev.CustomizedClient,
			Name:            twin.PropertyName,
			Type:            twin.ObservedDesired.Metadata.Type,
			ObservedDesired: twin.ObservedDesired,
			VisitorConfig:   &visitorConfig,
			Topic:           fmt.Sprintf(common.TopicTwinUpdate, dev.Instance.ID),
			CollectCycle:    time.Millisecond * time.Duration(twin.Property.CollectCycle),
			ReportToCloud:   twin.Property.ReportToCloud,
		}
		go twinData.Run(ctx)

		dataModel := common.NewDataModel(
			dev.Instance.Name,
			twin.Property.PropertyName,
			dev.Instance.Namespace,
			common.WithType(twin.ObservedDesired.Metadata.Type),
		)
		if twin.Property.PushMethod.MethodConfig != nil && twin.Property.PushMethod.MethodName != "" {
			pushHandler(ctx, &twin, dev.CustomizedClient, &visitorConfig, dataModel)
		}
	}
}

// pushHandler starts the data-plane push goroutine for a single device property.
func pushHandler(ctx context.Context, twin *common.Twin, client *driver.CustomizedClient, visitorConfig *driver.VisitorConfig, dataModel *common.DataModel) {
	var dataPanel global.DataPanel
	var err error

	switch twin.Property.PushMethod.MethodName {
	case common.PushMethodMQTT:
		dataPanel, err = mqttMethod.NewDataPanel(twin.Property.PushMethod.MethodConfig)
	default:
		klog.Warningf("push method %q is not supported; only mqtt is built in", twin.Property.PushMethod.MethodName)
		return
	}
	if err != nil {
		klog.Errorf("new data panel error: %v", err)
		return
	}
	if err = dataPanel.InitPushMethod(); err != nil {
		klog.Errorf("init push method err: %v", err)
		return
	}

	reportCycle := time.Millisecond * time.Duration(twin.Property.ReportCycle)
	if reportCycle == 0 {
		reportCycle = common.DefaultReportCycle
	}
	ticker := time.NewTicker(reportCycle)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				deviceData, err := client.GetDeviceData(visitorConfig)
				if err != nil {
					klog.Errorf("publish error: %v", err)
					continue
				}
				sData, err := common.ConvertToString(deviceData)
				if err != nil {
					klog.Errorf("Failed to convert publish method data: %v", err)
					continue
				}
				dataModel.SetValue(sData)
				dataModel.SetTimeStamp()
				dataPanel.Push(dataModel)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// setVisitor applies the desired value from the Device CRD to the device driver.
func setVisitor(visitorConfig *driver.VisitorConfig, twin *common.Twin, dev *driver.CustomizedDev) error {
	if twin.Property.PProperty.AccessMode == "ReadOnly" {
		klog.V(3).Infof("%s twin readonly property: %s", dev.Instance.Name, twin.PropertyName)
		return nil
	}
	klog.V(2).Infof("Convert type: %s, value: %s", twin.Property.PProperty.DataType, twin.ObservedDesired.Value)
	var value interface{}
	if twin.ObservedDesired.Value != "" {
		convertedValue, err := common.Convert(twin.Property.PProperty.DataType, twin.ObservedDesired.Value)
		if err != nil {
			klog.Errorf("Failed to convert value as %s: %v", twin.Property.PProperty.DataType, err)
			return err
		}
		value = convertedValue
	} else {
		value = twin.ObservedDesired.Value
	}
	if err := dev.CustomizedClient.SetDeviceData(value, visitorConfig); err != nil {
		return fmt.Errorf("%s set device data error: %v", twin.PropertyName, err)
	}
	return nil
}

// DevInit parses the gRPC device/model lists and populates the DevPanel.
func (d *DevPanel) DevInit(deviceList []*dmiapi.Device, deviceModelList []*dmiapi.DeviceModel) error {
	if len(deviceList) == 0 || len(deviceModelList) == 0 {
		return ErrEmptyData
	}

	for i := range deviceModelList {
		model := deviceModelList[i]
		cur := parse.GetDeviceModelFromGrpc(model)
		modelID := parse.GetResourceID(model.Namespace, model.Name)
		d.models[modelID] = cur
	}

	for i := range deviceList {
		device := deviceList[i]
		modelID := parse.GetResourceID(device.Namespace, device.Spec.DeviceModelReference)
		commonModel := d.models[modelID]
		protocol, err := parse.BuildProtocolFromGrpc(device)
		if err != nil {
			return err
		}
		instance, err := parse.GetDeviceFromGrpc(device, &commonModel)
		if err != nil {
			return err
		}
		instance.PProtocol = protocol

		cur := new(driver.CustomizedDev)
		cur.Instance = *instance
		d.devices[instance.ID] = cur
	}

	return nil
}

// UpdateDev stops the old device and starts the updated one.
func (d *DevPanel) UpdateDev(model *common.DeviceModel, device *common.DeviceInstance) {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()

	if oldDevice, ok := d.devices[device.ID]; ok {
		if err := d.stopDev(oldDevice, device.ID); err != nil {
			klog.Error(err)
		}
	}
	d.devices[device.ID] = new(driver.CustomizedDev)
	d.devices[device.ID].Instance = *device
	d.models[model.ID] = *model

	ctx, cancelFunc := context.WithCancel(context.Background())
	d.deviceMuxs[device.ID] = cancelFunc
	d.wg.Add(1)
	go d.start(ctx, d.devices[device.ID])
}

// UpdateDevTwins updates twin definitions and restarts the device.
func (d *DevPanel) UpdateDevTwins(deviceID string, twins []common.Twin) error {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()
	dev, ok := d.devices[deviceID]
	if !ok {
		return fmt.Errorf("device %s not found", deviceID)
	}
	dev.Instance.Twins = twins
	model := d.models[dev.Instance.Model]
	d.UpdateDev(&model, &dev.Instance)
	return nil
}

// DealDeviceTwinGet returns the current reported value for a twin property.
func (d *DevPanel) DealDeviceTwinGet(deviceID string, twinName string) (interface{}, error) {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()
	dev, ok := d.devices[deviceID]
	if !ok {
		return nil, fmt.Errorf("not found device %s", deviceID)
	}
	var res []parse.TwinResultResponse
	for _, twin := range dev.Instance.Twins {
		if twinName != "" && twin.PropertyName != twinName {
			continue
		}
		payload, err := getTwinData(deviceID, twin, d.devices[deviceID])
		if err != nil {
			return nil, err
		}
		item := parse.TwinResultResponse{
			PropertyName: twinName,
			Payload:      payload,
		}
		res = append(res, item)
	}
	return json.Marshal(res)
}

func getTwinData(deviceID string, twin common.Twin, dev *driver.CustomizedDev) ([]byte, error) {
	var visitorConfig driver.VisitorConfig
	if err := json.Unmarshal(twin.Property.Visitors, &visitorConfig); err != nil {
		return nil, err
	}
	if err := setVisitor(&visitorConfig, &twin, dev); err != nil {
		return nil, err
	}
	twinData := &TwinData{
		DeviceName:    deviceID,
		Client:        dev.CustomizedClient,
		Name:          twin.PropertyName,
		Type:          twin.ObservedDesired.Metadata.Type,
		VisitorConfig: &visitorConfig,
		Topic:         fmt.Sprintf(common.TopicTwinUpdate, deviceID),
	}
	return twinData.GetPayLoad()
}

// GetDevice returns the device instance with up-to-date twin values.
func (d *DevPanel) GetDevice(deviceID string) (interface{}, error) {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()
	found, ok := d.devices[deviceID]
	if !ok || found == nil {
		return nil, fmt.Errorf("device %s not found", deviceID)
	}
	for i, twin := range found.Instance.Twins {
		payload, err := getTwinData(deviceID, twin, found)
		if err != nil {
			return nil, err
		}
		found.Instance.Twins[i].Reported.Value = string(payload)
	}
	return found, nil
}

// RemoveDevice stops and removes a device from the panel.
func (d *DevPanel) RemoveDevice(deviceID string) error {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()
	dev := d.devices[deviceID]
	delete(d.devices, deviceID)
	return d.stopDev(dev, deviceID)
}

// WriteDevice invokes a device method via the HTTP API.
func (d *DevPanel) WriteDevice(deviceMethodName, deviceID, propertyName, data string) error {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()
	dev, ok := d.devices[deviceID]
	if !ok {
		return fmt.Errorf("not found device %s", deviceID)
	}

	deviceMethodMap := make(map[string][]string)
	for _, method := range dev.Instance.Methods {
		deviceMethodMap[method.Name] = append(deviceMethodMap[method.Name], method.PropertyNames...)
	}

	propertyNames, ok := deviceMethodMap[deviceMethodName]
	if !ok {
		return fmt.Errorf("deviceMethod name %s does not exist in device instance", deviceMethodName)
	}

	flag := false
	for _, name := range propertyNames {
		if name == propertyName {
			flag = true
			break
		}
	}
	if !flag {
		return fmt.Errorf("deviceProperty %s to be written is not in the list defined by devicemethod", propertyName)
	}

	var dataType string
	var deviceproperty common.DeviceProperty
	flag = false
	for _, property := range dev.Instance.Properties {
		if property.PropertyName != propertyName {
			continue
		}
		dataType = property.PProperty.DataType
		deviceproperty = property
		flag = true
		break
	}
	if !flag {
		return fmt.Errorf("can't find device propertyName %s in device instance", propertyName)
	}

	writeData, err := common.Convert(strings.ToLower(dataType), data)
	if err != nil {
		return fmt.Errorf("conversion data format failed: datatype=%s data=%s", strings.ToLower(dataType), data)
	}

	var visitorConfig driver.VisitorConfig
	if err = json.Unmarshal(deviceproperty.Visitors, &visitorConfig); err != nil {
		return err
	}
	return dev.CustomizedClient.DeviceDataWrite(&visitorConfig, deviceMethodName, propertyName, writeData)
}

func (d *DevPanel) stopDev(dev *driver.CustomizedDev, id string) error {
	cancelFunc, ok := d.deviceMuxs[id]
	if !ok {
		return fmt.Errorf("can not find device %s from device muxs", id)
	}
	if err := dev.CustomizedClient.StopDevice(); err != nil {
		klog.Errorf("stop device %s error: %v", id, err)
	}
	cancelFunc()
	return nil
}

// GetModel returns the device model by ID.
func (d *DevPanel) GetModel(modelID string) (common.DeviceModel, error) {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()
	if model, ok := d.models[modelID]; ok {
		return model, nil
	}
	return common.DeviceModel{}, fmt.Errorf("deviceModel %s not found", modelID)
}

// UpdateModel updates a device model definition.
func (d *DevPanel) UpdateModel(model *common.DeviceModel) {
	d.serviceMutex.Lock()
	d.models[model.ID] = *model
	d.serviceMutex.Unlock()
}

// RemoveModel removes a device model.
func (d *DevPanel) RemoveModel(modelID string) {
	d.serviceMutex.Lock()
	delete(d.models, modelID)
	d.serviceMutex.Unlock()
}

// GetTwinResult returns the current value and data type for a twin property.
func (d *DevPanel) GetTwinResult(deviceID string, twinName string) (string, string, error) {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()
	dev, ok := d.devices[deviceID]
	if !ok {
		return "", "", fmt.Errorf("not found device %s", deviceID)
	}
	var res string
	var dataType string
	for _, twin := range dev.Instance.Twins {
		if twinName != "" && twin.PropertyName != twinName {
			continue
		}
		var visitorConfig driver.VisitorConfig
		if err := json.Unmarshal(twin.Property.Visitors, &visitorConfig); err != nil {
			return "", "", err
		}
		if err := setVisitor(&visitorConfig, &twin, dev); err != nil {
			return "", "", err
		}
		data, err := dev.CustomizedClient.GetDeviceData(&visitorConfig)
		if err != nil {
			return "", "", fmt.Errorf("get device data failed: %v", err)
		}
		res, err = common.ConvertToString(data)
		if err != nil {
			return "", "", err
		}
		dataType = twin.Property.PProperty.DataType
	}
	return res, dataType, nil
}

// GetDeviceMethod returns a map of method names to property names, and property data types.
func (d *DevPanel) GetDeviceMethod(deviceID string) (map[string][]string, map[string]string, error) {
	d.serviceMutex.Lock()
	defer d.serviceMutex.Unlock()
	found, ok := d.devices[deviceID]
	if !ok || found == nil {
		return nil, nil, fmt.Errorf("device %s not found", deviceID)
	}

	deviceMethodMap := make(map[string][]string)
	propertyTypeMap := make(map[string]string)

	for _, method := range found.Instance.Methods {
		deviceMethodMap[method.Name] = append(deviceMethodMap[method.Name], method.PropertyNames...)
	}
	for _, property := range found.Instance.Properties {
		propertyTypeMap[property.Name] = strings.ToLower(property.PProperty.DataType)
	}
	return deviceMethodMap, propertyTypeMap, nil
}
