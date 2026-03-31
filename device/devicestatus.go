package device

import (
	"context"
	"time"

	"k8s.io/klog/v2"

	dmiapi "github.com/kubeedge/api/apis/dmi/v1beta1"
	"github.com/kubeedge/mapper-framework/pkg/common"
	"github.com/kubeedge/mapper-framework/pkg/grpcclient"
	"github.com/kubeedge/mqtt-iot-mapper/driver"
)

// DeviceStates handles periodic device-health state reporting to EdgeCore.
type DeviceStates struct {
	Client          *driver.CustomizedClient
	DeviceName      string
	DeviceNamespace string
	ReportToCloud   bool
	ReportCycle     time.Duration
}

// PushStatesToEdgeCore queries the driver health state and sends it via DMI gRPC.
func (ds *DeviceStates) PushStatesToEdgeCore() {
	states, err := ds.Client.GetDeviceStates()
	if err != nil {
		klog.Errorf("GetDeviceStates failed: %v", err)
		return
	}

	req := &dmiapi.ReportDeviceStatesRequest{
		DeviceName:      ds.DeviceName,
		State:           states,
		DeviceNamespace: ds.DeviceNamespace,
	}

	klog.V(4).Infof("send device %s status %s request to cloud", req.DeviceName, req.State)
	if err = grpcclient.ReportDeviceStates(req); err != nil {
		klog.Errorf("fail to report device states of %s with err: %+v", ds.DeviceName, err)
	}
}

// Run starts the health-state reporting loop.
func (ds *DeviceStates) Run(ctx context.Context) {
	if !ds.ReportToCloud {
		return
	}
	if ds.ReportCycle == 0 {
		ds.ReportCycle = common.DefaultReportCycle
	}
	ticker := time.NewTicker(ds.ReportCycle)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ds.PushStatesToEdgeCore()
		case <-ctx.Done():
			return
		}
	}
}
