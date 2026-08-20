package main

import (
	"errors"
	"os"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/klog/v2"

	"github.com/kubeedge/mapper-framework/pkg/common"
	"github.com/kubeedge/mapper-framework/pkg/config"
	"github.com/kubeedge/mapper-framework/pkg/grpcclient"
	"github.com/kubeedge/mapper-framework/pkg/grpcserver"
	"github.com/kubeedge/mapper-framework/pkg/httpserver"
	"github.com/kubeedge/mqtt-iot-mapper/device"
)

func main() {
	klog.InitFlags(nil)
	defer klog.Flush()

	// 桥接 klog 到 zap，输出平台规范的 JSON 日志（level 字段 + 大写枚举）
	klog.SetLogger(zapr.NewLogger(buildLogger()))

	c, err := config.Parse()
	if err != nil {
		klog.Fatal(err)
	}
	klog.Infof("config: %+v", c)

	klog.Infoln("Mapper will register to edgecore")
	deviceList, deviceModelList, err := grpcclient.RegisterMapper(true)
	if err != nil {
		klog.Fatal(err)
	}
	klog.Infoln("Mapper register finished")

	panel := device.NewDevPanel()
	err = panel.DevInit(deviceList, deviceModelList)
	if err != nil && !errors.Is(err, device.ErrEmptyData) {
		klog.Fatal(err)
	}
	klog.Infoln("devInit finished")
	go panel.DevStart()

	httpServer := httpserver.NewRestServer(panel, c.Common.HTTPPort)
	go httpServer.StartServer()

	grpcServer := grpcserver.NewServer(
		grpcserver.Config{
			SockPath: c.GrpcServer.SocketPath,
			Protocol: common.ProtocolCustomized,
		},
		panel,
	)
	defer grpcServer.Stop()
	if err = grpcServer.Start(); err != nil {
		klog.Fatal(err)
	}
}

// buildLogger 构建符合平台日志规范的 JSON logger。
// 字段：time / level（大写 TRACE|DEBUG|INFO|WARN|ERROR|FATAL）/ message
func buildLogger() *zap.Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "time"
	encoderCfg.LevelKey = "level"
	encoderCfg.MessageKey = "message"
	encoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderCfg.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02T15:04:05.000")

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		zapcore.InfoLevel,
	)
	return zap.New(core)
}
