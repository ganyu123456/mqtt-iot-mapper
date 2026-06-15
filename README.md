# mqtt-iot-mapper

通用 IoT MQTT Mapper（v1），基于 KubeEdge mapper-framework 开发，实现平台 Device Twin 与第三方 IoT 设备三主题协议（status / cmd / data）的双向桥接。

## 简介

本 mapper 是《云边协同平台物联网软件接入通信方案》中「边缘 Mapper 桥接层」的核心实现。第三方厂商只需按规范将设备接入边缘节点 EMQX，本 mapper 即可自动完成：

- 订阅 `device/{deviceId}/status` → 解析状态字段 → 更新 KubeEdge Device Twin
- 监听 Device Twin desired 值变更 → 生成标准 cmd 消息 → 发布到 `device/{deviceId}/cmd`
- 订阅 `device/{deviceId}/data` → 可选透传到平台数据面 MQTT Broker

**通用设计**：同一个 mapper 实例可同时管理网关、门禁、摄像头等多种类型设备，无需为每种设备单独开发。

## 整体架构

```
平台云端 (KubeEdge Cloud)
    │  Device Twin desired 值变更
    │                              ▲  reported 值上报
    ▼                              │
KubeEdge EdgeCore (DMI gRPC)
    │                              ▲
    ▼                              │
┌─────────────────────────────────────────────────────────┐
│                  mqtt-iot-mapper                         │
│                                                         │
│  SetDeviceData(field, value)                            │
│    → 生成 cmd 消息 → 发布 device/{id}/cmd               │
│                                                         │
│  GetDeviceData(field)                                   │
│    → 读取内存缓存 ← 解析 device/{id}/status             │
│                                                         │
│  data 透传：device/{id}/data → DataForwardBroker        │
└─────────────────────────────────────────────────────────┘
    │  订阅 status/data                   │  发布 cmd
    ▼                                     ▼
边缘节点 EMQX（tcp://192.168.122.212:1884）
    ▲  status/data 上报      ▼  cmd 下发
第三方设备（网关 / 门禁 / 摄像头）
```

## 支持的设备类型

| 设备类型 | DeviceModel | DeviceInstance | 对应模拟器 |
|---------|-------------|----------------|-----------|
| 工业网关 | `iot-gateway-model` | `iot-gateway-001` | `10-gateway-simulator` |
| 门禁设备 | `iot-door-access-model` | `iot-door-access-001` | `11-door-access-simulator` |
| 摄像头 | `iot-camera-model` | `iot-camera-001` | `12-camera-simulator` |

## MQTT 消息规范

### status 主题（设备 → mapper）

```json
{
  "timestamp": 1750453800000,
  "deviceId": "gateway-001",
  "status": {
    "taskControl": 1,
    "collectInterval": 5,
    "collectPointTotal": 100,
    "collectPointOnline": 98,
    "lastCollectTime": 1750453800000
  }
}
```

### cmd 主题（mapper → 设备）

```json
{
  "requestId": "CMD-1750453800000-0012",
  "deviceId": "gateway-001",
  "params": { "collectInterval": 10 },
  "timestamp": 1750453800000
}
```

- `requestId`：mapper 自动生成，格式 `CMD-{毫秒时间戳}-{4位随机数}`
- `params`：仅包含单个可写字段及目标值
- 设备须在 1s 内执行，完成后通过 status 主题推送最新全量状态

### data 主题（设备 → mapper）

```json
{
  "timestamp": 1750453800000,
  "deviceId": "gateway-001",
  "batchData": { ... }
}
```

mapper 不解析 `batchData` 内容，若配置了 `dataForwardBroker`，则原样转发到目标 Broker。

## Device CRD 配置

### protocol.configData 字段说明

```yaml
protocol:
  protocolName: mqtt-iot-mapper
  configData:
    mqttBroker: "tcp://192.168.122.212:1884"  # 边缘节点 EMQX 地址（必填）
    deviceId: "gateway-001"                    # 设备唯一标识（必填，决定 MQTT 主题）
    mqttClientId: "mapper-gateway-001"         # MQTT 客户端 ID（多设备须唯一）
    mqttUsername: ""                           # MQTT 认证（留空表示无认证）
    mqttPassword: ""
    dataForwardBroker: ""                      # 可选：data 透传目标 Broker
    dataForwardTopic: ""                       # 可选：data 透传目标主题
```

### visitors.configData 字段说明

```yaml
visitors:
  configData:
    fieldName: "collectInterval"  # 对应 status.{fieldName}，mapper 据此路由读写操作
```

## 项目结构

```
13-mqtt-iot-mapper/
├── cmd/
│   └── main.go                 # 入口：注册 mapper、启动 gRPC/HTTP server
├── device/
│   ├── device.go               # DevPanel：管理所有设备实例生命周期
│   ├── devicestatus.go         # 设备健康状态定期上报 EdgeCore
│   └── devicetwin.go           # Device Twin 属性采集与上报
├── driver/
│   ├── devicetype.go           # 数据结构：ConfigData、VisitorConfig、IoTDeviceState
│   ├── driver.go               # GetDeviceData / SetDeviceData / InitDevice 实现
│   └── iot-gateway.go          # MQTT 连接、status 解析、cmd 发布、data 转发
├── data/publish/mqtt/
│   └── client.go               # MQTT push 方法（Device CRD pushMethod 支持）
├── crds/
│   ├── gateway-model.yaml      # 工业网关 DeviceModel
│   ├── gateway-instance.yaml   # 工业网关 Device 实例
│   ├── door-access-model.yaml  # 门禁 DeviceModel
│   ├── door-access-instance.yaml
│   ├── camera-model.yaml       # 摄像头 DeviceModel
│   └── camera-instance.yaml
├── helm/mqtt-iot-mapper/       # Helm Chart
├── resource/                   # kubectl apply 示例
│   ├── configmap.yaml
│   └── deployment.yaml
├── config.yaml                 # mapper-framework 配置（本地调试用）
├── Dockerfile
└── .drone.yml
```

## 部署步骤

### 1. 创建设备模型和设备实例

```bash
# 按需 apply 所需设备类型的 CRD
kubectl apply -f crds/gateway-model.yaml
kubectl apply -f crds/gateway-instance.yaml

kubectl apply -f crds/door-access-model.yaml
kubectl apply -f crds/door-access-instance.yaml

kubectl apply -f crds/camera-model.yaml
kubectl apply -f crds/camera-instance.yaml
```

### 2. 部署 mapper

#### 方式一：kubectl apply（快速验证）

```bash
# 先创建 ConfigMap
kubectl apply -f resource/configmap.yaml

# 再创建 Deployment
kubectl apply -f resource/deployment.yaml
```

#### 方式二：Helm Chart（推荐生产使用）

```bash
# 创建镜像仓库凭证
kubectl create secret docker-registry harbor-secret \
  --docker-server=harbor.zkjgy.online \
  --docker-username=<用户名> \
  --docker-password=<密码> \
  -n <命名空间>

# 安装
helm install mqtt-iot-mapper ./helm/mqtt-iot-mapper \
  -n <命名空间> \
  --set nodeName=cloudedge-edge-02
```

### 3. 部署模拟设备（可选，联调测试用）

```bash
kubectl apply -f ../10-gateway-simulator/resource/deployment.yaml
kubectl apply -f ../11-door-access-simulator/resource/deployment.yaml
kubectl apply -f ../12-camera-simulator/resource/deployment.yaml
```

### 4. 验证

```bash
# 查看 mapper 日志
kubectl logs -f deployment/mqtt-iot-mapper -n <命名空间>

# 查看设备 Twin 状态
kubectl get device iot-gateway-001 -o yaml

# 订阅 EMQX 验证 status 数据到达
mosquitto_sub -h 192.168.122.212 -p 1884 -t "device/gateway-001/status" -v
```

## Helm Chart 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `nodeName` | `cloudedge-edge-02` | 部署边缘节点 |
| `image.tag` | `latest` | 镜像标签 |
| `logLevel` | `4` | 日志级别（4=Debug, 2=Info） |
| `mapperConfig.grpc_server.socket_path` | `/etc/kubeedge/mqtt-iot-mapper.sock` | gRPC socket 路径 |
| `mapperConfig.common.protocol` | `mqtt-iot-mapper` | mapper 协议标识（需与 Device CRD protocolName 一致） |

### 自定义 values 示例

```yaml
nodeName: cloudedge-edge-02
logLevel: "2"   # 生产环境建议 Info 级别

mapperConfig:
  grpc_server:
    socket_path: /etc/kubeedge/mqtt-iot-mapper.sock
  common:
    name: mqtt-iot-mapper
    version: v1.0.0
    api_version: v1.0.0
    protocol: mqtt-iot-mapper
    address: 127.0.0.1
    edgecore_sock: /etc/kubeedge/dmi.sock
```

## 多设备实例配置说明

同一个 mapper 实例可管理多个设备，每个设备通过独立的 Device CRD 配置，关键原则：

1. `configData.deviceId` 须全局唯一（决定 MQTT 主题路径）
2. `configData.mqttClientId` 须全局唯一（EMQX 会踢掉同 ID 的旧连接）
3. 不同设备类型共用同一个 mapper，只需各自有对应的 DeviceModel

## 本地开发构建

```bash
# 生成 go.sum（首次）
go mod tidy

# 本地构建二进制
go build -o mqtt-iot-mapper ./cmd/main.go

# 本地运行（需要 EdgeCore 在同一节点运行）
./mqtt-iot-mapper --config-file config.yaml --v 4
```

## CI/CD

`.drone.yml` 自动完成 `linux/amd64` + `linux/arm64` 双架构镜像构建及 Helm Chart 推送：

```
harbor.zkjgy.online/library/mqtt-iot-mapper:latest
```

> **注意**：Dockerfile 中已内置 `go mod tidy`，CI 构建无需预先生成 `go.sum`。
