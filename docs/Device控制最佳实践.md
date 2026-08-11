# KubeEdge MQTT-IoT-Mapper Device 控制最佳实践

> 基于 KubeEdge 设计哲学，指导 Mapper 中 Device 控制命令（cmd）的可靠投递实现。

---

## 一、设计原则

### 1.1 KubeEdge 核心设计哲学

| 原则 | 说明 |
|------|------|
| **Device Twin 为唯一真相源** | 设备期望状态通过 Kubernetes CRD 表达，`desired` 值是控制指令的唯一权威来源。 |
| **声明式控制** | 运维方声明"设备应该是什么状态"，系统负责将实际状态向期望状态收敛。这是 Kubernetes Controller 模式的核心思想。 |
| **边缘自治** | 当云边网络不稳定或边缘组件重启时，边缘节点应自主运行，不应依赖云端干预来恢复服务。 |
| **可靠消息投递** | 各层级需有各自的持久化和恢复机制，保证消息不丢失。 |

### 1.2 Mapper 在 KubeEdge 架构中的角色

```
CloudCore                         EdgeCore                        Mapper + 设备
────────                          ────────                        ────────────
DeviceController ────同步────→ DeviceTwin (SQLite持久化) ──gRPC──→ CustomizedClient
(CRD desired值)                  (边端本地缓存)                    (协议翻译层)
                                                                      │
                                                               MQTT cmd → 网关
                                                               网关 status → Mapper
```

Mapper 是 KubeEdge 设备管理链路的**最后一公里**。KubeEdge 框架已经将 `desired`（期望状态）从云端可靠同步到了边缘节点，Mapper 的职责是将期望状态翻译为设备协议命令，并将设备上报的 `actual`（实际状态）反馈到链路中。

**如果 Mapper 丢失了控制指令，前面 CloudCore → EdgeCore 的可靠性保障全部失效。**

### 1.3 声明式控制 vs 命令式控制

| | 命令式 | 声明式（本方案） |
|------|-------|----------------|
| 模式 | 平台下发"执行某个操作" | 平台声明"设备应该处于某个状态" |
| 状态感知 | 不关心设备当前状态 | 持续对比 desired vs actual |
| 故障恢复 | 需重新下发指令 | 自动发现漂移并修复 |
| 对应模型 | RPC 调用 | Kubernetes Controller Reconcile |

声明式控制更契合 KubeEdge 基于 CRD 的设备管理模式——Device Twin 的 `desired` 字段本身就是声明式的，Mapper 只需在 MQTT 层闭环这个调谐逻辑。

---

## 二、Reconcile Loop（调谐循环）模型

### 2.1 核心思路

Mapper 周期性地对比两个数据源：

```
Device Twin desired (CloudCore 同步)      网关 MQTT status 上报 actual
         │                                          │
         └──────────── 对比 desired vs actual ───────┘
                              │
                    不一致 → 下发 cmd
                      一致 → 无操作
```

这是 Kubernetes Controller 的标准模式，对标 `kube-controller-manager` 中的各类控制器。

### 2.2 与 KubeEdge DeviceTwin 的角色分工

```
KubeEdge DeviceTwin ──── 负责 desired ↔ reported 在 EdgeCore 层的对比
Mapper Reconcile     ──── 负责 desired ↔ actual   在 MQTT 协议层的对比
```

EdgeCore 的 DeviceTwin 保证了 CRD 层面的 desired 值正确到达边端。Mapper 的 Reconcile Loop 保证了这个 desired 值真正到达了物理设备并被执行了。

### 2.3 数据流

```
平台修改 Twin desired (如 taskControl: 0→1)
  → CloudCore → EdgeCore
  → Mapper UpdateDevTwins()
  → UpdateDev() → start()
  → setVisitor() → 逐字段下发 cmd

同时，周期性 Reconcile:
  for 每个 ReadWrite 属性:
    desired = Twin.desired.value          // 从 Device CRD 来
    actual  = IoTGateway.GetProperty(key) // 从 MQTT status 来
    
    if desired != "" && desired != actual:
        SendCmd(key, desired)   ← 单字段下发，与现有接口一致
```

cmd 消息格式不变，仍为逐字段下发。网关无需修改任何代码。


---

## 三、异常场景恢复机制

### 3.1 网关异常重启

网关重启后内存状态丢失，status 上报空值或零值：

```
网关重启
  → 重新连接 MQTT
  → status 上报空值/零值
  → Mapper Reconcile 逐字段对比:
       taskControl:  desired=1 vs actual="" → 不一致 → SendCmd(taskControl, 1)
       collectInterval: desired=30 vs actual="" → 不一致 → SendCmd(collectInterval, 30)
  → 网关收到 → 恢复运行态 → status 回写
  → 下一个周期对比 → desired=1 vs actual=1 → 一致 → 不再下发

延迟: 一个采集周期 (默认 5s)
触发源: Mapper Reconcile Loop
```

### 3.2 平台重新下发相同值

这是云边协同中最容易出问题的场景——值没变，Delta 不触发：

```
之前: 平台设 taskControl=1 → 下发成功 → 网关正常运行
之后: 网关异常重启，状态丢失
      → 平台再次设 taskControl=1 → EdgeCore 发现 desired 未变化 → 不下发
      → (这是 Twin Delta 机制的正常行为，但物理设备确实丢了这个值)

Reconcile Loop 的解决:
  → 平台设 taskControl=1 → desired 保持 1
  → 网关 status 上报 actual=0 (重启后内存丢失)
  → Mapper 对比: desired=1 vs actual=0 → 不一致
  → 自动重发 cmd(taskControl, 1)
  → 平台无需感知，自动修复
```

### 3.3 Mapper 重启

```
Mapper 重启
  → RegisterMapper → 从 EdgeCore 拉取 Device Twin (真相源)
  → DevInit → start()
  → dataHandler() 启动 Reconcile Loop
  → 从 Twin 读取 desired: {taskControl: 1, collectInterval: 30}
  → 从 MQTT status 读取 actual: {taskControl: 1, collectInterval: 30}
  → 对比一致 → 无操作 (网关本身正常运行，不需要重发)

恢复来源: Device Twin (真相源)
```

### 3.4 EMQX 重启

```
EMQX 重启
  → MQTT AutoReconnect 恢复连接
  → 网关继续周期性上报 status
  → Mapper Reconcile 继续周期性对比
  → desired vs actual 一致 → 无操作

与网关重启的区别: EMQX 重启只是 broker 挂了，网关和 Mapper 的内存状态都没有丢失，
                    MQTT 恢复后系统自动回到稳态，不需要重发任何 cmd。
```

### 3.5 全节点重启

```
边缘节点整体重启 (网关 + EMQX + Mapper + EdgeCore)
  → Mapper connect 重试 (指数退避 1s→30s)
  → EMQX 就绪 → connect 成功
  → dataHandler() 启动 Reconcile Loop
  → status 上报空值 → 对比不一致 → 逐字段重发 cmd
  → MQTT 断连期间 DeviceStates 上报 Unknown 给 EdgeCore

恢复来源: Device Twin (真相源) + Reconcile Loop
```

---

## 四、场景覆盖一览

| 故障场景 | 恢复方式 | 是否需要人为干预 | 恢复延迟 |
|---------|---------|----------------|---------|
| 网关重启 | Reconcile 发现 desired≠actual → 重发 cmd | 否 | 一个采集周期 |
| 平台重发相同值 | Reconcile 发现 desired≠actual → 重发 cmd | 否 | 一个采集周期 |
| Mapper 重启 | 从 Twin 重建 desired → 对比 actual → 一致则不动 | 否 | 启动时立即 |
| EMQX 重启 | 重连后继续 reconcile → 一致则不动 | 否 | 实时 |
| 全节点重启 | connect 重试 → Twin 重建 → reconcile 修复 | 否 | 取决于 EMQX 启动时间 |
| 平台下发控制命令 | UpdateDevTwins → setVisitor 立即下发 | 否 | 立即 |

---

## 五、与 KubeEdge 设计模式的对应

| KubeEdge 组件 | 模式 | Mapper 对应实现 |
|--------------|------|---------------|
| DeviceController | Watch CRD 变化 → 同步到 EdgeCore | — (由 KubeEdge 框架完成) |
| DeviceTwin | desired/reported 对比 → 上云同步 | — (由 EdgeCore 完成) |
| **Mapper Reconcile** | **desired/actual 对比 → MQTT cmd** | **本方案实现** |
| kube-controller-manager | spec/status 对比 → 调谐 | 同模式，协议层翻译 |

Mapper 的 Reconcile Loop 不是凭空创造的——它是 Kubernetes Controller 模式在设备协议层的自然延伸。

---

## 六、实现要点

### 6.1 Reconcile Loop 的触发时机

1. **周期性触发**：以 Twin 采集周期为间隔，对比所有 ReadWrite 属性的 desired 值 vs 实际值
2. **事件触发**：收到 `UpdateDevTwins()` 回调时立即下发（与当前逻辑一致）

### 6.2 网关配合要求

网关 status 上报必须包含所有 ReadWrite 属性的当前实际值，且字段名与 cmd 参数一致：

```json
// 网关 status 上报 (device/{id}/status)
{
  "deviceId": "sis",
  "timestamp": 1786411358523,
  "status": {
    "taskControl": "1",
    "collectInterval": "30",
    "collectPointTotal": "100",
    "collectPointOnline": "98",
    "lastCollectTime": "1786343723265"
  }
}
```

`taskControl` 和 `collectInterval` 既是 cmd 下发字段也是 status 上报字段——Mapper 对比这两个的值来判断是否一致。

网关重启后这些字段会变成空值或零值，Mapper 即检测到漂移并自动修复。

### 6.3 性能开销

每个 Mapper 实例只管理一个设备，reconcile 循环的稳态开销极小：

| 操作 | 耗时 | 频率 |
|------|------|------|
| 解析 visitor config | 微秒级 | 每周期 2 次 |
| 读 status 内存缓存 | 纳秒级 | 每周期 2 次 |
| 字符串对比 | 纳秒级 | 每周期 2 次 |
| MQTT cmd 下发 | 仅在不一致时 | 仅发生一次（恢复后即停止） |

索引周期 5 秒，与 KubeEdge 默认 collectCycle 一致。稳态下 CPU 和网络开销可忽略不计。

### 6.4 无外部依赖

Reconcile Loop 的运行条件只有两个：

- Mapper 从 EdgeCore 拿到了 Device Twin（通过 gRPC）
- Mapper 收到了网关的 MQTT status 消息（通过 MQTT subscribe）

不依赖 EMQX retained 消息、不依赖额外的持久化存储、不依赖网关改造。

---

## 七、实现概要

### 7.1 Reconcile Loop 入口

`dataHandler()` 函数末尾启动 reconcile goroutine：

```go
// device/device.go
go startReconcileLoop(ctx, dev)
```

### 7.2 Reconcile Loop 逻辑

```go
func startReconcileLoop(ctx context.Context, dev *driver.CustomizedDev) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            for i := range dev.Instance.Twins {
                twin := &dev.Instance.Twins[i]
                // 跳过 ReadOnly 和无 desired 值的属性
                if twin.Property.PProperty.AccessMode == "ReadOnly" { continue }
                if twin.ObservedDesired.Value == "" { continue }

                // 从 MQTT status 读取设备实际值
                actualValue, _ := dev.CustomizedClient.GetDeviceData(&visitorConfig)
                actualStr, _ := actualValue.(string)

                // desired ≠ actual → 逐字段下发 cmd
                if twin.ObservedDesired.Value != actualStr {
                    setVisitor(&visitorConfig, twin, dev)
                }
            }
        case <-ctx.Done():
            return
        }
    }
}
```

### 7.3 改动清单

| 文件 | 位置 | 改动 | 状态 |
|------|------|------|------|
| `driver/iot-gateway.go` | `connect()` | 首次连接指数退避重试 (1s→30s, 最多 10 次) | 已完成 |
| `driver/iot-gateway.go` | 新增 `IsConnected()` | 暴露真实 MQTT 连接状态 | 已完成 |
| `driver/driver.go` | `GetDeviceStates()` | 检查 `IsConnected()`，MQTT 断连时上报 Unknown | 已完成 |
| `device/device.go` | `start()` | InitDevice 失败后启动 DeviceStates 上报 Unknown | 已完成 |
| `device/device.go` | 新增 `startReconcileLoop()` | 周期性对比 desired vs actual，不一致时逐字段重发 cmd | 已完成 |
| `device/device.go` | `dataHandler()` | 末尾启动 reconcile goroutine | 已完成 |

**网关侧无需改动**。cmd 消息格式不变，仍为单字段逐条下发。

---

## 八、可靠性边界

```
Cloud ────→ EdgeCore ──gRPC UDS──→ Mapper ──MQTT──→ EMQX ──MQTT──→ 网关
 │              │                    │                │           │
KubeEdge框架    KubeEdge框架       本方案覆盖       MQTT QoS1   设备自身
WebSocket重连   SQLite            connect重试                 
                DeviceTwin        Reconcile Loop
                                  离线上报
```

- **EdgeCore → Mapper**：KubeEdge 框架保障
- **Mapper Reconcile Loop**：本方案核心——持续对比 desired vs actual，发现漂移自动修复
- **EMQX → 网关**：MQTT QoS 1 保障消息投递，非 Mapper 职责

### 可选增强：端到端确认

如果业务需要在平台上看到"cmd 确实被设备执行了"：

1. 网关执行完 cmd 后在 status 中回写执行结果
2. Mapper `handleStatus()` 检测结果并更新 Twin `reported` 值
3. 云端可通过 Device CRD `reported` 字段确认执行状态

此机制在 Reconcile Loop 的基础上实现成本极低，属于可选增强。
