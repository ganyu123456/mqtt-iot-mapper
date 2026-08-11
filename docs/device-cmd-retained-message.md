# device/{id}/cmd 全量参数下发方案

## 一、问题背景

当前 MQTT-IoT-Mapper 向网关设备下发控制命令时，存在三个问题：

### 1.1 逐字段下发，网关无法获得完整状态

设备有两个需要下发的控制参数 `taskControl` 和 `collectInterval`，当前 Mapper 每个参数单独发一条 cmd 消息：

```json
// 第 1 条
topic: device/sis/cmd
{ "params": {"taskControl": "1"} }

// 第 2 条
topic: device/sis/cmd
{ "params": {"collectInterval": "30"} }
```

### 1.2 网关重启后收不到之前的命令

cmd 消息未设置 MQTT retained 标志。正常情况下平台下发一次指令后网关已经执行完毕，不会再次订阅 cmd topic。但当网关异常重启后，需要重新知道"我应该处于什么状态"——此时 EMQX 没有保留最后一条指令，网关订阅 cmd topic 后收不到任何历史消息，不知道自己应该开始采集还是不采集。

### 1.3 边缘组件重启后 cmd 消息丢失

EMQX 是边缘节点的 MQTT broker，与 Mapper 同节点部署。当边缘节点异常重启时，Mapper 和 EMQX 并发启动。EMQX 启动较慢，Mapper 首次连接失败后缺少有效的重试机制，会导致 Mapper 进入"僵尸状态"——进程在运行但 MQTT 未连通，设备离线但是平台无法感知。

---

## 二、方案说明

### 2.1 核心思路

将 cmd 消息改为**全量参数一次下发 + MQTT retained 持久化**。

```
CloudCore Device Twin (真相源)
       │
       ▼
Mapper 下发全量 cmd ──→ EMQX retained 持久化 ──→ 网关
       │                      │
   每次 Twin 变化         EMQX 保留最后一条
   都会重建              cmd 用于网关恢复
```

**两个关键改变：**

- **全量参数**：一条 cmd 消息包含设备所有可写属性的 desired 值，而不是拆成多条。网关收到后按参数 key 解析即可。
- **retained 标记**：消息发布时设置 MQTT retained 标志，EMQX 持久化保存最后一条。任何网关重新订阅 `device/{id}/cmd` 时，EMQX 主动推送这条消息。

### 2.2 cmd 消息格式

```json
topic: device/sis/cmd

{
  "requestId": "CMD-1786411358516-2400",
  "deviceId": "sis",
  "params": {
    "taskControl": "1",
    "collectInterval": "30"
  },
  "timestamp": 1786411358516
}
```

`params` 包含了设备当前**所有可下发参数的完整值**，网关收到后即可知道自己应该处在什么运行状态。

### 2.3 网关适配说明

cmd 消息的 `params` 字段结构未变，只是从包含单个 key 变为包含多个 key。网关只需从原来的"取固定 key"改为"遍历 params map 取值"：

```
收到 cmd → 遍历 params 所有 key → 根据 key 执行对应操作
```

### 2.4 Mapper 首次连接重试

Mapper 启动时如果 EMQX 还未就绪，会以指数退避方式自动重试连接（1秒 → 2秒 → 4秒 → 8秒 → 16秒 → 30秒），最长等待约 4 分钟。重试期间持续向 EdgeCore 上报设备 `Unknown` 状态，确保平台能感知到设备在线情况。

---

## 三、异常场景恢复机制

### 3.1 网关异常重启

这是最常见的工业现场场景——网关设备因断电、看门狗复位等重启，启动后需要恢复之前的运行状态。

```
网关重启 → 重新订阅 device/{id}/cmd
  → EMQX 推送 retained 消息 (包含全量参数)
  → 网关收到完整控制状态
  → 无需平台、EdgeCore、Mapper 任何干预
```

### 3.2 EMQX 异常重启

EMQX 重启后 retained 消息会丢失。Mapper 通过 AutoReconnect 机制感知到连接恢复后，从自身缓存中重发当前的全量 cmd 并重新设置 retained 标记，确保 EMQX 中的 retained 消息与当前 Twin 状态一致。

```
EMQX 重启 → retained 消息丢失
  → Mapper 检测到重连成功
  → 从缓存重发全量 cmd + retained
  → EMQX retained 消息恢复
```

### 3.3 Mapper 异常重启

Mapper 重启后从 EdgeCore 重新拉取完整的 Device Twin 信息，遍历所有可写属性的 desired 值，一次性下发全量 cmd（含 retained 标记），EMQX 和网关收到最新的控制状态。

```
Mapper 重启 → RegisterMapper → 拉取 Device Twin
  → 收集所有 desired 值 → 下发全量 cmd + retained
```

### 3.4 全节点重启（最极端场景）

边缘节点整体电力恢复后，所有组件并发启动、EMQX 启动慢、网络就绪时间不确定。

```
节点重启
  → Mapper 连接 EMQX 指数退避重试（最长约 4 分钟）
  → 期间上报设备 Unknown 给平台
  → EMQX 就绪 → 连接成功
  → 从 Device Twin 重建全量 cmd + retained
  → 网关收到 retained cmd → 恢复正常
```

---

## 四、场景覆盖一览

| 故障场景 | 恢复方式 | 是否需要人为干预 |
|---------|---------|----------------|
| 网关重启 | EMQX 推送 retained cmd | 否 |
| EMQX 重启 | Mapper 重连后从缓存重建 retained | 否 |
| Mapper 重启 | 从 Device Twin 重建全量 cmd | 否 |
| 全节点重启 | 连接重试 + Twin 重建 retained | 否 |
| 平台下发控制命令 | UpdateDevTwins 触发全量 cmd 下发 | 否（正常流程） |
| MQTT 断连期间 | 上报 Unknown 状态给平台 | 否 |
