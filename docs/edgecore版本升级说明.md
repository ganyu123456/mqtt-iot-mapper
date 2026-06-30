# EdgeCore DMI 组件缺陷说明及升级建议

## 一、问题背景

生产环境中修改 device CRD 的 writable twin desired 值后，MQTT IoT Mapper 无法自动下发 cmd 指令到 IoT 设备。经完整排查，定位到 **EdgeCore DMI 模块存在已知缺陷**。

## 二、期望行为 vs 实际行为

### 2.1 完整数据流

```
CloudCore ──device CRD 变更──▶ EdgeCore ──DMI gRPC UpdateDevice──▶ Mapper ──SetDeviceData──▶ MQTT device/{id}/cmd ──▶ IoT 设备
```

### 2.2 期望行为

修改 `device.yaml` 中 writable property 的 `desired.value`：

```yaml
spec:
  properties:
  - name: collectInterval
    desired:
      value: "20"     # ← 从 5 改为 20
```

Mapper 应立即收到更新，调用 `SetDeviceData`，MQTT topic `device/sis/cmd` 收到对应 cmd 消息。

### 2.3 实际行为

| 操作 | 期望 | 实际 |
|------|------|------|
| 第 1 次修改 device.yaml | cmd 下发成功 | **无 cmd 消息** |
| 第 2 次修改 device.yaml | cmd 下发成功 | cmd 下发成功 |
| Mapper Pod 重启 | 自动重连 | **mapper 卡死，所有 device 操作失败** |

## 三、故障链路分析（日志证据）

### 3.1 故障场景一：首次修改不生效

**操作**：修改 device CRD desired value

**Mapper 日志**：无任何 `[DIAG] UpdateDev called` 日志 — 说明 mapper 根本没收到 EdgeCore 的更新通知。

**EdgeCore 日志**：

```
dmiworker.go:122] Overriding device properties for device sis-gateway-bz-001 using model sis-gateway-model
dmiworker.go:154] Merged instance visitors for property collectInterval of device sis-gateway-bz-001
dmiworker.go:257] udpate device sis-gateway-bz-001 failed with err: fail to get dmi client of protocol mqtt-iot-mapper
dmiworker.go:84]  DMIModule deal MetaDeviceOperation event failed: fail to get dmi client of protocol mqtt-iot-mapper
```

**分析**：

- EdgeCore 从 CloudCore 收到了 device 变更事件 → **上游正常**
- `dmiworker.go` 正确解析了 device model 和 property (merge visitors) → **解析正常**
- 往 mapper 下发更新时，`dmiworker.go:257` 找不到 protocol 为 `mqtt-iot-mapper` 的 DMI gRPC 客户端 → **下发失败**

EdgeCore 内部维护了一个 `protocol → gRPC client` 的映射表。Mapper 首次注册时写入该映射。当映射丢失后，EdgeCore 无法将更新推送给 mapper，mapper 无任何感知。这就解释了为什么 "第一次修改不生效" — EdgeCore 知道 device 变了，但找不到 mapper 的 gRPC 连接。

**为什么第二次修改又生效了？** CloudCore 重试机制在第二次下发时，EdgeCore 可能通过内部重连重新建立映射，从而推送成功。但这个恢复不稳定。

### 3.2 故障场景二：Mapper 重启后完全失联

**操作**：kubectl delete pod（模拟 mapper 重启）

**Mapper 日志（仅 2 行，此后永久卡死）**：

```
I0630 21:26:55.544770 main.go:24] config: &{GrpcServer:{SocketPath:/etc/kubeedge/mqtt-iot-mapper.sock} Common:{Name:mqtt-iot-mapper Version:v1.0.0 APIVersion:v1.0.0 Protocol:mqtt-iot-mapper Address:127.0.0.1 EdgeCoreSock:/etc/kubeedge/dmi.sock}}
I0630 21:26:55.545179 main.go:26] Mapper will register to edgecore
# ← 第 3 条日志 "Mapper register finished" 永远不会出现，进程挂死
```

**分析**：Mapper 启动时调用 `RegisterMapper(true)` 向 EdgeCore 注册。该函数内部使用 `grpc.WithBlock()` 连接 EdgeCore 的 Unix socket (`dmi.sock`)。由于 EdgeCore DMI 模块在处理重新注册时存在缺陷，对注册请求无响应，导致 mapper gRPC 调用永久阻塞。

**验证了 socket 本身正常**：

```
# ss -lpn | grep dmi
u_str LISTEN 0 0 /etc/kubeedge/dmi.sock ... users:(("edgecore",pid=3571122,fd=16))

# kubectl exec mapper -- ls -la /etc/kubeedge/dmi.sock
srwxr-xr-x root root 0 Jun 30 21:22 /etc/kubeedge/dmi.sock
```

- EdgeCore 在监听 socket → 监听正常
- Mapper 容器内可见 socket → 挂载正常
- 但 mapper 就是连不上 → DMI 模块内部逻辑缺陷

**EdgeCore 日志（持续报错，所有 device 操作全部失败）**：

```
dmiworker.go:257] udpate device sis-gateway-bz-001 failed with err: fail to get dmi client of protocol mqtt-iot-mapper
dmiworker.go:84]  DMIModule deal MetaDeviceOperation event failed: fail to get dmi client of protocol mqtt-iot-mapper
dmiworker.go:282] delete device model sis-gateway-model failed with err: fail to get dmi client of protocol mqtt-iot-mapper
dmiworker.go:239] delete device sis-gateway-bz-001 failed with err: fail to get dmi client of protocol mqtt-iot-mapper
dmiworker.go:276] add device model sis-gateway-model failed with err: fail to get dmi client of protocol mqtt-iot-mapper
dmiworker.go:233] add device sis-gateway-bz-001 failed with err: fail to get dmi client of protocol mqtt-iot-mapper
```

所有 device 操作（新增/更新/删除）全部因找不到 DMI client 而失败，设备管理功能完全瘫痪。

## 四、根因定位

### 4.1 代码层面

EdgeCore `dmiworker.go:257` — DMI 内部维护一个 `protocolName → DMIClient` 的映射缓存。Mapper 重启时调用 `RegisterMapper` 重新注册，但 **EdgeCore 没有正确更新该映射缓存**，导致旧的（已断开的）mapper 连接信息仍然占据该 protocol 槽位，新的注册请求又无法覆盖它。结果是：

- EdgeCore 无法将 device 变更推送给 mapper（找不到可用的 client）
- Mapper 注册请求被阻塞（EdgeCore 不响应）

### 4.2 社区确认

该问题已被 KubeEdge 社区确认为已知缺陷：

- **GitHub Issue**: [#6597](https://github.com/kubeedge/kubeedge/issues/6597)
- **问题描述**: DMI client cache not updated after mapper reconnect
- **影响范围**: 所有使用 DMI v1beta1 接口的 device mapper

### 4.3 修复版本

该缺陷在 **KubeEdge v1.23.0** 中已修复。

## 五、当前环境

| 组件 | 当前版本 | 目标版本 |
|------|----------|----------|
| EdgeCore | v1.22.0 | **>= v1.23.0** |
| mapper-framework | v1.21.0 | 随 EdgeCore 升级同步至 v1.23.0+ |

## 六、临时缓解方案（不推荐长期使用）

`systemctl restart edgecore` 可临时恢复。原因是 EdgeCore 重启时会从本地 DB 重新加载 mapper 注册信息并重建 DMI client 映射。但 **每次 mapper 重启后故障必然复现**，且 mapper 有概率卡死，需要同时重启 mapper pod 才能恢复。

## 七、建议

将 EdgeCore 升级至 **v1.23.0 或更高版本**，以彻底修复 DMI 客户端映射管理缺陷。
