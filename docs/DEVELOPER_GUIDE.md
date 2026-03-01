# reliableupload 开发者接入文档

本文档说明如何将 `reliableupload` 集成到你的业务系统，并稳定落地分钟任务与每日任务的可靠上报。

## 目录

- [1. 架构与职责](#1-架构与职责)
- [2. 核心流程](#2-核心流程)
- [3. 接口契约](#3-接口契约)
- [4. 仓储实现要求（MySQL/GORM）](#4-仓储实现要求mysqlgorm)
- [5. 引擎初始化与调度](#5-引擎初始化与调度)
- [6. 幂等与重试建议](#6-幂等与重试建议)
- [7. 启动恢复机制](#7-启动恢复机制)
- [8. 观测与告警](#8-观测与告警)
- [9. 生产检查清单](#9-生产检查清单)
- [10. 示例与源码定位](#10-示例与源码定位)
- [11. FAQ](#11-faq)

## 1. 架构与职责

框架负责“可靠性基础设施”，业务负责“数据与通道实现”。

框架负责：
- 任务编排（生产/上报）
- 状态机推进
- 重试控制
- 启动恢复
- 并发隔离

业务负责：
- `DataSource`（查数、编码、加密、分批）
- `Reporter`（上报到 SFTP/HTTP/Kafka 等）
- Repo（配置表、日志表、每日任务表）

## 2. 核心流程

### 2.1 分钟任务

1. `RunProducer` 计算时间窗（考虑 `DelaySeconds`）
2. 调用 `DataSource.FetchAndEncode`
3. 每个分片先写备份文件
4. 成功后写 `uploadlog` 为 `pending`
5. `RunUploader` 扫描 `pending` 并按顺序上报
6. 上报成功后标记 `uploaded`，失败则 `retry_count + 1`

### 2.2 每日任务

1. `RunDailyTask` 获取或创建 `daily_task_instance`
2. 若尚未生产完成，先分批生产并写 `daily_task_batch`
3. 扫描 `pending batch` 顺序上报
4. 全部成功后标记实例完成

## 3. 接口契约

接口定义见 [reliableupload/interfaces.go](../reliableupload/interfaces.go)。

### 3.1 DataSource

```go
type DataSource interface {
    FetchAndEncode(ctx context.Context, cfg TaskConfig, start, end time.Time) ([][]byte, error)
}
```

约束：
- 建议按 `[start, end)` 语义处理时间范围
- 返回顺序即上报顺序
- 每个 `[]byte` 对应一个上传文件

### 3.2 Reporter

```go
type Reporter interface {
    Upload(ctx context.Context, cfg TaskConfig, fileName string, data []byte) error
}
```

约束：
- 必须保证幂等（同一 `fileName` 重复执行不可重复入库）
- 推荐“已存在即成功”的语义
- 错误信息包含足够上下文（目标路径、状态码、原因）

### 3.3 仓储接口

- `TaskConfigRepo`
- `UploadLogRepo`
- `DailyTaskRepo`

实现时请确保：
- `FindPendingByCode` 按 `time_start ASC`
- `FindPendingBatches` 按 `batch_index ASC`
- `IncrRetry`/`IncrBatchRetry` 使用原子 SQL 更新

## 4. 仓储实现要求（MySQL/GORM）

本仓库示例已提供 GORM 版本：
- [example/mysql_repo.go](../example/mysql_repo.go)

关键索引建议：
- `uploadlog(task_code, status, time_start)`
- `daily_task_instance(task_code, status)`
- `daily_task_batch(instance_id, status, batch_index)`

关键唯一约束建议：
- `uploadlog.file_name`
- `daily_task_instance(task_code, task_date)`
- `daily_task_batch(instance_id, batch_index)`

## 5. 引擎初始化与调度

引擎定义见 [reliableupload/engine.go](../reliableupload/engine.go)。

最小初始化：

```go
engine := reliableupload.NewEngine(
    registry,
    cfgRepo,
    uploadLogRepo,
    dailyRepo,
    reliableupload.NewFSBackupStore("./backup"),
)
```

推荐调度：
- Cron A（每分钟）：`engine.RunProducer(ctx)`
- Cron B（每分钟，错峰 20~40 秒）：`engine.RunUploader(ctx)`
- 服务启动：`engine.OnStartup(ctx)`
- 每日触发：`engine.RunDailyTask(ctx, taskCode, date)`

## 6. 幂等与重试建议

推荐的 `Reporter` 实现策略：
1. 检查远端正式文件是否存在
2. 不存在时写入临时文件（如 `.tmp`）
3. 上传完成后原子 rename

这样可以处理：
- 上传成功但状态未更新
- 崩溃后重试
- 防止对端读取半文件

## 7. 启动恢复机制

### 7.1 分钟补窗

通过 `GetLastTimeEndByCode` 与当前窗口推导缺失分钟，逐分钟补跑。

### 7.2 每日续传

扫描 `running` 实例：
- 若生产未完成则继续生产
- 若生产完成则继续上传 `pending` 批次

## 8. 观测与告警

建议指标：
- `reliableupload_produce_seconds`
- `reliableupload_upload_seconds`
- `reliableupload_pending_total`
- `reliableupload_retry_total`
- `reliableupload_failed_total`

建议告警：
- 某 `task_code` 连续失败
- `pending` 持续堆积
- 重试次数超过阈值
- 每日任务未在窗口内完成

## 9. 生产检查清单

1. 每个启用 `task_code` 都注册了 `DataSource + Reporter`
2. 仓储查询顺序与索引正确
3. Reporter 幂等策略已验证
4. 备份目录可写，容量告警已配置
5. Cron A/B 已错峰
6. OnStartup 已在服务启动后执行
7. 历史数据归档任务已上线

## 10. 示例与源码定位

- 示例入口：[example/main.go](../example/main.go)
- MySQL Repo：[example/mysql_repo.go](../example/mysql_repo.go)
- 核心类型：[reliableupload/types.go](../reliableupload/types.go)
- 注册中心：[reliableupload/registry.go](../reliableupload/registry.go)
- 本地备份：[reliableupload/backup_fs.go](../reliableupload/backup_fs.go)

运行示例：

```bash
go run ./example
```

## 11. FAQ

Q: 新增数据类型要改框架吗？  
A: 不需要。新增配置并注册 `DataSource + Reporter` 即可。

Q: 只能用于 SFTP 吗？  
A: 不是。`Reporter` 可对接任意目标系统。

Q: 为什么强调“先备份再写 pending”？  
A: 保障“有日志就有文件”，重试时可稳定回放。
