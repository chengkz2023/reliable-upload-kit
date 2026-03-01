# reliableupload 开发者接入文档

本文档面向需要把 `reliableupload` 集成进业务系统的开发者，目标是让你快速稳定地落地：
- 多数据类型上报（按 `task_code` 配置化）
- 崩溃可恢复
- 幂等重试
- 分钟任务补窗 + 每日任务断点续传

## 1. 设计目标

框架只负责可靠性与流程编排，不侵入业务查询和具体上报通道。

- 框架负责：
  - 生产调度（Cron A）
  - 上报调度（Cron B）
  - 状态机（pending/uploaded/failed/running）
  - 启动恢复（OnStartup）
  - 按 `task_code` 隔离并发
- 业务方负责：
  - `DataSource`：查数 + 编码/加密
  - `Reporter`：上报到目标系统（SFTP/HTTP/Kafka...）
  - 仓储实现（MySQL/GORM/SQLX）

## 2. 核心概念与流程

### 2.1 三个关键原则

1. 备份文件是唯一重试数据源  
生产成功后先落盘，再写 `pending` 日志；重试只读备份，不重查业务库。

2. 状态机驱动恢复  
重启后扫描状态表即可恢复到中断点。

3. 生产与上报解耦  
Cron A 只生产，Cron B 只上报，互相独立。

### 2.2 运行时流程

1. `RunProducer`（分钟任务）
   - 按配置计算时间窗
   - 调用 `DataSource.FetchAndEncode`
   - 分批落备份
   - 写 `uploadlog(status=pending)`
2. `RunUploader`
   - 扫描分钟 `pending`，按 `task_code` 并发上报
   - 扫描每日 `running` 实例，继续上报 `pending batch`
3. `OnStartup`
   - 分钟任务补跑空窗
   - 恢复每日 `running` 任务

## 3. 公共接口契约

定义见 [interfaces.go](/D:/goworkspace/smart-upload/reliableupload/interfaces.go)。

### 3.1 DataSource（业务必实现）

```go
type DataSource interface {
    FetchAndEncode(ctx context.Context, cfg TaskConfig, start, end time.Time) ([][]byte, error)
}
```

契约要求：
- 入参 `start/end` 为闭开区间 `[start, end)` 语义使用最安全。
- 返回的每个 `[]byte` 对应一个待上报文件（一个批次）。
- 返回顺序即上报顺序，建议业务侧稳定排序。
- 方法应是纯函数式行为：同时间窗重复调用尽量得到一致结果（便于排障）。

### 3.2 Reporter（业务必实现）

```go
type Reporter interface {
    Upload(ctx context.Context, cfg TaskConfig, fileName string, data []byte) error
}
```

契约要求：
- 必须保证幂等：同一 `fileName` 重复上传不应造成重复入库。
- 推荐语义：远端已存在则返回成功（或可识别的“已存在”后映射为成功）。
- 错误应尽可能携带可观测上下文（远端路径、状态码、失败原因）。

### 3.3 仓储接口（业务必实现）

框架通过仓储接口访问状态数据，不直接耦合数据库：
- `TaskConfigRepo`
- `UploadLogRepo`
- `DailyTaskRepo`

定义见 [interfaces.go](/D:/goworkspace/smart-upload/reliableupload/interfaces.go)。

实现注意：
- `FindPendingByCode` 必须按 `time_start ASC` 返回，保证同任务顺序。
- `FindPendingBatches` 必须按 `batch_index ASC` 返回。
- 重试计数方法需具备并发安全（建议单 SQL 原子更新）。

## 4. 任务配置模型

结构定义见 [types.go](/D:/goworkspace/smart-upload/reliableupload/types.go) `TaskConfig`。

核心字段说明：
- `TaskCode`：任务唯一标识
- `TaskType`：`1=minute, 2=daily`
- `DelaySeconds`：分钟任务延迟（防止采到未落稳数据）
- `BatchSize`：单批大小（供 `DataSource` 内部使用）
- `MaxRetry`：最大重试次数
- `SFTPSubdir`：远端子目录（也可泛化为“逻辑目标路径”）
- `FilePrefix`：文件名前缀
- `Enabled`：是否启用

## 5. 引擎 API 使用方式

定义见 [engine.go](/D:/goworkspace/smart-upload/reliableupload/engine.go)。

### 5.1 初始化

```go
registry := reliableupload.NewRegistry()
registry.RegisterDataSource("order_minute", orderDS)
registry.RegisterReporter("order_minute", orderReporter)

engine := reliableupload.NewEngine(
    registry,
    cfgRepo,
    uploadLogRepo,
    dailyRepo,
    reliableupload.NewFSBackupStore("./backup"),
)
```

### 5.2 调度建议

- Cron A（每分钟）：`engine.RunProducer(ctx)`
- Cron B（每分钟，建议与 A 错开 20~40 秒）：`engine.RunUploader(ctx)`
- 服务启动：`engine.OnStartup(ctx)`
- 每日任务触发（例如 T+1 00:10）：`engine.RunDailyTask(ctx, "order_daily", yesterday)`

## 6. 推荐文件命名与幂等策略

默认命名器（已内置）：
- 分钟任务：`{prefix}_{start}_{end}_{index:03d}.dat`
- 每日任务：`{prefix}_{date}_{index:03d}.dat`

如需自定义可实现 `FileNamer`（见 [interfaces.go](/D:/goworkspace/smart-upload/reliableupload/interfaces.go)）。

Reporter 侧幂等建议：
1. 先判断远端是否存在目标文件
2. 不存在则上传临时名（例如 `.tmp`）
3. 上传完成后原子 rename 到正式名

## 7. 数据库实现建议（MySQL）

你可以按方案文档中表结构落地。下面是实践重点。

### 7.1 分钟日志索引

`uploadlog` 必须包含复合索引：
- `(task_code, status, time_start)`

用途：
- Cron B 扫描 `pending` 时避免全表扫描

### 7.2 每日任务索引

- `daily_task_instance(task_code, status)`
- `daily_task_batch(instance_id, status, batch_index)`

### 7.3 唯一约束

- 文件名全局唯一（`uk_file_name`）
- 每日实例 `(task_code, task_date)` 唯一
- 批次 `(instance_id, batch_index)` 唯一

### 7.4 归档建议

主表保留近 30~90 天；历史已完成/失败记录归档到历史表，避免索引膨胀。

## 8. 错误处理与重试语义

### 8.1 分钟任务

- 上传失败：
  - `IncrRetry`
  - 当前 `task_code` 本轮停止继续上传（顺序保证）
  - 其他 `task_code` 不受影响
- 上传成功：
  - `MarkUploaded`

### 8.2 每日任务

- 批次失败：
  - `IncrBatchRetry`
  - 当前实例暂停，等待下轮或启动恢复
- 全部批次成功：
  - `MarkInstanceCompleted`

## 9. 启动恢复机制

### 9.1 分钟补窗

根据 `GetLastTimeEndByCode` 与当前截止窗口之间缺口逐分钟补跑。

### 9.2 每日续传

扫描 `running` 实例：
- 若已生产批次数 < `total_batches`：继续生产
- 否则继续上传 `pending` 批次

## 10. 可观测性建议

建议按 `task_code` 打维度化日志与指标：
- 生产耗时、上报耗时、成功率、重试次数
- pending 数量、failed 数量
- 每日任务 uploaded_batches / total_batches

推荐指标：
- `reliableupload_produce_seconds`
- `reliableupload_upload_seconds`
- `reliableupload_pending_total`
- `reliableupload_retry_total`
- `reliableupload_failed_total`

## 11. 生产落地检查清单

1. 所有启用 `task_code` 均注册 `DataSource + Reporter`
2. 仓储查询顺序与索引正确
3. Reporter 幂等策略已实现并压测
4. 备份目录可写，磁盘容量告警已接入
5. Cron A/B 已错峰
6. OnStartup 在进程启动后执行
7. 归档任务已上线
8. 告警规则已配置（失败、重试超限、pending 堆积）

## 12. 本仓库示例代码

- 示例入口：[main.go](/D:/goworkspace/smart-upload/example/main.go)
- 文件备份实现：[backup_fs.go](/D:/goworkspace/smart-upload/reliableupload/backup_fs.go)
- 引擎实现：[engine.go](/D:/goworkspace/smart-upload/reliableupload/engine.go)

示例运行：

```bash
go run ./example
```

## 13. 常见问题（FAQ）

Q1：为什么要“先备份再写 pending”？  
A：保证“日志存在 => 文件必存在”，恢复时才能稳定读到重试数据源。

Q2：是否可以只实现 DataSource，不实现 Reporter？  
A：不可以。框架只编排流程，具体上报通道由 Reporter 决定。

Q3：支持非 SFTP 吗？  
A：支持。Reporter 可以是 HTTP、对象存储、消息队列等任何目标。

Q4：新增一种数据类型是否要改框架代码？  
A：不需要。新增配置 + 注册实现即可。
