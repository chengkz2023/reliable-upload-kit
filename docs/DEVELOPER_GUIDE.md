# reliableupload 开发者接入文档

`reliableupload` 现在采用三类任务模型：
- 分钟任务（小批量，自动窗口）
- 自定义大任务（任意时间窗口，由业务触发）
- 业务触发任务（无强时间条件，由 trigger_key 触发）

## 1. 接口契约

定义见 [reliableupload/interfaces.go](../reliableupload/interfaces.go)。

```go
type DataSource interface {
    CountChunks(ctx context.Context, cfg TaskConfig, start, end time.Time) (int, error)
    FetchChunk(ctx context.Context, cfg TaskConfig, start, end time.Time, index int) (Chunk, error)
}

type Reporter interface {
    Upload(ctx context.Context, cfg TaskConfig, item UploadItem) error
}
```

业务触发任务里可通过上下文读取触发参数：

```go
trigger, ok := reliableupload.BizTriggerFromContext(ctx)
```

## 2. 引擎能力

定义见 [reliableupload/engine.go](../reliableupload/engine.go)。

- `RunProducer(ctx)`：扫描所有分钟任务并生产
- `RunUploader(ctx)`：兼容入口，统一上传分钟/大任务/业务任务 pending
- `RunMinuteUploader(ctx)`：仅上传分钟任务 pending
- `RunBigUploader(ctx)`：仅上传大任务 pending
- `RunBizUploader(ctx)`：仅上传业务任务 pending
- `OnStartup(ctx)`：分钟补窗 + 恢复 running 大任务 + 恢复 running 业务任务
- `RunBigTask(ctx, taskCode, start, end)`：创建/恢复一个大任务窗口
- `RunBizTask(ctx, taskCode, triggerKey, triggerPayload)`：创建/恢复一个业务触发任务实例

## 3. 幂等建议

`RunBizTask` 建议使用稳定的业务键作为 `triggerKey`，例如：
- 审批单号
- 结算批次号
- 活动ID+版本号

并在实例表上建立唯一键 `(task_code, trigger_key)`，实现触发幂等。

生产侧并发幂等建议：
- `UploadLogRepo.Create`
- `BigTaskRepo.CreateBatch`
- `BizTaskRepo.CreateBatch`

当遇到唯一键冲突时，Repo 实现应返回 `reliableupload.ErrAlreadyExists`（可 wrap）。引擎会将其视为并发下的幂等冲突并继续执行。

## 4. MySQL/GORM 示例

可参考：
- [example/main.go](../example/main.go)
- [example/mysql_repo.go](../example/mysql_repo.go)

示例表：
- `uploadlog`
- `big_task_instance`
- `big_task_batch`
- `biz_task_instance`
- `biz_task_batch`
