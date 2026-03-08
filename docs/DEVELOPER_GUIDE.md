# reliableupload 开发者接入文档

`reliableupload` 现在采用两类任务模型：
- 分钟任务（小批量，自动窗口）
- 自定义大任务（任意时间窗口，由业务触发）

## 1. 核心变化

- 已去掉“每日任务”固定概念
- 改为 `TaskTypeBig` + `RunBigTask(ctx, taskCode, windowStart, windowEnd)`
- 大任务可用于每日、每小时、每 10 分钟或任意补数窗口

## 2. 接口契约

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

- `DataSource` 统一负责分钟任务/大任务的数据分批生产
- `Reporter` 统一负责上报并可直接使用 `item.BizKey/item.Meta`

## 3. 引擎能力

定义见 [reliableupload/engine.go](../reliableupload/engine.go)。

- `RunProducer(ctx)`：扫描所有分钟任务并生产
- `RunUploader(ctx)`：兼容入口，统一上传分钟与大任务 pending
- `RunMinuteUploader(ctx)`：仅上传分钟任务 pending
- `RunBigUploader(ctx)`：仅上传大任务 pending
- `OnStartup(ctx)`：分钟补窗 + 恢复 running 大任务
- `RunBigTask(ctx, taskCode, start, end)`：创建/恢复一个大任务窗口

自由触发 API（推荐）：
- `ProduceCurrentWindowForTask(ctx, taskCode)`
- `ProduceForTask(ctx, taskCode, start, end)`
- `UploadPendingForTask(ctx, taskCode)`

## 4. 日志接入（zap）

可直接使用函数注入：

```go
engine := reliableupload.NewEngine(
    registry, cfgRepo, uploadLogRepo, bigRepo, backup,
    reliableupload.WithLoggerFuncs(
        func(format string, args ...any) { zap.L().Sugar().Infof(format, args...) },
        func(format string, args ...any) { zap.L().Sugar().Errorf(format, args...) },
    ),
)
```

## 5. 文件名策略

默认文件名可用；若某个 `task_code` 需要特殊规则：

```go
registry.RegisterFileNamer("order_big", myNamer)
```

框架会优先使用任务级命名器，没有则回退全局命名器。

## 6. MySQL/GORM 示例

可参考：
- [example/main.go](../example/main.go)
- [example/mysql_repo.go](../example/mysql_repo.go)

示例表：
- `uploadlog`
- `big_task_instance`
- `big_task_batch`

## 7. 调度建议

- Cron A（每分钟）：`RunProducer`
- Cron B（每分钟，错峰 20~40 秒）：可按需选择
- 仅分钟任务上传：`RunMinuteUploader`
- 仅大任务上传：`RunBigUploader`
- 或兼容入口：`RunUploader`
- 自定义调度器：按你的业务频率触发 `RunBigTask`
- 服务启动后执行一次：`OnStartup`

