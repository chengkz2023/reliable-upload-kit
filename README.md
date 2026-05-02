# reliable-upload-kit

面向生产环境的 Go 可靠上报框架：
- 分钟任务（小批量）
- 自定义大任务（任意时间窗口）
- 业务触发任务（无强时间条件）

## 功能特性

- 备份文件作为重试唯一数据源
- 状态机驱动（`pending` / `uploaded` / `failed` / `running`）
- 生产与上报解耦（Cron A / Cron B）
- 按 `task_code` 隔离并发
- 大任务/业务任务支持按任务配置单实例生产并发，缓解数据倾斜长尾
- 启动恢复（分钟补窗 + 大任务/业务任务断点续传）
- 按 `task_code` 自由触发生产/上报
- 按 `task_code` 自定义文件命名
- `WithLoggerFuncs` 便于接入 zap

## 文档导航

- 开发者接入文档: [docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md)
- 示例入口: [example/main.go](example/main.go)
- MySQL Repo 示例: [example/mysql_repo.go](example/mysql_repo.go)

## 快速开始

```bash
go get github.com/chengkz2023/reliable-upload-kit/reliableupload
```

先设置 MySQL DSN：

```powershell
$env:MYSQL_DSN = "root:password@tcp(127.0.0.1:3306)/smart_upload?charset=utf8mb4&parseTime=True&loc=Local"
cd example
go mod tidy
go run .
```

`example/main.go` 不再内置默认 DSN，未设置 `MYSQL_DSN` 会直接退出。

## 核心接口

```go
type DataSource interface {
    CountChunks(ctx context.Context, cfg TaskConfig, start, end time.Time) (int, error)
    FetchChunk(ctx context.Context, cfg TaskConfig, start, end time.Time, index int) (Chunk, error)
}

type Reporter interface {
    Upload(ctx context.Context, cfg TaskConfig, item UploadItem) error
}
```

业务触发上下文可通过 helper 读取：

```go
trigger, ok := reliableupload.BizTriggerFromContext(ctx)
```

## 核心入口

- `RunProducer(ctx)`
- `RunUploader(ctx)`（兼容入口：分钟+大任务+业务任务都跑）
- `RunMinuteUploader(ctx)`（仅分钟任务上传）
- `RunBigUploader(ctx)`（仅大任务上传）
- `RunBizUploader(ctx)`（仅业务任务上传）
- `OnStartup(ctx)`
- `RunBigTask(ctx, taskCode, windowStart, windowEnd)`
- `RunBizTask(ctx, taskCode, triggerKey, triggerPayload)`

## License

当前仓库未声明 License；如需开源请补充 `LICENSE` 文件。
