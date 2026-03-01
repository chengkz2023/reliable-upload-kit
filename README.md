# smart-upload

`reliableupload` 是一个可集成到任意 Go 项目的可靠上报框架，基于方案文档中的核心思路实现：
- 备份文件作为重试唯一数据源
- 状态机驱动（pending / uploaded / failed）
- 生产与上报解耦
- 按 `task_code` 隔离并发
- 启动补窗恢复

详细接入文档见：[docs/DEVELOPER_GUIDE.md](/D:/goworkspace/smart-upload/docs/DEVELOPER_GUIDE.md)

## 目录结构

- `reliableupload/`：框架核心
- `example/main.go`：可运行示例（内存仓储 + 本地备份）

## 开发者可插拔点

业务方只需实现并注册两个接口：

```go
type DataSource interface {
    FetchAndEncode(ctx context.Context, cfg TaskConfig, start, end time.Time) ([][]byte, error)
}

type Reporter interface {
    Upload(ctx context.Context, cfg TaskConfig, fileName string, data []byte) error
}
```

- `DataSource`：负责查数 + 编码/加密，返回分批文件内容
- `Reporter`：负责对端上报（建议内部做幂等，比如远端已存在即视为成功）

## 引擎能力

- `RunProducer(ctx)`：分钟任务生产（查数 -> 备份 -> 写 pending）
- `RunUploader(ctx)`：分钟任务 + 每日任务统一上报
- `OnStartup(ctx)`：启动时补跑分钟空窗并恢复每日断点
- `RunDailyTask(ctx, taskCode, date)`：触发/恢复某个每日任务

## 最小接入步骤

1. 实现仓储接口（可用 MySQL/GORM/SQLX）：
   - `TaskConfigRepo`
   - `UploadLogRepo`
   - `DailyTaskRepo`
2. 为每个 `task_code` 注册 `DataSource` 与 `Reporter`
3. 提供 `BackupStore`（已内置 `FSBackupStore`）
4. 初始化 `Engine` 并接入你的定时任务：
   - Cron A：`RunProducer`
   - Cron B：`RunUploader`
   - 服务启动：`OnStartup`

## 运行示例

```bash
go run ./example
```

示例会演示：分钟任务生产 + 上报，以及每日任务生产 + 上报流程。
