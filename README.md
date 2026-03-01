# reliable-upload-kit

面向生产环境的 Go 可靠上报框架。框架聚焦于调度、状态机、重试与恢复；业务方只需实现数据获取和上报逻辑。

## 功能特性

- 备份文件作为重试唯一数据源（避免重查导致数据漂移）
- 状态机驱动（`pending` / `uploaded` / `failed` / `running`）
- 生产与上报解耦（Cron A / Cron B）
- 按 `task_code` 隔离并发，单任务失败不阻塞其他任务
- 启动恢复（分钟补窗 + 每日断点续传）
- 可插拔接口：`DataSource`、`Reporter`、`BackupStore`、Repo

## 文档导航

- 开发者接入文档: [docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md)
- 示例入口: [example/main.go](example/main.go)
- 核心引擎: [reliableupload/engine.go](reliableupload/engine.go)
- 接口定义: [reliableupload/interfaces.go](reliableupload/interfaces.go)

## 快速开始

### 1. 安装依赖

```bash
go mod tidy
```

### 2. 运行示例

```bash
go run ./example
```

示例默认会：
- 连接 MySQL（可通过环境变量覆盖 DSN）
- 自动创建数据库与核心表
- 演示分钟任务生产/上报 + 每日任务生产/上报

### 3. 覆盖 MySQL DSN

Windows PowerShell:

```powershell
$env:MYSQL_DSN = "root:password@tcp(127.0.0.1:3306)/smart_upload?charset=utf8mb4&parseTime=True&loc=Local"
go run ./example
```

## 核心抽象

```go
type DataSource interface {
    FetchAndEncode(ctx context.Context, cfg TaskConfig, start, end time.Time) ([][]byte, error)
}

type Reporter interface {
    Upload(ctx context.Context, cfg TaskConfig, fileName string, data []byte) error
}
```

- `DataSource`: 业务查数、编码、加密、分批
- `Reporter`: 将批次上报到对端并保证幂等

## 目录结构

```text
.
├── docs/
│   └── DEVELOPER_GUIDE.md
├── example/
│   ├── main.go
│   └── mysql_repo.go
├── reliableupload/
│   ├── engine.go
│   ├── interfaces.go
│   ├── registry.go
│   ├── types.go
│   └── backup_fs.go
├── README.md
└── go.mod
```

## 路线建议

1. 先跑通 `example`（验证数据库与流程）
2. 将 `TaskConfigRepo` 改为你自己的配置表实现
3. 实现业务 `DataSource` 与真实 `Reporter`（SFTP/HTTP/对象存储）
4. 接入 Cron A、Cron B、OnStartup

## License

当前仓库未声明 License；如需开源请补充 `LICENSE` 文件。
