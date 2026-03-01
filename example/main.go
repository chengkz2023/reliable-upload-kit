package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"smart-upload/reliableupload"
)

const defaultDSN = "root:chengkangze.1@tcp(139.196.105.27:3306)/smart_upload?charset=utf8mb4&parseTime=True&loc=Local"

func main() {
	ctx := context.Background()
	// 支持通过环境变量覆盖 DSN，方便本地/测试/生产切换。
	// 例如:
	//   set MYSQL_DSN=root:pwd@tcp(127.0.0.1:3306)/smart_upload?charset=utf8mb4&parseTime=True&loc=Local
	dsn := envOrDefault("MYSQL_DSN", defaultDSN)

	// 1) 启动时保障数据库存在，再建立 GORM 连接。
	if err := ensureMySQLDatabase(dsn); err != nil {
		panic(fmt.Sprintf("ensure mysql database failed: %v", err))
	}
	db, err := openMySQL(dsn)
	if err != nil {
		panic(fmt.Sprintf("open mysql failed: %v", err))
	}
	// 2) 自动建表/补字段（uploadlog + daily_task_instance + daily_task_batch）。
	if err := initMySQLSchema(db); err != nil {
		panic(fmt.Sprintf("init mysql schema failed: %v", err))
	}
	// 可选但建议: 配置连接池，避免默认值在高并发下不够用。
	// sqlDB, _ := db.DB()
	// sqlDB.SetMaxOpenConns(30)
	// sqlDB.SetMaxIdleConns(10)
	// sqlDB.SetConnMaxLifetime(30 * time.Minute)

	// 3) 注册中心: 每个 task_code 绑定一个 DataSource + Reporter。
	registry := reliableupload.NewRegistry()
	ds := &demoDataSource{}
	rp := newDemoReporter()

	registry.RegisterDataSource("order_minute", ds)
	registry.RegisterReporter("order_minute", rp)
	registry.RegisterDataSource("order_daily", ds)
	registry.RegisterReporter("order_daily", rp)

	cfgRepo := &memConfigRepo{m: map[string]reliableupload.TaskConfig{
		"order_minute": {
			TaskCode:     "order_minute",
			TaskType:     reliableupload.TaskTypeMinute,
			DelaySeconds: 60,
			BatchSize:    500,
			MaxRetry:     3,
			SFTPSubdir:   "/remote/order",
			FilePrefix:   "order",
			Enabled:      true,
		},
		"order_daily": {
			TaskCode:   "order_daily",
			TaskType:   reliableupload.TaskTypeDaily,
			BatchSize:  2000,
			MaxRetry:   3,
			SFTPSubdir: "/remote/order",
			FilePrefix: "order_daily",
			Enabled:    true,
		},
	}}

	// 4) 引擎初始化:
	// TaskConfigRepo 继续使用内存实现；
	// UploadLogRepo / DailyTaskRepo 使用 MySQL + GORM 实现。
	engine := reliableupload.NewEngine(
		registry,
		cfgRepo,
		newMySQLUploadLogRepo(db),
		newMySQLDailyRepo(db),
		reliableupload.NewFSBackupStore("./backup"),
		// 可选特性: 控制单轮扫描 pending 的上限，避免一次处理过多。
		// reliableupload.WithPendingLimit(500),
		// 可选特性: 注入自定义日志器，接入你现有日志系统。
		// reliableupload.WithLogger(myLogger{}),
		// 可选特性: 注入自定义文件命名策略。
		// reliableupload.WithFileNamer(myFileNamer{}),
		// 可选特性: 注入时钟（常用于测试）。
		// reliableupload.WithClock(myClock{}),
	)

	// 5) 关键流程演示:
	// RunProducer: 生产分钟任务（查数 -> 备份 -> 写 pending）
	// RunUploader: 扫描 pending 并上报（分钟 + 每日 running）
	// RunDailyTask: 触发/恢复指定日期的每日任务
	_ = engine.RunProducer(ctx)
	_ = engine.RunUploader(ctx)
	_ = engine.RunDailyTask(ctx, "order_daily", time.Now().AddDate(0, 0, -1))

	// 可选特性: 服务启动后执行恢复（补分钟空窗 + 续传每日任务）。
	// _ = engine.OnStartup(ctx)
	//
	// 可选特性: 接入真实调度器（示意）。
	// producerTicker := time.NewTicker(time.Minute) // Cron A
	// uploaderTicker := time.NewTicker(time.Minute) // Cron B（建议与 A 错峰 20~40 秒）
	// defer producerTicker.Stop()
	// defer uploaderTicker.Stop()
	// for {
	//     select {
	//     case <-producerTicker.C:
	//         _ = engine.RunProducer(ctx)
	//     case <-uploaderTicker.C:
	//         _ = engine.RunUploader(ctx)
	//     }
	// }

	fmt.Println("uploaded files:")
	for _, name := range rp.UploadedFiles() {
		fmt.Println(" -", name)
	}
}

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

type demoDataSource struct{}

func (d *demoDataSource) FetchAndEncode(_ context.Context, cfg reliableupload.TaskConfig, start, end time.Time) ([][]byte, error) {
	// 示例数据源:
	// - 分钟任务: 1 个分片
	// - 每日任务: 2 个分片
	// 实际业务中请在这里实现: 数据查询 -> 加密/编码 -> 按 batch_size 切片。
	if cfg.TaskType == reliableupload.TaskTypeDaily {
		return [][]byte{
			[]byte(fmt.Sprintf("%s chunk-1 %s", cfg.TaskCode, start.Format("2006-01-02"))),
			[]byte(fmt.Sprintf("%s chunk-2 %s", cfg.TaskCode, start.Format("2006-01-02"))),
		}, nil
	}
	return [][]byte{[]byte(fmt.Sprintf("%s %s~%s", cfg.TaskCode, start.Format(time.RFC3339), end.Format(time.RFC3339)))}, nil
}

type demoReporter struct {
	mu       sync.Mutex
	uploaded map[string]struct{}
}

func newDemoReporter() *demoReporter {
	return &demoReporter{uploaded: map[string]struct{}{}}
}

func (r *demoReporter) Upload(_ context.Context, cfg reliableupload.TaskConfig, fileName string, _ []byte) error {
	// 示例上报器: 仅做内存记录，模拟“上报成功”。
	// 真实场景建议实现幂等语义:
	// 1. 先检查远端 fileName 是否存在
	// 2. 不存在则上传 fileName.tmp
	// 3. 成功后 rename 为 fileName
	r.mu.Lock()
	defer r.mu.Unlock()
	key := cfg.TaskCode + "/" + fileName
	r.uploaded[key] = struct{}{}
	return nil
}

func (r *demoReporter) UploadedFiles() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.uploaded))
	for k := range r.uploaded {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

type memConfigRepo struct {
	m map[string]reliableupload.TaskConfig
}

func (r *memConfigRepo) FindEnabledByType(_ context.Context, typ reliableupload.TaskType) ([]reliableupload.TaskConfig, error) {
	// 示例中 TaskConfigRepo 仍用内存；
	// 实际可替换为 MySQL 配置表实现，支持动态启停与参数变更。
	var out []reliableupload.TaskConfig
	for _, cfg := range r.m {
		if cfg.Enabled && cfg.TaskType == typ {
			out = append(out, cfg)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskCode < out[j].TaskCode })
	return out, nil
}

func (r *memConfigRepo) Get(_ context.Context, taskCode string) (reliableupload.TaskConfig, error) {
	cfg, ok := r.m[taskCode]
	if !ok {
		return reliableupload.TaskConfig{}, fmt.Errorf("task not found: %s", taskCode)
	}
	return cfg, nil
}
