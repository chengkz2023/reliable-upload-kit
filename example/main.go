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
	dsn := envOrDefault("MYSQL_DSN", defaultDSN)

	if err := ensureMySQLDatabase(dsn); err != nil {
		panic(fmt.Sprintf("ensure mysql database failed: %v", err))
	}
	db, err := openMySQL(dsn)
	if err != nil {
		panic(fmt.Sprintf("open mysql failed: %v", err))
	}
	if err := initMySQLSchema(db); err != nil {
		panic(fmt.Sprintf("init mysql schema failed: %v", err))
	}

	registry := reliableupload.NewRegistry()
	ds := &demoDataSource{}
	rp := newDemoReporter()

	registry.RegisterDataSource("order_minute", ds)
	registry.RegisterReporter("order_minute", rp)
	registry.RegisterDataSource("order_big", ds)
	registry.RegisterReporter("order_big", rp)

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
		"order_big": {
			TaskCode:   "order_big",
			TaskType:   reliableupload.TaskTypeBig,
			BatchSize:  2000,
			MaxRetry:   3,
			SFTPSubdir: "/remote/order",
			FilePrefix: "order_big",
			Enabled:    true,
		},
	}}

	engine := reliableupload.NewEngine(
		registry,
		cfgRepo,
		newMySQLUploadLogRepo(db),
		newMySQLBigRepo(db),
		reliableupload.NewFSBackupStore("./backup"),
	)

	_ = engine.RunProducer(ctx)
	_ = engine.RunUploader(ctx)
	bigStart := time.Now().Add(-time.Hour).Truncate(time.Hour)
	bigEnd := bigStart.Add(time.Hour)
	_ = engine.RunBigTask(ctx, "order_big", bigStart, bigEnd)

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

func (d *demoDataSource) CountChunks(_ context.Context, cfg reliableupload.TaskConfig, _, _ time.Time) (int, error) {
	if cfg.TaskType == reliableupload.TaskTypeBig {
		return 2, nil
	}
	return 1, nil
}

func (d *demoDataSource) FetchChunk(_ context.Context, cfg reliableupload.TaskConfig, start, _ time.Time, index int) (reliableupload.Chunk, error) {
	if cfg.TaskType == reliableupload.TaskTypeBig {
		return reliableupload.Chunk{
			Data:   []byte(fmt.Sprintf("%s chunk-%d %s", cfg.TaskCode, index, start.Format("2006-01-02"))),
			BizKey: fmt.Sprintf("biz-%d", index),
			Meta: map[string]string{
				"channel": "demo",
				"bucket":  fmt.Sprintf("%d", index),
			},
		}, nil
	}
	return reliableupload.Chunk{
		Data: []byte(fmt.Sprintf("%s %s", cfg.TaskCode, start.Format(time.RFC3339))),
		Meta: map[string]string{"kind": "minute"},
	}, nil
}

type demoReporter struct {
	mu       sync.Mutex
	uploaded map[string]struct{}
}

func newDemoReporter() *demoReporter {
	return &demoReporter{uploaded: map[string]struct{}{}}
}

func (r *demoReporter) Upload(_ context.Context, cfg reliableupload.TaskConfig, item reliableupload.UploadItem) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := cfg.TaskCode + "/" + item.FileName
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
