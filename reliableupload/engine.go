package reliableupload

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultScanLimit = 1000

// Engine orchestrates producer/uploader/recovery flows.
type Engine struct {
	registry     *Registry
	cfgRepo      TaskConfigRepo
	logRepo      UploadLogRepo
	bigRepo      BigTaskRepo
	backup       BackupStore
	clock        Clock
	logger       Logger
	namer        FileNamer
	pendingLimit int
}

// EngineOption customizes engine behavior.
type EngineOption func(*Engine)

func WithLogger(logger Logger) EngineOption {
	return func(e *Engine) { e.logger = logger }
}

// WithLoggerFuncs allows injecting plain function callbacks as logger.
func WithLoggerFuncs(infof func(string, ...any), errorf func(string, ...any)) EngineOption {
	return func(e *Engine) {
		e.logger = LoggerFuncs{InfofFunc: infof, ErrorfFunc: errorf}
	}
}

func WithClock(clock Clock) EngineOption {
	return func(e *Engine) { e.clock = clock }
}

func WithFileNamer(namer FileNamer) EngineOption {
	return func(e *Engine) { e.namer = namer }
}

func WithPendingLimit(limit int) EngineOption {
	return func(e *Engine) {
		if limit > 0 {
			e.pendingLimit = limit
		}
	}
}

func NewEngine(registry *Registry, cfgRepo TaskConfigRepo, logRepo UploadLogRepo, bigRepo BigTaskRepo, backup BackupStore, opts ...EngineOption) *Engine {
	e := &Engine{
		registry:     registry,
		cfgRepo:      cfgRepo,
		logRepo:      logRepo,
		bigRepo:      bigRepo,
		backup:       backup,
		clock:        systemClock{},
		logger:       noopLogger{},
		namer:        defaultFileNamer{},
		pendingLimit: defaultScanLimit,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// RunProducer produces minute-task files and writes pending logs.
func (e *Engine) RunProducer(ctx context.Context) error {
	configs, err := e.cfgRepo.FindEnabledByType(ctx, TaskTypeMinute)
	if err != nil {
		return err
	}
	return e.runInParallel(configs, func(cfg TaskConfig) error {
		start, end := calcMinuteRange(e.clock.Now(), cfg.DelaySeconds)
		return e.produceRange(ctx, cfg, start, end)
	})
}

// ProduceForTask produces one explicit time range for the given task_code.
func (e *Engine) ProduceForTask(ctx context.Context, taskCode string, start, end time.Time) error {
	cfg, err := e.cfgRepo.Get(ctx, taskCode)
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return fmt.Errorf("task=%s is disabled", taskCode)
	}
	return e.produceRange(ctx, cfg, start, end)
}

// ProduceCurrentWindowForTask produces only current minute window for one task.
func (e *Engine) ProduceCurrentWindowForTask(ctx context.Context, taskCode string) error {
	cfg, err := e.cfgRepo.Get(ctx, taskCode)
	if err != nil {
		return err
	}
	if cfg.TaskType != TaskTypeMinute {
		return fmt.Errorf("task=%s is not minute type", taskCode)
	}
	start, end := calcMinuteRange(e.clock.Now(), cfg.DelaySeconds)
	return e.produceRange(ctx, cfg, start, end)
}

// RunUploader is compatibility entrypoint that uploads minute + big task pending records.
func (e *Engine) RunUploader(ctx context.Context) error {
	if err := e.RunMinuteUploader(ctx); err != nil {
		return err
	}
	return e.RunBigUploader(ctx)
}

func (e *Engine) RunMinuteUploader(ctx context.Context) error { return e.uploadMinute(ctx) }
func (e *Engine) RunBigUploader(ctx context.Context) error    { return e.uploadBig(ctx) }

// UploadPendingForTask uploads minute pending logs for one task_code.
func (e *Engine) UploadPendingForTask(ctx context.Context, taskCode string) error {
	cfg, err := e.cfgRepo.Get(ctx, taskCode)
	if err != nil {
		return err
	}
	return e.uploadMinuteByTaskCode(ctx, cfg)
}

// OnStartup backfills minute gaps and resumes running big tasks.
func (e *Engine) OnStartup(ctx context.Context) error {
	configs, err := e.cfgRepo.FindEnabledByType(ctx, TaskTypeMinute)
	if err != nil {
		return err
	}
	for _, cfg := range configs {
		if err := e.recoverMinuteGaps(ctx, cfg); err != nil {
			e.logger.Errorf("recover minute gap failed task=%s err=%v", cfg.TaskCode, err)
		}
	}
	return e.resumeBig(ctx)
}

// RunBigTask creates/resumes one custom-window big task and uploads pending batches.
func (e *Engine) RunBigTask(ctx context.Context, taskCode string, windowStart, windowEnd time.Time) error {
	cfg, err := e.cfgRepo.Get(ctx, taskCode)
	if err != nil {
		return err
	}
	if cfg.TaskType != TaskTypeBig {
		return fmt.Errorf("task=%s is not big task type", taskCode)
	}
	inst, err := e.bigRepo.GetOrCreateInstance(ctx, taskCode, windowStart, windowEnd)
	if err != nil {
		return err
	}
	if inst.TotalBatches == 0 {
		if err := e.produceBig(ctx, cfg, inst); err != nil {
			return err
		}
	}
	return e.uploadPendingBigBatches(ctx, cfg, inst.ID)
}

func (e *Engine) produceRange(ctx context.Context, cfg TaskConfig, start, end time.Time) error {
	exists, err := e.logRepo.ExistsByTaskAndTimeRange(ctx, cfg.TaskCode, start, end)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	ds, err := e.registry.DataSource(cfg.TaskCode)
	if err != nil {
		return err
	}
	total, err := ds.CountChunks(ctx, cfg, start, end)
	if err != nil {
		return err
	}
	if total < 0 {
		return fmt.Errorf("task=%s invalid chunk count: %d", cfg.TaskCode, total)
	}
	if total == 0 {
		now := e.clock.Now()
		log := UploadLog{
			TaskCode:   cfg.TaskCode,
			TimeStart:  start,
			TimeEnd:    end,
			FileName:   e.fileName(cfg, start, end, 0, NameContext{}),
			Status:     StatusUploaded,
			BackupPath: "",
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		return e.logRepo.Create(ctx, log)
	}
	for index := 1; index <= total; index++ {
		chunk, err := ds.FetchChunk(ctx, cfg, start, end, index)
		if err != nil {
			return err
		}
		fileName := e.fileName(cfg, start, end, index, NameContext{BizKey: chunk.BizKey, Meta: chunk.Meta})
		backupPath, err := e.backup.Save(ctx, cfg.TaskCode, fileName, chunk.Data)
		if err != nil {
			return err
		}
		metaJSON, err := encodeMeta(chunk.Meta)
		if err != nil {
			return err
		}
		now := e.clock.Now()
		log := UploadLog{
			TaskCode:   cfg.TaskCode,
			TimeStart:  start,
			TimeEnd:    end,
			FileName:   fileName,
			BizKey:     chunk.BizKey,
			MetaJSON:   metaJSON,
			Status:     StatusPending,
			BackupPath: backupPath,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if err := e.logRepo.Create(ctx, log); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) uploadMinute(ctx context.Context) error {
	codes, err := e.logRepo.FindDistinctPendingTaskCodes(ctx)
	if err != nil {
		return err
	}
	sort.Strings(codes)
	return e.runInParallelTaskCodes(codes, func(taskCode string) error {
		cfg, err := e.cfgRepo.Get(ctx, taskCode)
		if err != nil {
			return err
		}
		return e.uploadMinuteByTaskCode(ctx, cfg)
	})
}

func (e *Engine) uploadMinuteByTaskCode(ctx context.Context, cfg TaskConfig) error {
	rp, err := e.registry.Reporter(cfg.TaskCode)
	if err != nil {
		return err
	}
	logs, err := e.logRepo.FindPendingByCode(ctx, cfg.TaskCode, cfg.MaxRetry, e.pendingLimit)
	if err != nil {
		return err
	}
	for _, log := range logs {
		data, err := e.backup.Read(ctx, log.BackupPath)
		if err != nil {
			e.logRepo.IncrRetry(ctx, log.ID, err.Error())
			return err
		}
		meta, err := decodeMeta(log.MetaJSON)
		if err != nil {
			e.logRepo.IncrRetry(ctx, log.ID, err.Error())
			return err
		}
		item := UploadItem{FileName: log.FileName, Data: data, BizKey: log.BizKey, Meta: meta, BackupPath: log.BackupPath}
		if err := rp.Upload(ctx, cfg, item); err != nil {
			e.logRepo.IncrRetry(ctx, log.ID, err.Error())
			return err
		}
		if err := e.logRepo.MarkUploaded(ctx, log.ID); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) recoverMinuteGaps(ctx context.Context, cfg TaskConfig) error {
	lastEnd, ok, err := e.logRepo.GetLastTimeEndByCode(ctx, cfg.TaskCode)
	if err != nil {
		return err
	}
	if !ok {
		start, _ := calcMinuteRange(e.clock.Now(), cfg.DelaySeconds)
		lastEnd = start.Add(-time.Minute)
	}
	cutoffStart, _ := calcMinuteRange(e.clock.Now(), cfg.DelaySeconds)
	for t := lastEnd; t.Before(cutoffStart); t = t.Add(time.Minute) {
		start, end := t, t.Add(time.Minute)
		exists, err := e.logRepo.ExistsByTaskAndTimeRange(ctx, cfg.TaskCode, start, end)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if err := e.produceRange(ctx, cfg, start, end); err != nil {
			e.logger.Errorf("backfill failed task=%s start=%s err=%v", cfg.TaskCode, start.Format(time.RFC3339), err)
		}
	}
	return nil
}

func (e *Engine) produceBig(ctx context.Context, cfg TaskConfig, inst BigTaskInstance) error {
	ds, err := e.registry.DataSource(cfg.TaskCode)
	if err != nil {
		return err
	}
	start, end := inst.WindowStart, inst.WindowEnd
	total, err := ds.CountChunks(ctx, cfg, start, end)
	if err != nil {
		return err
	}
	if total < 0 {
		return fmt.Errorf("task=%s invalid chunk count: %d", cfg.TaskCode, total)
	}
	if err := e.bigRepo.UpdateProducedMeta(ctx, inst.ID, total, 0); err != nil {
		return err
	}
	existingCount, err := e.bigRepo.CountBatches(ctx, inst.ID)
	if err != nil {
		return err
	}
	for index := existingCount + 1; index <= total; index++ {
		chunk, err := ds.FetchChunk(ctx, cfg, start, end, index)
		if err != nil {
			return err
		}
		fileName := e.fileName(cfg, start, end, index, NameContext{BizKey: chunk.BizKey, Meta: chunk.Meta})
		backupPath, err := e.backup.Save(ctx, cfg.TaskCode, fileName, chunk.Data)
		if err != nil {
			return err
		}
		metaJSON, err := encodeMeta(chunk.Meta)
		if err != nil {
			return err
		}
		now := e.clock.Now()
		batch := BigTaskBatch{
			InstanceID: inst.ID,
			BatchIndex: index,
			FileName:   fileName,
			BizKey:     chunk.BizKey,
			MetaJSON:   metaJSON,
			BackupPath: backupPath,
			Status:     StatusPending,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if err := e.bigRepo.CreateBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) uploadBig(ctx context.Context) error {
	instances, err := e.bigRepo.FindRunningInstances(ctx)
	if err != nil {
		return err
	}
	return e.runInParallelInstances(instances, func(inst BigTaskInstance) error {
		cfg, err := e.cfgRepo.Get(ctx, inst.TaskCode)
		if err != nil {
			return err
		}
		return e.uploadPendingBigBatches(ctx, cfg, inst.ID)
	})
}

func (e *Engine) uploadPendingBigBatches(ctx context.Context, cfg TaskConfig, instanceID int64) error {
	rp, err := e.registry.Reporter(cfg.TaskCode)
	if err != nil {
		return err
	}
	batches, err := e.bigRepo.FindPendingBatches(ctx, instanceID, cfg.MaxRetry, e.pendingLimit)
	if err != nil {
		return err
	}
	for _, b := range batches {
		data, err := e.backup.Read(ctx, b.BackupPath)
		if err != nil {
			e.bigRepo.IncrBatchRetry(ctx, b.ID, err.Error())
			return err
		}
		meta, err := decodeMeta(b.MetaJSON)
		if err != nil {
			e.bigRepo.IncrBatchRetry(ctx, b.ID, err.Error())
			return err
		}
		item := UploadItem{FileName: b.FileName, Data: data, BizKey: b.BizKey, Meta: meta, BackupPath: b.BackupPath}
		if err := rp.Upload(ctx, cfg, item); err != nil {
			e.bigRepo.IncrBatchRetry(ctx, b.ID, err.Error())
			return err
		}
		if err := e.bigRepo.MarkBatchUploaded(ctx, b.ID); err != nil {
			return err
		}
	}
	uploaded, err := e.bigRepo.CountUploadedBatches(ctx, instanceID)
	if err != nil {
		return err
	}
	total, err := e.bigRepo.CountBatches(ctx, instanceID)
	if err != nil {
		return err
	}
	if total == 0 || uploaded >= total {
		_ = e.bigRepo.MarkInstanceCompleted(ctx, instanceID, e.clock.Now())
	}
	return nil
}

func (e *Engine) resumeBig(ctx context.Context) error {
	instances, err := e.bigRepo.FindRunningInstances(ctx)
	if err != nil {
		return err
	}
	return e.runInParallelInstances(instances, func(inst BigTaskInstance) error {
		cfg, err := e.cfgRepo.Get(ctx, inst.TaskCode)
		if err != nil {
			return err
		}
		batchCount, err := e.bigRepo.CountBatches(ctx, inst.ID)
		if err != nil {
			return err
		}
		if inst.TotalBatches > 0 && batchCount < inst.TotalBatches {
			return e.produceBig(ctx, cfg, inst)
		}
		return e.uploadPendingBigBatches(ctx, cfg, inst.ID)
	})
}

func calcMinuteRange(now time.Time, delaySeconds int) (time.Time, time.Time) {
	t := now.Add(-time.Duration(delaySeconds) * time.Second).Truncate(time.Minute).Add(-time.Minute)
	return t, t.Add(time.Minute)
}

func (e *Engine) runInParallel(configs []TaskConfig, fn func(cfg TaskConfig) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(configs))
	for _, cfg := range configs {
		cfg := cfg
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(cfg); err != nil {
				errCh <- fmt.Errorf("task=%s: %w", cfg.TaskCode, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	return joinErrors(errCh)
}

func (e *Engine) runInParallelTaskCodes(codes []string, fn func(taskCode string) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(codes))
	for _, code := range codes {
		code := code
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(code); err != nil {
				errCh <- fmt.Errorf("task=%s: %w", code, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	return joinErrors(errCh)
}

func (e *Engine) runInParallelInstances(instances []BigTaskInstance, fn func(inst BigTaskInstance) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(instances))
	for _, inst := range instances {
		inst := inst
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(inst); err != nil {
				errCh <- fmt.Errorf("instance=%d: %w", inst.ID, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	return joinErrors(errCh)
}

func joinErrors(errCh <-chan error) error {
	msgs := make([]string, 0)
	for err := range errCh {
		if err != nil {
			msgs = append(msgs, err.Error())
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	return fmt.Errorf(strings.Join(msgs, " | "))
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type noopLogger struct{}

func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

type defaultFileNamer struct{}

func (defaultFileNamer) FileName(cfg TaskConfig, windowStart, windowEnd time.Time, batchIndex int, _ NameContext) string {
	return fmt.Sprintf("%s_%s_%s_%03d.dat", cfg.FilePrefix, windowStart.Format("20060102150405"), windowEnd.Format("20060102150405"), batchIndex)
}

func BuildRemotePath(cfg TaskConfig, fileName string) string {
	return path.Join(cfg.SFTPSubdir, fileName)
}

func (e *Engine) fileNamerForTask(taskCode string) FileNamer {
	if namer, ok := e.registry.FileNamer(taskCode); ok {
		return namer
	}
	return e.namer
}

func (e *Engine) fileName(cfg TaskConfig, windowStart, windowEnd time.Time, batchIndex int, ctx NameContext) string {
	return e.fileNamerForTask(cfg.TaskCode).FileName(cfg, windowStart, windowEnd, batchIndex, ctx)
}

func encodeMeta(meta map[string]string) (string, error) {
	if len(meta) == 0 {
		return "", nil
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func decodeMeta(metaJSON string) (map[string]string, error) {
	if metaJSON == "" {
		return nil, nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(metaJSON), &out); err != nil {
		return nil, err
	}
	return out, nil
}
