package reliableupload

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"
)

const defaultScanLimit = 1000

// Engine orchestrates producer/uploader/recovery flows.
type Engine struct {
	registry     *Registry
	cfgRepo      TaskConfigRepo
	logRepo      UploadLogRepo
	dailyRepo    DailyTaskRepo
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

func NewEngine(registry *Registry, cfgRepo TaskConfigRepo, logRepo UploadLogRepo, dailyRepo DailyTaskRepo, backup BackupStore, opts ...EngineOption) *Engine {
	e := &Engine{
		registry:     registry,
		cfgRepo:      cfgRepo,
		logRepo:      logRepo,
		dailyRepo:    dailyRepo,
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

// RunUploader uploads minute pending logs and daily pending batches.
func (e *Engine) RunUploader(ctx context.Context) error {
	if err := e.uploadMinute(ctx); err != nil {
		return err
	}
	return e.uploadDaily(ctx)
}

// OnStartup backfills minute gaps and resumes daily running instances.
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
	return e.resumeDaily(ctx)
}

// RunDailyTask creates/resumes one full-day task and uploads all pending batches.
func (e *Engine) RunDailyTask(ctx context.Context, taskCode string, taskDate time.Time) error {
	cfg, err := e.cfgRepo.Get(ctx, taskCode)
	if err != nil {
		return err
	}
	if cfg.TaskType != TaskTypeDaily {
		return fmt.Errorf("task=%s is not daily", taskCode)
	}
	inst, err := e.dailyRepo.GetOrCreateInstance(ctx, taskCode, normalizeDate(taskDate))
	if err != nil {
		return err
	}
	if inst.TotalBatches == 0 {
		if err := e.produceDaily(ctx, cfg, inst); err != nil {
			return err
		}
	}
	return e.uploadPendingBatches(ctx, cfg, inst.ID)
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
	chunks, err := ds.FetchAndEncode(ctx, cfg, start, end)
	if err != nil {
		return err
	}
	for i, chunk := range chunks {
		fileName := e.namer.MinuteFileName(cfg, start, end, i+1)
		backupPath, err := e.backup.Save(ctx, cfg.TaskCode, fileName, chunk)
		if err != nil {
			return err
		}
		now := e.clock.Now()
		log := UploadLog{
			TaskCode:   cfg.TaskCode,
			TimeStart:  start,
			TimeEnd:    end,
			FileName:   fileName,
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
		err = rp.Upload(ctx, cfg, log.FileName, data)
		if err != nil {
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

func (e *Engine) produceDaily(ctx context.Context, cfg TaskConfig, inst DailyTaskInstance) error {
	ds, err := e.registry.DataSource(cfg.TaskCode)
	if err != nil {
		return err
	}
	start := normalizeDate(inst.TaskDate)
	end := start.Add(24 * time.Hour)
	chunks, err := ds.FetchAndEncode(ctx, cfg, start, end)
	if err != nil {
		return err
	}
	for i, chunk := range chunks {
		index := i + 1
		fileName := e.namer.DailyFileName(cfg, start, index)
		backupPath, err := e.backup.Save(ctx, cfg.TaskCode, fileName, chunk)
		if err != nil {
			return err
		}
		batch := DailyTaskBatch{
			InstanceID: inst.ID,
			BatchIndex: index,
			FileName:   fileName,
			BackupPath: backupPath,
			Status:     StatusPending,
			CreatedAt:  e.clock.Now(),
			UpdatedAt:  e.clock.Now(),
		}
		if err := e.dailyRepo.CreateBatch(ctx, batch); err != nil {
			return err
		}
	}
	return e.dailyRepo.UpdateProducedMeta(ctx, inst.ID, len(chunks), 0)
}

func (e *Engine) uploadDaily(ctx context.Context) error {
	instances, err := e.dailyRepo.FindRunningInstances(ctx)
	if err != nil {
		return err
	}
	return e.runInParallelInstances(instances, func(inst DailyTaskInstance) error {
		cfg, err := e.cfgRepo.Get(ctx, inst.TaskCode)
		if err != nil {
			return err
		}
		return e.uploadPendingBatches(ctx, cfg, inst.ID)
	})
}

func (e *Engine) uploadPendingBatches(ctx context.Context, cfg TaskConfig, instanceID int64) error {
	rp, err := e.registry.Reporter(cfg.TaskCode)
	if err != nil {
		return err
	}
	batches, err := e.dailyRepo.FindPendingBatches(ctx, instanceID, cfg.MaxRetry, e.pendingLimit)
	if err != nil {
		return err
	}
	for _, b := range batches {
		data, err := e.backup.Read(ctx, b.BackupPath)
		if err != nil {
			e.dailyRepo.IncrBatchRetry(ctx, b.ID, err.Error())
			return err
		}
		err = rp.Upload(ctx, cfg, b.FileName, data)
		if err != nil {
			e.dailyRepo.IncrBatchRetry(ctx, b.ID, err.Error())
			return err
		}
		if err := e.dailyRepo.MarkBatchUploaded(ctx, b.ID); err != nil {
			return err
		}
	}
	uploaded, err := e.dailyRepo.CountUploadedBatches(ctx, instanceID)
	if err != nil {
		return err
	}
	total, err := e.dailyRepo.CountBatches(ctx, instanceID)
	if err != nil {
		return err
	}
	if total > 0 && uploaded >= total {
		_ = e.dailyRepo.MarkInstanceCompleted(ctx, instanceID, e.clock.Now())
	}
	return nil
}

func (e *Engine) resumeDaily(ctx context.Context) error {
	instances, err := e.dailyRepo.FindRunningInstances(ctx)
	if err != nil {
		return err
	}
	return e.runInParallelInstances(instances, func(inst DailyTaskInstance) error {
		cfg, err := e.cfgRepo.Get(ctx, inst.TaskCode)
		if err != nil {
			return err
		}
		batchCount, err := e.dailyRepo.CountBatches(ctx, inst.ID)
		if err != nil {
			return err
		}
		if inst.TotalBatches > 0 && batchCount < inst.TotalBatches {
			return e.produceDaily(ctx, cfg, inst)
		}
		return e.uploadPendingBatches(ctx, cfg, inst.ID)
	})
}

func calcMinuteRange(now time.Time, delaySeconds int) (time.Time, time.Time) {
	t := now.Add(-time.Duration(delaySeconds) * time.Second).Truncate(time.Minute).Add(-time.Minute)
	return t, t.Add(time.Minute)
}

func normalizeDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
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

func (e *Engine) runInParallelInstances(instances []DailyTaskInstance, fn func(inst DailyTaskInstance) error) error {
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
	var all error
	for err := range errCh {
		all = errors.Join(all, err)
	}
	return all
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type noopLogger struct{}

func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

type defaultFileNamer struct{}

func (defaultFileNamer) MinuteFileName(cfg TaskConfig, start, end time.Time, batchIndex int) string {
	return fmt.Sprintf("%s_%s_%s_%03d.dat", cfg.FilePrefix, start.Format("20060102150405"), end.Format("20060102150405"), batchIndex)
}

func (defaultFileNamer) DailyFileName(cfg TaskConfig, date time.Time, batchIndex int) string {
	return fmt.Sprintf("%s_%s_%03d.dat", cfg.FilePrefix, date.Format("20060102"), batchIndex)
}

func BuildRemotePath(cfg TaskConfig, fileName string) string {
	return path.Join(cfg.SFTPSubdir, fileName)
}
