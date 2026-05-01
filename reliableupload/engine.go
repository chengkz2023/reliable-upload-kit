package reliableupload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultScanLimit = 1000
const defaultClaimLease = 2 * time.Minute

// Engine orchestrates producer/uploader/recovery flows.
type Engine struct {
	registry     *Registry
	cfgRepo      TaskConfigRepo
	logRepo      UploadLogRepo
	bigRepo      BigTaskRepo
	bizRepo      BizTaskRepo
	backup       BackupStore
	clock        Clock
	logger       Logger
	namer        FileNamer
	hooks        UploadHooks
	failureMode  UploadFailureStrategy
	pendingLimit int
	workerID     string
	claimLease   time.Duration
	maxParallel  int
	// productionParallelism controls per-instance chunk production fan-out for big/biz tasks.
	// It parallelizes FetchChunk + BackupStore.Save while preserving ordered batch creation.
	productionParallelism int
	// startupBackfillLimit limits missing minute windows produced per task in one startup recovery run.
	// 0 means unlimited.
	startupBackfillLimit int
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

func WithUploadHooks(hooks UploadHooks) EngineOption {
	return func(e *Engine) {
		if hooks != nil {
			e.hooks = hooks
		}
	}
}

func WithUploadFailureStrategy(mode UploadFailureStrategy) EngineOption {
	return func(e *Engine) {
		if mode == UploadFailureContinue || mode == UploadFailureFailFast {
			e.failureMode = mode
		}
	}
}

func WithWorkerID(workerID string) EngineOption {
	return func(e *Engine) {
		if strings.TrimSpace(workerID) != "" {
			e.workerID = workerID
		}
	}
}

func WithClaimLease(d time.Duration) EngineOption {
	return func(e *Engine) {
		if d > 0 {
			e.claimLease = d
		}
	}
}

func WithMaxParallel(n int) EngineOption {
	return func(e *Engine) {
		if n > 0 {
			e.maxParallel = n
		}
	}
}

func WithProductionParallelism(n int) EngineOption {
	return func(e *Engine) {
		if n > 0 {
			e.productionParallelism = n
		}
	}
}

func WithStartupBackfillLimit(limit int) EngineOption {
	return func(e *Engine) {
		if limit > 0 {
			e.startupBackfillLimit = limit
		}
	}
}

func NewEngine(registry *Registry, cfgRepo TaskConfigRepo, logRepo UploadLogRepo, bigRepo BigTaskRepo, bizRepo BizTaskRepo, backup BackupStore, opts ...EngineOption) *Engine {
	e := &Engine{
		registry:              registry,
		cfgRepo:               cfgRepo,
		logRepo:               logRepo,
		bigRepo:               bigRepo,
		bizRepo:               bizRepo,
		backup:                backup,
		clock:                 systemClock{},
		logger:                noopLogger{},
		namer:                 defaultFileNamer{},
		hooks:                 noopUploadHooks{},
		failureMode:           UploadFailureFailFast,
		pendingLimit:          defaultScanLimit,
		workerID:              defaultWorkerID(),
		claimLease:            defaultClaimLease,
		maxParallel:           0,
		productionParallelism: 1,
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
		start, end := calcMinuteRange(e.clock.Now(), cfg.DelaySeconds, cfg.IntervalMinutes)
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
	start, end := calcMinuteRange(e.clock.Now(), cfg.DelaySeconds, cfg.IntervalMinutes)
	return e.produceRange(ctx, cfg, start, end)
}

// RunUploader is compatibility entrypoint that uploads minute + big + biz pending records.
func (e *Engine) RunUploader(ctx context.Context) error {
	if err := e.RunMinuteUploader(ctx); err != nil {
		return err
	}
	if err := e.RunBigUploader(ctx); err != nil {
		return err
	}
	return e.RunBizUploader(ctx)
}

func (e *Engine) RunMinuteUploader(ctx context.Context) error { return e.uploadMinute(ctx) }
func (e *Engine) RunBigUploader(ctx context.Context) error    { return e.uploadBig(ctx) }

func (e *Engine) RunBizUploader(ctx context.Context) error {
	if e.bizRepo == nil {
		return nil
	}
	return e.uploadBiz(ctx)
}

// UploadPendingForTask uploads minute pending logs for one task_code.
func (e *Engine) UploadPendingForTask(ctx context.Context, taskCode string) error {
	cfg, err := e.cfgRepo.Get(ctx, taskCode)
	if err != nil {
		return err
	}
	return e.uploadMinuteByTaskCode(ctx, cfg)
}

// OnStartup backfills minute gaps and resumes running big/biz tasks.
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
	if err := e.resumeBig(ctx); err != nil {
		return err
	}
	return e.resumeBiz(ctx)
}

// RunBigTask creates/resumes one custom-window big task and uploads pending batches.
func (e *Engine) RunBigTask(ctx context.Context, taskCode string, windowStart, windowEnd time.Time) error {
	if e.bigRepo == nil {
		return fmt.Errorf("big repo not configured")
	}
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

// RunBizTask creates/resumes one business-trigger task and uploads pending batches.
func (e *Engine) RunBizTask(ctx context.Context, taskCode, triggerKey, triggerPayload string) error {
	if e.bizRepo == nil {
		return fmt.Errorf("biz repo not configured")
	}
	if triggerKey == "" {
		return fmt.Errorf("trigger_key is required")
	}
	cfg, err := e.cfgRepo.Get(ctx, taskCode)
	if err != nil {
		return err
	}
	if cfg.TaskType != TaskTypeBiz {
		return fmt.Errorf("task=%s is not biz task type", taskCode)
	}
	inst, err := e.bizRepo.GetOrCreateInstance(ctx, taskCode, triggerKey, triggerPayload)
	if err != nil {
		return err
	}
	if inst.TotalBatches == 0 {
		if err := e.produceBiz(ctx, cfg, inst); err != nil {
			return err
		}
	}
	return e.uploadPendingBizBatches(ctx, cfg, inst.ID)
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
		if err := e.logRepo.Create(ctx, log); err != nil {
			if isAlreadyExistsErr(err) {
				return nil
			}
			return err
		}
		return nil
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
			if isAlreadyExistsErr(err) {
				continue
			}
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
	for {
		nextVisibleAt := e.clock.Now().Add(e.claimLease)
		logs, err := e.logRepo.ClaimPendingByCode(ctx, cfg.TaskCode, cfg.MaxRetry, e.pendingLimit, e.workerID, nextVisibleAt)
		if err != nil {
			return err
		}
		if len(logs) == 0 {
			return nil
		}
		for _, log := range logs {
			data, err := e.backup.Read(ctx, log.BackupPath)
			if err != nil {
				item := UploadItem{FileName: log.FileName, BackupPath: log.BackupPath, BizKey: log.BizKey}
				if markErr := e.logRepo.MarkRetryOrFailed(ctx, log.ID, log.ClaimID, cfg.MaxRetry, err.Error(), nextVisibleAt); markErr != nil {
					return fmt.Errorf("%v; mark retry status failed: %w", err, markErr)
				}
				e.hooks.OnUploadError(ctx, cfg, item, err)
				return err
			}
			meta, err := decodeMeta(log.MetaJSON)
			if err != nil {
				item := UploadItem{FileName: log.FileName, BackupPath: log.BackupPath, BizKey: log.BizKey}
				if markErr := e.logRepo.MarkRetryOrFailed(ctx, log.ID, log.ClaimID, cfg.MaxRetry, err.Error(), nextVisibleAt); markErr != nil {
					return fmt.Errorf("%v; mark retry status failed: %w", err, markErr)
				}
				e.hooks.OnUploadError(ctx, cfg, item, err)
				return err
			}
			item := UploadItem{FileName: log.FileName, Data: data, BizKey: log.BizKey, Meta: meta, BackupPath: log.BackupPath}
			uploadCtx, uploadItem, err := e.hooks.BeforeUpload(ctx, cfg, item)
			if err != nil {
				if markErr := e.logRepo.MarkRetryOrFailed(ctx, log.ID, log.ClaimID, cfg.MaxRetry, err.Error(), nextVisibleAt); markErr != nil {
					return fmt.Errorf("%v; mark retry status failed: %w", err, markErr)
				}
				e.hooks.OnUploadError(ctx, cfg, item, err)
				return err
			}
			if err := rp.Upload(uploadCtx, cfg, uploadItem); err != nil {
				if markErr := e.logRepo.MarkRetryOrFailed(ctx, log.ID, log.ClaimID, cfg.MaxRetry, err.Error(), nextVisibleAt); markErr != nil {
					return fmt.Errorf("%v; mark retry status failed: %w", err, markErr)
				}
				e.hooks.OnUploadError(uploadCtx, cfg, uploadItem, err)
				return err
			}
			if err := e.logRepo.MarkUploaded(ctx, log.ID, log.ClaimID); err != nil {
				return err
			}
			e.hooks.AfterUpload(uploadCtx, cfg, uploadItem)
		}
	}
}

func (e *Engine) recoverMinuteGaps(ctx context.Context, cfg TaskConfig) error {
	lastEnd, ok, err := e.logRepo.GetLastTimeEndByCode(ctx, cfg.TaskCode)
	if err != nil {
		return err
	}
	if !ok {
		start, end := calcMinuteRange(e.clock.Now(), cfg.DelaySeconds, cfg.IntervalMinutes)
		lastEnd = start.Add(-(end.Sub(start)))
	}
	cutoffStart, _ := calcMinuteRange(e.clock.Now(), cfg.DelaySeconds, cfg.IntervalMinutes)
	step := minuteInterval(cfg.IntervalMinutes)
	attempted := 0
	for t := lastEnd; t.Before(cutoffStart); t = t.Add(step) {
		if e.startupBackfillLimit > 0 && attempted >= e.startupBackfillLimit {
			e.logger.Infof("startup backfill limit reached task=%s limit=%d", cfg.TaskCode, e.startupBackfillLimit)
			break
		}
		start, end := t, t.Add(step)
		exists, err := e.logRepo.ExistsByTaskAndTimeRange(ctx, cfg.TaskCode, start, end)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		attempted++
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
	existingCount, err := e.bigRepo.CountBatches(ctx, inst.ID)
	if err != nil {
		return err
	}
	if err := produceIndexedBatches(ctx, e.productionParallelismFor(cfg), existingCount+1, total, func(ctx context.Context, index int) (BigTaskBatch, error) {
		chunk, err := ds.FetchChunk(ctx, cfg, start, end, index)
		if err != nil {
			return BigTaskBatch{}, err
		}
		if chunk.RecordCount < 0 {
			return BigTaskBatch{}, fmt.Errorf("task=%s invalid record count at batch=%d: %d", cfg.TaskCode, index, chunk.RecordCount)
		}
		fileName := e.fileName(cfg, start, end, index, NameContext{BizKey: chunk.BizKey, Meta: chunk.Meta})
		backupPath, err := e.backup.Save(ctx, cfg.TaskCode, fileName, chunk.Data)
		if err != nil {
			return BigTaskBatch{}, err
		}
		metaJSON, err := encodeMeta(chunk.Meta)
		if err != nil {
			return BigTaskBatch{}, err
		}
		now := e.clock.Now()
		return BigTaskBatch{
			InstanceID:  inst.ID,
			BatchIndex:  index,
			FileName:    fileName,
			RecordCount: chunk.RecordCount,
			BizKey:      chunk.BizKey,
			MetaJSON:    metaJSON,
			BackupPath:  backupPath,
			Status:      StatusPending,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, nil
	}, func(ctx context.Context, batch BigTaskBatch) error {
		if err := e.bigRepo.CreateBatch(ctx, batch); err != nil {
			if isAlreadyExistsErr(err) {
				return nil
			}
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	latestRecords, err := e.bigRepo.SumBatchRecords(ctx, inst.ID)
	if err != nil {
		return err
	}
	return e.bigRepo.UpdateProducedMeta(ctx, inst.ID, total, latestRecords)
}

func (e *Engine) produceBiz(ctx context.Context, cfg TaskConfig, inst BizTaskInstance) error {
	ds, err := e.registry.DataSource(cfg.TaskCode)
	if err != nil {
		return err
	}
	ctx = WithBizTrigger(ctx, BizTrigger{Key: inst.TriggerKey, Payload: inst.TriggerPayload})
	anchor := inst.StartedAt
	if anchor.IsZero() {
		anchor = e.clock.Now()
	}
	start, end := anchor, anchor
	total, err := ds.CountChunks(ctx, cfg, start, end)
	if err != nil {
		return err
	}
	if total < 0 {
		return fmt.Errorf("task=%s invalid chunk count: %d", cfg.TaskCode, total)
	}
	existingCount, err := e.bizRepo.CountBatches(ctx, inst.ID)
	if err != nil {
		return err
	}
	if err := produceIndexedBatches(ctx, e.productionParallelismFor(cfg), existingCount+1, total, func(ctx context.Context, index int) (BizTaskBatch, error) {
		chunk, err := ds.FetchChunk(ctx, cfg, start, end, index)
		if err != nil {
			return BizTaskBatch{}, err
		}
		if chunk.RecordCount < 0 {
			return BizTaskBatch{}, fmt.Errorf("task=%s invalid record count at batch=%d: %d", cfg.TaskCode, index, chunk.RecordCount)
		}
		nameMeta := cloneMeta(chunk.Meta)
		nameMeta["trigger_key"] = inst.TriggerKey
		fileName := e.fileName(cfg, start, end, index, NameContext{BizKey: chunk.BizKey, Meta: nameMeta})
		backupPath, err := e.backup.Save(ctx, cfg.TaskCode, fileName, chunk.Data)
		if err != nil {
			return BizTaskBatch{}, err
		}
		metaJSON, err := encodeMeta(chunk.Meta)
		if err != nil {
			return BizTaskBatch{}, err
		}
		now := e.clock.Now()
		return BizTaskBatch{
			InstanceID:  inst.ID,
			BatchIndex:  index,
			FileName:    fileName,
			RecordCount: chunk.RecordCount,
			BizKey:      chunk.BizKey,
			MetaJSON:    metaJSON,
			BackupPath:  backupPath,
			Status:      StatusPending,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, nil
	}, func(ctx context.Context, batch BizTaskBatch) error {
		if err := e.bizRepo.CreateBatch(ctx, batch); err != nil {
			if isAlreadyExistsErr(err) {
				return nil
			}
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	latestRecords, err := e.bizRepo.SumBatchRecords(ctx, inst.ID)
	if err != nil {
		return err
	}
	return e.bizRepo.UpdateProducedMeta(ctx, inst.ID, total, latestRecords)
}

func (e *Engine) uploadBig(ctx context.Context) error {
	if e.bigRepo == nil {
		return nil
	}
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
	return e.uploadPendingBatches(ctx, cfg, uploadBatchOptions{
		countUploaded: func() (int, error) {
			return e.bigRepo.CountUploadedBatches(ctx, instanceID)
		},
		claimPending: func(limit int, leaseUntil time.Time) ([]uploadBatchRecord, error) {
			batches, err := e.bigRepo.ClaimPendingBatches(ctx, instanceID, cfg.MaxRetry, limit, e.workerID, leaseUntil)
			if err != nil {
				return nil, err
			}
			return toUploadBatchRecordsFromBig(batches), nil
		},
		markRetryOrFailed: func(batchID int64, claimID, errMsg string, nextVisibleAt time.Time) error {
			return e.bigRepo.MarkBatchRetryOrFailed(ctx, batchID, claimID, cfg.MaxRetry, errMsg, nextVisibleAt)
		},
		markUploaded: func(batchID int64, claimID string) error {
			return e.bigRepo.MarkBatchUploaded(ctx, batchID, claimID)
		},
		updateUploaded: func(uploaded int) error {
			return e.bigRepo.UpdateUploadedBatches(ctx, instanceID, uploaded)
		},
		finalize: func(finishedAt time.Time) error {
			return e.bigRepo.FinalizeInstance(ctx, instanceID, finishedAt)
		},
	})
}

func (e *Engine) uploadBiz(ctx context.Context) error {
	instances, err := e.bizRepo.FindRunningInstances(ctx)
	if err != nil {
		return err
	}
	return e.runInParallelBizInstances(instances, func(inst BizTaskInstance) error {
		cfg, err := e.cfgRepo.Get(ctx, inst.TaskCode)
		if err != nil {
			return err
		}
		return e.uploadPendingBizBatches(ctx, cfg, inst.ID)
	})
}

func (e *Engine) uploadPendingBizBatches(ctx context.Context, cfg TaskConfig, instanceID int64) error {
	return e.uploadPendingBatches(ctx, cfg, uploadBatchOptions{
		countUploaded: func() (int, error) {
			return e.bizRepo.CountUploadedBatches(ctx, instanceID)
		},
		claimPending: func(limit int, leaseUntil time.Time) ([]uploadBatchRecord, error) {
			batches, err := e.bizRepo.ClaimPendingBatches(ctx, instanceID, cfg.MaxRetry, limit, e.workerID, leaseUntil)
			if err != nil {
				return nil, err
			}
			return toUploadBatchRecordsFromBiz(batches), nil
		},
		markRetryOrFailed: func(batchID int64, claimID, errMsg string, nextVisibleAt time.Time) error {
			return e.bizRepo.MarkBatchRetryOrFailed(ctx, batchID, claimID, cfg.MaxRetry, errMsg, nextVisibleAt)
		},
		markUploaded: func(batchID int64, claimID string) error {
			return e.bizRepo.MarkBatchUploaded(ctx, batchID, claimID)
		},
		updateUploaded: func(uploaded int) error {
			return e.bizRepo.UpdateUploadedBatches(ctx, instanceID, uploaded)
		},
		finalize: func(finishedAt time.Time) error {
			return e.bizRepo.FinalizeInstance(ctx, instanceID, finishedAt)
		},
	})
}

type uploadBatchRecord struct {
	ID         int64
	FileName   string
	BizKey     string
	MetaJSON   string
	ClaimID    string
	BackupPath string
}

type uploadBatchOptions struct {
	countUploaded     func() (int, error)
	claimPending      func(limit int, leaseUntil time.Time) ([]uploadBatchRecord, error)
	markRetryOrFailed func(batchID int64, claimID, errMsg string, nextVisibleAt time.Time) error
	markUploaded      func(batchID int64, claimID string) error
	updateUploaded    func(uploaded int) error
	finalize          func(finishedAt time.Time) error
}

func toUploadBatchRecordsFromBig(in []BigTaskBatch) []uploadBatchRecord {
	out := make([]uploadBatchRecord, 0, len(in))
	for _, batch := range in {
		out = append(out, uploadBatchRecord{
			ID:         batch.ID,
			FileName:   batch.FileName,
			BizKey:     batch.BizKey,
			MetaJSON:   batch.MetaJSON,
			ClaimID:    batch.ClaimID,
			BackupPath: batch.BackupPath,
		})
	}
	return out
}

func toUploadBatchRecordsFromBiz(in []BizTaskBatch) []uploadBatchRecord {
	out := make([]uploadBatchRecord, 0, len(in))
	for _, batch := range in {
		out = append(out, uploadBatchRecord{
			ID:         batch.ID,
			FileName:   batch.FileName,
			BizKey:     batch.BizKey,
			MetaJSON:   batch.MetaJSON,
			ClaimID:    batch.ClaimID,
			BackupPath: batch.BackupPath,
		})
	}
	return out
}

func (e *Engine) uploadPendingBatches(ctx context.Context, cfg TaskConfig, opts uploadBatchOptions) error {
	rp, err := e.registry.Reporter(cfg.TaskCode)
	if err != nil {
		return err
	}
	uploaded, err := opts.countUploaded()
	if err != nil {
		return err
	}
	attempted := map[int64]struct{}{}
	errs := make([]string, 0)

	handleBatchErr := func(batch uploadBatchRecord, item UploadItem, err error) error {
		nextVisibleAt := e.clock.Now().Add(e.claimLease)
		if markErr := opts.markRetryOrFailed(batch.ID, batch.ClaimID, err.Error(), nextVisibleAt); markErr != nil {
			return fmt.Errorf("batch=%d upload error: %v; mark retry status failed: %w", batch.ID, err, markErr)
		}
		e.hooks.OnUploadError(ctx, cfg, item, err)
		if finalizeErr := opts.finalize(e.clock.Now()); finalizeErr != nil {
			return finalizeErr
		}
		if e.failureMode == UploadFailureFailFast {
			return err
		}
		errs = append(errs, fmt.Sprintf("batch=%d: %v", batch.ID, err))
		return nil
	}

	for {
		queryLimit := e.pendingLimit
		if e.failureMode == UploadFailureContinue {
			queryLimit += len(attempted)
		}
		leaseUntil := e.clock.Now().Add(e.claimLease)
		batches, err := opts.claimPending(queryLimit, leaseUntil)
		if err != nil {
			return err
		}
		if len(batches) == 0 {
			break
		}

		processed := 0
		for _, batch := range batches {
			if _, seen := attempted[batch.ID]; seen {
				continue
			}
			attempted[batch.ID] = struct{}{}
			processed++

			data, err := e.backup.Read(ctx, batch.BackupPath)
			if err != nil {
				if handleErr := handleBatchErr(batch, UploadItem{FileName: batch.FileName, BackupPath: batch.BackupPath, BizKey: batch.BizKey}, err); handleErr != nil {
					return handleErr
				}
				continue
			}
			meta, err := decodeMeta(batch.MetaJSON)
			if err != nil {
				if handleErr := handleBatchErr(batch, UploadItem{FileName: batch.FileName, BackupPath: batch.BackupPath, BizKey: batch.BizKey}, err); handleErr != nil {
					return handleErr
				}
				continue
			}

			item := UploadItem{FileName: batch.FileName, Data: data, BizKey: batch.BizKey, Meta: meta, BackupPath: batch.BackupPath}
			uploadCtx, uploadItem, err := e.hooks.BeforeUpload(ctx, cfg, item)
			if err != nil {
				if handleErr := handleBatchErr(batch, item, err); handleErr != nil {
					return handleErr
				}
				continue
			}
			if err := rp.Upload(uploadCtx, cfg, uploadItem); err != nil {
				if handleErr := handleBatchErr(batch, uploadItem, err); handleErr != nil {
					return handleErr
				}
				continue
			}
			e.hooks.AfterUpload(uploadCtx, cfg, uploadItem)
			if err := opts.markUploaded(batch.ID, batch.ClaimID); err != nil {
				return err
			}
			uploaded++
			if err := opts.updateUploaded(uploaded); err != nil {
				return err
			}
		}
		if processed == 0 {
			break
		}
	}

	if err := opts.finalize(e.clock.Now()); err != nil {
		return err
	}
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, " | "))
	}
	return nil
}

func (e *Engine) resumeBig(ctx context.Context) error {
	if e.bigRepo == nil {
		return nil
	}
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

func (e *Engine) resumeBiz(ctx context.Context) error {
	if e.bizRepo == nil {
		return nil
	}
	instances, err := e.bizRepo.FindRunningInstances(ctx)
	if err != nil {
		return err
	}
	return e.runInParallelBizInstances(instances, func(inst BizTaskInstance) error {
		cfg, err := e.cfgRepo.Get(ctx, inst.TaskCode)
		if err != nil {
			return err
		}
		batchCount, err := e.bizRepo.CountBatches(ctx, inst.ID)
		if err != nil {
			return err
		}
		if inst.TotalBatches > 0 && batchCount < inst.TotalBatches {
			return e.produceBiz(ctx, cfg, inst)
		}
		return e.uploadPendingBizBatches(ctx, cfg, inst.ID)
	})
}

func calcMinuteRange(now time.Time, delaySeconds int, intervalMinutes int) (time.Time, time.Time) {
	interval := minuteInterval(intervalMinutes)
	anchor := now.Add(-time.Duration(delaySeconds) * time.Second).Truncate(time.Minute)
	alignedEndUnix := (anchor.Unix() / int64(interval/time.Second)) * int64(interval/time.Second)
	end := time.Unix(alignedEndUnix, 0).In(anchor.Location())
	start := end.Add(-interval)
	return start, end
}

func minuteInterval(intervalMinutes int) time.Duration {
	if intervalMinutes <= 0 {
		intervalMinutes = 1
	}
	return time.Duration(intervalMinutes) * time.Minute
}

func (e *Engine) runInParallel(configs []TaskConfig, fn func(cfg TaskConfig) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(configs))
	sem := e.newParallelSemaphore()
	for _, cfg := range configs {
		cfg := cfg
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.acquireParallelSlot(sem)
			defer e.releaseParallelSlot(sem)
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
	sem := e.newParallelSemaphore()
	for _, code := range codes {
		code := code
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.acquireParallelSlot(sem)
			defer e.releaseParallelSlot(sem)
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
	sem := e.newParallelSemaphore()
	for _, inst := range instances {
		inst := inst
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.acquireParallelSlot(sem)
			defer e.releaseParallelSlot(sem)
			if err := fn(inst); err != nil {
				errCh <- fmt.Errorf("instance=%d: %w", inst.ID, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	return joinErrors(errCh)
}

func (e *Engine) runInParallelBizInstances(instances []BizTaskInstance, fn func(inst BizTaskInstance) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(instances))
	sem := e.newParallelSemaphore()
	for _, inst := range instances {
		inst := inst
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.acquireParallelSlot(sem)
			defer e.releaseParallelSlot(sem)
			if err := fn(inst); err != nil {
				errCh <- fmt.Errorf("biz_instance=%d: %w", inst.ID, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	return joinErrors(errCh)
}

func (e *Engine) productionParallelismFor(cfg TaskConfig) int {
	if cfg.ProductionParallelism > 0 {
		return cfg.ProductionParallelism
	}
	return e.productionParallelism
}

type indexedProduceResult[T any] struct {
	index int
	value T
	err   error
}

func produceIndexedBatches[T any](
	ctx context.Context,
	parallelism int,
	startIndex int,
	endIndex int,
	build func(context.Context, int) (T, error),
	create func(context.Context, T) error,
) error {
	if startIndex > endIndex {
		return nil
	}
	if parallelism <= 1 {
		for index := startIndex; index <= endIndex; index++ {
			value, err := build(ctx, index)
			if err != nil {
				return fmt.Errorf("batch=%d: %w", index, err)
			}
			if err := create(ctx, value); err != nil {
				return fmt.Errorf("batch=%d: %w", index, err)
			}
		}
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	results := make(chan indexedProduceResult[T], parallelism)
	var wg sync.WaitGroup

	workerCount := parallelism
	total := endIndex - startIndex + 1
	if workerCount > total {
		workerCount = total
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				value, err := build(ctx, index)
				results <- indexedProduceResult[T]{index: index, value: value, err: err}
			}
		}()
	}

	go func() {
		for index := startIndex; index <= endIndex; index++ {
			jobs <- index
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	nextIndex := startIndex
	pending := make(map[int]indexedProduceResult[T], workerCount)
	var firstErr error

	for result := range results {
		pending[result.index] = result
		for {
			result, ok := pending[nextIndex]
			if !ok {
				break
			}
			delete(pending, nextIndex)

			if firstErr == nil {
				if result.err != nil {
					firstErr = fmt.Errorf("batch=%d: %w", nextIndex, result.err)
					cancel()
				} else if err := create(ctx, result.value); err != nil {
					firstErr = fmt.Errorf("batch=%d: %w", nextIndex, err)
					cancel()
				}
			}
			nextIndex++
		}
	}
	return firstErr
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

func (e *Engine) newParallelSemaphore() chan struct{} {
	if e.maxParallel <= 0 {
		return nil
	}
	return make(chan struct{}, e.maxParallel)
}

func (e *Engine) acquireParallelSlot(sem chan struct{}) {
	if sem == nil {
		return
	}
	sem <- struct{}{}
}

func (e *Engine) releaseParallelSlot(sem chan struct{}) {
	if sem == nil {
		return
	}
	<-sem
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

var engineIDSeq atomic.Uint64

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "unknown-host"
	}
	return strings.Join([]string{
		host,
		"pid" + strconv.Itoa(os.Getpid()),
		"e" + strconv.FormatUint(engineIDSeq.Add(1), 10),
	}, "-")
}

type noopLogger struct{}

func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

type noopUploadHooks struct{}

func (noopUploadHooks) BeforeUpload(ctx context.Context, _ TaskConfig, item UploadItem) (context.Context, UploadItem, error) {
	return ctx, item, nil
}

func (noopUploadHooks) AfterUpload(context.Context, TaskConfig, UploadItem) {}

func (noopUploadHooks) OnUploadError(context.Context, TaskConfig, UploadItem, error) {}

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

func cloneMeta(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func isAlreadyExistsErr(err error) bool {
	return errors.Is(err, ErrAlreadyExists)
}
