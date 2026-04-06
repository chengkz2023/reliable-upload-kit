package reliableupload

import (
	"context"
	"time"
)

// DataSource is the only data production interface.
// It must provide deterministic chunk count and chunk-by-index fetch.
type DataSource interface {
	CountChunks(ctx context.Context, cfg TaskConfig, start, end time.Time) (int, error)
	FetchChunk(ctx context.Context, cfg TaskConfig, start, end time.Time, index int) (Chunk, error)
}

// Reporter is the only upload interface.
// It receives UploadItem with business metadata.
type Reporter interface {
	Upload(ctx context.Context, cfg TaskConfig, item UploadItem) error
}

// BackupStore persists produced files as the single source for retries.
type BackupStore interface {
	Save(ctx context.Context, taskCode, fileName string, data []byte) (backupPath string, err error)
	Read(ctx context.Context, backupPath string) ([]byte, error)
}

// TaskConfigRepo provides task configurations.
type TaskConfigRepo interface {
	FindEnabledByType(ctx context.Context, typ TaskType) ([]TaskConfig, error)
	Get(ctx context.Context, taskCode string) (TaskConfig, error)
}

// UploadLogRepo provides minute task persistence.
type UploadLogRepo interface {
	ExistsByTaskAndTimeRange(ctx context.Context, taskCode string, start, end time.Time) (bool, error)
	Create(ctx context.Context, log UploadLog) error
	FindDistinctPendingTaskCodes(ctx context.Context) ([]string, error)
	FindPendingByCode(ctx context.Context, taskCode string, maxRetry, limit int) ([]UploadLog, error)
	MarkUploaded(ctx context.Context, id int64) error
	MarkRetryOrFailed(ctx context.Context, id int64, maxRetry int, errMsg string) error
	GetLastTimeEndByCode(ctx context.Context, taskCode string) (time.Time, bool, error)
}

// BigTaskRepo provides big task persistence.
type BigTaskRepo interface {
	GetOrCreateInstance(ctx context.Context, taskCode string, windowStart, windowEnd time.Time) (BigTaskInstance, error)
	UpdateProducedMeta(ctx context.Context, instanceID int64, totalBatches, totalRecords int) error
	CreateBatch(ctx context.Context, batch BigTaskBatch) error
	FindRunningInstances(ctx context.Context) ([]BigTaskInstance, error)
	CountBatches(ctx context.Context, instanceID int64) (int, error)
	SumBatchRecords(ctx context.Context, instanceID int64) (int, error)
	FindPendingBatches(ctx context.Context, instanceID int64, maxRetry, limit int) ([]BigTaskBatch, error)
	MarkBatchUploaded(ctx context.Context, batchID int64) error
	MarkBatchRetryOrFailed(ctx context.Context, batchID int64, maxRetry int, errMsg string) error
	CountUploadedBatches(ctx context.Context, instanceID int64) (int, error)
	UpdateUploadedBatches(ctx context.Context, instanceID int64, uploadedBatches int) error
	FinalizeInstance(ctx context.Context, instanceID int64, finishedAt time.Time) error
}

// BizTaskRepo provides business-trigger task persistence.
type BizTaskRepo interface {
	GetOrCreateInstance(ctx context.Context, taskCode, triggerKey, triggerPayload string) (BizTaskInstance, error)
	UpdateProducedMeta(ctx context.Context, instanceID int64, totalBatches, totalRecords int) error
	CreateBatch(ctx context.Context, batch BizTaskBatch) error
	FindRunningInstances(ctx context.Context) ([]BizTaskInstance, error)
	CountBatches(ctx context.Context, instanceID int64) (int, error)
	SumBatchRecords(ctx context.Context, instanceID int64) (int, error)
	FindPendingBatches(ctx context.Context, instanceID int64, maxRetry, limit int) ([]BizTaskBatch, error)
	MarkBatchUploaded(ctx context.Context, batchID int64) error
	MarkBatchRetryOrFailed(ctx context.Context, batchID int64, maxRetry int, errMsg string) error
	CountUploadedBatches(ctx context.Context, instanceID int64) (int, error)
	UpdateUploadedBatches(ctx context.Context, instanceID int64, uploadedBatches int) error
	FinalizeInstance(ctx context.Context, instanceID int64, finishedAt time.Time) error
}

// UploadFailureStrategy controls behavior when single batch upload fails.
type UploadFailureStrategy uint8

const (
	UploadFailureFailFast UploadFailureStrategy = iota + 1
	UploadFailureContinue
)

// UploadHooks allows optional custom actions around each upload attempt.
type UploadHooks interface {
	BeforeUpload(ctx context.Context, cfg TaskConfig, item UploadItem) (context.Context, UploadItem, error)
	AfterUpload(ctx context.Context, cfg TaskConfig, item UploadItem)
	OnUploadError(ctx context.Context, cfg TaskConfig, item UploadItem, err error)
}

// UploadHookFuncs adapts function callbacks to UploadHooks.
type UploadHookFuncs struct {
	BeforeUploadFunc func(ctx context.Context, cfg TaskConfig, item UploadItem) (context.Context, UploadItem, error)
	AfterUploadFunc  func(ctx context.Context, cfg TaskConfig, item UploadItem)
	OnUploadErrFunc  func(ctx context.Context, cfg TaskConfig, item UploadItem, err error)
}

func (h UploadHookFuncs) BeforeUpload(ctx context.Context, cfg TaskConfig, item UploadItem) (context.Context, UploadItem, error) {
	if h.BeforeUploadFunc == nil {
		return ctx, item, nil
	}
	return h.BeforeUploadFunc(ctx, cfg, item)
}

func (h UploadHookFuncs) AfterUpload(ctx context.Context, cfg TaskConfig, item UploadItem) {
	if h.AfterUploadFunc != nil {
		h.AfterUploadFunc(ctx, cfg, item)
	}
}

func (h UploadHookFuncs) OnUploadError(ctx context.Context, cfg TaskConfig, item UploadItem, err error) {
	if h.OnUploadErrFunc != nil {
		h.OnUploadErrFunc(ctx, cfg, item, err)
	}
}

// Logger is optional; nil-safe no-op logger is used by default.
type Logger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

// LoggerFuncs adapts plain function callbacks to Logger.
// Useful for integrating loggers like zap without creating a custom type.
type LoggerFuncs struct {
	InfofFunc  func(format string, args ...any)
	ErrorfFunc func(format string, args ...any)
}

func (l LoggerFuncs) Infof(format string, args ...any) {
	if l.InfofFunc != nil {
		l.InfofFunc(format, args...)
	}
}

func (l LoggerFuncs) Errorf(format string, args ...any) {
	if l.ErrorfFunc != nil {
		l.ErrorfFunc(format, args...)
	}
}

// Clock allows deterministic tests.
type Clock interface {
	Now() time.Time
}

// FileNamer builds file name with lightweight naming context.
type FileNamer interface {
	FileName(cfg TaskConfig, windowStart, windowEnd time.Time, batchIndex int, ctx NameContext) string
}

// Reconciler can optionally detect backup-vs-log drifts and alert.
type Reconciler interface {
	Reconcile(ctx context.Context) error
}
