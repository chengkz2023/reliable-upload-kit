package reliableupload

import (
	"context"
	"io"
	"time"
)

// DataSource is implemented by business teams.
// Framework calls it to fetch and encode data into upload-ready chunks.
type DataSource interface {
	FetchAndEncode(ctx context.Context, cfg TaskConfig, start, end time.Time) ([][]byte, error)
}

// BigDataSource is optional and used only for large-window big tasks.
// It allows the engine to avoid loading all data into memory at once.
// Engine will prefer this interface for TaskTypeBig when implemented.
type BigDataSource interface {
	CountBatches(ctx context.Context, cfg TaskConfig, start, end time.Time) (int, error)
	FetchAndEncodeBatch(ctx context.Context, cfg TaskConfig, start, end time.Time, batchIndex int) ([]byte, error)
}

// Reporter is implemented by business teams for idempotent upload.
// Implementations should treat existing remote files as success.
type Reporter interface {
	Upload(ctx context.Context, cfg TaskConfig, fileName string, data []byte) error
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
	IncrRetry(ctx context.Context, id int64, errMsg string) error
	GetLastTimeEndByCode(ctx context.Context, taskCode string) (time.Time, bool, error)
}

// BigTaskRepo provides big task persistence.
type BigTaskRepo interface {
	GetOrCreateInstance(ctx context.Context, taskCode string, windowStart, windowEnd time.Time) (BigTaskInstance, error)
	UpdateProducedMeta(ctx context.Context, instanceID int64, totalBatches, totalRecords int) error
	CreateBatch(ctx context.Context, batch BigTaskBatch) error
	FindRunningInstances(ctx context.Context) ([]BigTaskInstance, error)
	CountBatches(ctx context.Context, instanceID int64) (int, error)
	FindPendingBatches(ctx context.Context, instanceID int64, maxRetry, limit int) ([]BigTaskBatch, error)
	MarkBatchUploaded(ctx context.Context, batchID int64) error
	IncrBatchRetry(ctx context.Context, batchID int64, errMsg string) error
	CountUploadedBatches(ctx context.Context, instanceID int64) (int, error)
	MarkInstanceCompleted(ctx context.Context, instanceID int64, finishedAt time.Time) error
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

// FileNamer allows custom file naming strategy.
type FileNamer interface {
	MinuteFileName(cfg TaskConfig, start, end time.Time, batchIndex int) string
	BigFileName(cfg TaskConfig, windowStart, windowEnd time.Time, batchIndex int) string
}

// Reconciler can optionally detect backup-vs-log drifts and alert.
type Reconciler interface {
	Reconcile(ctx context.Context) error
}

// BackupReaderWriter helper for implementations using filesystems.
type BackupReaderWriter interface {
	io.ReaderAt
	io.WriterAt
}
