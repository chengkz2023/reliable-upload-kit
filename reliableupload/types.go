package reliableupload

import "time"

// TaskType defines runtime behavior for a task.
type TaskType uint8

const (
	TaskTypeMinute TaskType = 1
	TaskTypeBig    TaskType = 2
	TaskTypeBiz    TaskType = 3
)

// Status tracks upload progress.
type Status uint8

const (
	StatusPending  Status = 0
	StatusUploaded Status = 1
	StatusFailed   Status = 2
	StatusRunning  Status = 3 // task instance running or uploader-claimed batch/log
)

// TaskConfig is runtime configuration loaded from repository.
type TaskConfig struct {
	TaskCode string
	TaskType TaskType
	// IntervalMinutes controls minute-task window size.
	// Example: 1, 5, 10...
	IntervalMinutes int
	DelaySeconds    int
	BatchSize       int
	MaxRetry        int
	SFTPSubdir      string
	FilePrefix      string
	Enabled         bool
}

// Chunk is one upload unit with optional business metadata.
// BizKey/Meta can drive file naming and reporting behavior.
type Chunk struct {
	Data []byte
	// RecordCount is the business row count represented by this chunk.
	// Engine uses it to persist BigTaskInstance/BizTaskInstance.TotalRecords.
	RecordCount int
	BizKey      string
	Meta        map[string]string
}

// NameContext carries lightweight naming metadata only.
type NameContext struct {
	BizKey string
	Meta   map[string]string
}

// UploadItem is the runtime payload passed to enhanced reporter.
type UploadItem struct {
	FileName   string
	Data       []byte
	BizKey     string
	Meta       map[string]string
	BackupPath string
}

// UploadLog is the minute-task state record.
type UploadLog struct {
	ID         int64
	TaskCode   string
	TimeStart  time.Time
	TimeEnd    time.Time
	FileName   string
	BizKey     string
	MetaJSON   string
	ClaimID    string
	Owner      string
	LeaseUntil *time.Time
	Status     Status
	BackupPath string
	RetryCount int
	ErrMsg     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// BigTaskInstance tracks one (task_code, window_start, window_end) run.
type BigTaskInstance struct {
	ID              int64
	TaskCode        string
	WindowStart     time.Time
	WindowEnd       time.Time
	Status          Status
	TotalBatches    int
	UploadedBatches int
	TotalRecords    int
	StartedAt       time.Time
	FinishedAt      *time.Time
}

// BigTaskBatch tracks one file batch for a big task instance.
type BigTaskBatch struct {
	ID          int64
	InstanceID  int64
	BatchIndex  int
	FileName    string
	RecordCount int
	BizKey      string
	MetaJSON    string
	ClaimID     string
	Owner       string
	LeaseUntil  *time.Time
	BackupPath  string
	Status      Status
	RetryCount  int
	ErrMsg      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// BizTrigger carries one business-trigger event context.
type BizTrigger struct {
	Key     string
	Payload string
}

// BizTaskInstance tracks one (task_code, trigger_key) run.
type BizTaskInstance struct {
	ID              int64
	TaskCode        string
	TriggerKey      string
	TriggerPayload  string
	Status          Status
	TotalBatches    int
	UploadedBatches int
	TotalRecords    int
	StartedAt       time.Time
	FinishedAt      *time.Time
}

// BizTaskBatch tracks one file batch for a biz task instance.
type BizTaskBatch struct {
	ID          int64
	InstanceID  int64
	BatchIndex  int
	FileName    string
	RecordCount int
	BizKey      string
	MetaJSON    string
	ClaimID     string
	Owner       string
	LeaseUntil  *time.Time
	BackupPath  string
	Status      Status
	RetryCount  int
	ErrMsg      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
