package reliableupload

import "time"

// TaskType defines runtime behavior for a task.
type TaskType uint8

const (
	TaskTypeMinute TaskType = 1
	TaskTypeBig    TaskType = 2
)

// Status tracks upload progress.
type Status uint8

const (
	StatusPending  Status = 0
	StatusUploaded Status = 1
	StatusFailed   Status = 2
	StatusRunning  Status = 3 // big task instance only
)

// TaskConfig is runtime configuration loaded from repository.
type TaskConfig struct {
	TaskCode     string
	TaskType     TaskType
	DelaySeconds int
	BatchSize    int
	MaxRetry     int
	SFTPSubdir   string
	FilePrefix   string
	Enabled      bool
}

// UploadLog is the minute-task state record.
type UploadLog struct {
	ID         int64
	TaskCode   string
	TimeStart  time.Time
	TimeEnd    time.Time
	FileName   string
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
	ID         int64
	InstanceID int64
	BatchIndex int
	FileName   string
	BackupPath string
	Status     Status
	RetryCount int
	ErrMsg     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
