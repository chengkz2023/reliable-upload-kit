package reliableupload

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type fakeUploadLogRepo struct {
	mu   sync.Mutex
	logs []UploadLog
	// Injects persistence failure for retry-status updates.
	markRetryErr error
}

func newFakeUploadLogRepo(taskCode string, total int) *fakeUploadLogRepo {
	out := &fakeUploadLogRepo{}
	for i := 1; i <= total; i++ {
		out.logs = append(out.logs, UploadLog{
			ID:         int64(i),
			TaskCode:   taskCode,
			FileName:   fmt.Sprintf("%s_%03d.dat", taskCode, i),
			BackupPath: fmt.Sprintf("lp%d", i),
			Status:     StatusPending,
		})
	}
	return out
}

func (r *fakeUploadLogRepo) ExistsByTaskAndTimeRange(context.Context, string, time.Time, time.Time) (bool, error) {
	return false, errors.New("not implemented")
}

func (r *fakeUploadLogRepo) Create(context.Context, UploadLog) error {
	return errors.New("not implemented")
}

func (r *fakeUploadLogRepo) FindDistinctPendingTaskCodes(_ context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]struct{}{}
	codes := make([]string, 0)
	for _, log := range r.logs {
		if log.Status != StatusPending {
			continue
		}
		if _, ok := seen[log.TaskCode]; ok {
			continue
		}
		seen[log.TaskCode] = struct{}{}
		codes = append(codes, log.TaskCode)
	}
	sort.Strings(codes)
	return codes, nil
}

func (r *fakeUploadLogRepo) FindPendingByCode(_ context.Context, taskCode string, maxRetry, limit int) ([]UploadLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]UploadLog, 0, limit)
	for _, log := range r.logs {
		if log.TaskCode == taskCode && log.Status == StatusPending && log.RetryCount <= maxRetry {
			out = append(out, log)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeUploadLogRepo) MarkUploaded(_ context.Context, id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.logs {
		if r.logs[i].ID == id {
			r.logs[i].Status = StatusUploaded
			return nil
		}
	}
	return fmt.Errorf("log not found: %d", id)
}

func (r *fakeUploadLogRepo) MarkRetryOrFailed(_ context.Context, id int64, maxRetry int, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.markRetryErr != nil {
		return r.markRetryErr
	}
	for i := range r.logs {
		if r.logs[i].ID == id {
			r.logs[i].RetryCount++
			r.logs[i].ErrMsg = errMsg
			if r.logs[i].RetryCount > maxRetry {
				r.logs[i].Status = StatusFailed
			}
			return nil
		}
	}
	return fmt.Errorf("log not found: %d", id)
}

func (r *fakeUploadLogRepo) GetLastTimeEndByCode(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, errors.New("not implemented")
}

func (r *fakeUploadLogRepo) logByID(id int64) UploadLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, log := range r.logs {
		if log.ID == id {
			return log
		}
	}
	return UploadLog{}
}

type fakeBigRepo struct {
	taskCode string

	mu            sync.Mutex
	batches       []BigTaskBatch
	uploadedTrace []int
	instance      BigTaskInstance
	completed     bool
	failed        bool
	// Injects persistence failure for retry-status updates.
	markRetryErr error
}

func newFakeBigRepo(taskCode string, instanceID int64, total int) *fakeBigRepo {
	out := &fakeBigRepo{
		taskCode: taskCode,
		instance: BigTaskInstance{ID: instanceID, TaskCode: taskCode, Status: StatusRunning, TotalBatches: total},
	}
	for i := 1; i <= total; i++ {
		out.batches = append(out.batches, BigTaskBatch{
			ID:         int64(i),
			InstanceID: instanceID,
			BatchIndex: i,
			FileName:   fmt.Sprintf("%s_%03d.dat", taskCode, i),
			BackupPath: fmt.Sprintf("p%d", i),
			Status:     StatusPending,
		})
	}
	return out
}

func (r *fakeBigRepo) GetOrCreateInstance(context.Context, string, time.Time, time.Time) (BigTaskInstance, error) {
	return BigTaskInstance{}, errors.New("not implemented")
}

func (r *fakeBigRepo) UpdateProducedMeta(context.Context, int64, int, int) error {
	return errors.New("not implemented")
}

func (r *fakeBigRepo) CreateBatch(context.Context, BigTaskBatch) error {
	return errors.New("not implemented")
}

func (r *fakeBigRepo) FindRunningInstances(context.Context) ([]BigTaskInstance, error) {
	return []BigTaskInstance{{ID: 1, TaskCode: r.taskCode, Status: StatusRunning}}, nil
}

func (r *fakeBigRepo) CountBatches(_ context.Context, instanceID int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, b := range r.batches {
		if b.InstanceID == instanceID {
			count++
		}
	}
	return count, nil
}

func (r *fakeBigRepo) SumBatchRecords(context.Context, int64) (int, error) {
	return 0, errors.New("not implemented")
}

func (r *fakeBigRepo) FindPendingBatches(_ context.Context, instanceID int64, maxRetry, limit int) ([]BigTaskBatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]BigTaskBatch, 0, limit)
	for _, b := range r.batches {
		if b.InstanceID == instanceID && b.Status == StatusPending && b.RetryCount <= maxRetry {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BatchIndex < out[j].BatchIndex })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeBigRepo) MarkBatchUploaded(_ context.Context, batchID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.batches {
		if r.batches[i].ID == batchID {
			r.batches[i].Status = StatusUploaded
			return nil
		}
	}
	return fmt.Errorf("batch not found: %d", batchID)
}

func (r *fakeBigRepo) MarkBatchRetryOrFailed(_ context.Context, batchID int64, maxRetry int, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.markRetryErr != nil {
		return r.markRetryErr
	}
	for i := range r.batches {
		if r.batches[i].ID == batchID {
			r.batches[i].RetryCount++
			r.batches[i].ErrMsg = errMsg
			if r.batches[i].RetryCount > maxRetry {
				r.batches[i].Status = StatusFailed
			}
			return nil
		}
	}
	return fmt.Errorf("batch not found: %d", batchID)
}

func (r *fakeBigRepo) CountUploadedBatches(_ context.Context, instanceID int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, b := range r.batches {
		if b.InstanceID == instanceID && b.Status == StatusUploaded {
			count++
		}
	}
	return count, nil
}

func (r *fakeBigRepo) FinalizeInstance(_ context.Context, instanceID int64, finishedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID != r.instance.ID {
		return fmt.Errorf("unexpected instance id: %d", instanceID)
	}
	total := 0
	uploaded := 0
	failed := 0
	for _, b := range r.batches {
		if b.InstanceID != instanceID {
			continue
		}
		total++
		switch b.Status {
		case StatusUploaded:
			uploaded++
		case StatusFailed:
			failed++
		}
	}
	r.instance.TotalBatches = total
	r.instance.UploadedBatches = uploaded
	switch {
	case total == 0 || uploaded >= total:
		r.instance.Status = StatusUploaded
		r.instance.FinishedAt = &finishedAt
		r.completed = true
		r.failed = false
	case failed > 0 && uploaded+failed >= total:
		r.instance.Status = StatusFailed
		r.instance.FinishedAt = &finishedAt
		r.completed = false
		r.failed = true
	default:
		r.instance.Status = StatusRunning
		r.instance.FinishedAt = nil
		r.completed = false
		r.failed = false
	}
	return nil
}

func (r *fakeBigRepo) UpdateUploadedBatches(_ context.Context, instanceID int64, uploaded int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID != r.instance.ID {
		return fmt.Errorf("unexpected instance id: %d", instanceID)
	}
	r.instance.UploadedBatches = uploaded
	r.uploadedTrace = append(r.uploadedTrace, uploaded)
	return nil
}

func (r *fakeBigRepo) uploadedHistory() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.uploadedTrace))
	copy(out, r.uploadedTrace)
	return out
}

func (r *fakeBigRepo) batchByID(id int64) BigTaskBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, batch := range r.batches {
		if batch.ID == id {
			return batch
		}
	}
	return BigTaskBatch{}
}

type fakeBizRepo struct {
	taskCode string

	mu            sync.Mutex
	batches       []BizTaskBatch
	uploadedTrace []int
	instance      BizTaskInstance
	completed     bool
	failed        bool
	// Injects persistence failure for retry-status updates.
	markRetryErr error
}

func newFakeBizRepo(taskCode string, instanceID int64, total int) *fakeBizRepo {
	out := &fakeBizRepo{
		taskCode: taskCode,
		instance: BizTaskInstance{ID: instanceID, TaskCode: taskCode, Status: StatusRunning, TotalBatches: total},
	}
	for i := 1; i <= total; i++ {
		out.batches = append(out.batches, BizTaskBatch{
			ID:         int64(i),
			InstanceID: instanceID,
			BatchIndex: i,
			FileName:   fmt.Sprintf("%s_%03d.dat", taskCode, i),
			BackupPath: fmt.Sprintf("bp%d", i),
			Status:     StatusPending,
		})
	}
	return out
}

func (r *fakeBizRepo) GetOrCreateInstance(context.Context, string, string, string) (BizTaskInstance, error) {
	return BizTaskInstance{}, errors.New("not implemented")
}

func (r *fakeBizRepo) UpdateProducedMeta(context.Context, int64, int, int) error {
	return errors.New("not implemented")
}

func (r *fakeBizRepo) CreateBatch(context.Context, BizTaskBatch) error {
	return errors.New("not implemented")
}

func (r *fakeBizRepo) FindRunningInstances(context.Context) ([]BizTaskInstance, error) {
	return []BizTaskInstance{{ID: 1, TaskCode: r.taskCode, Status: StatusRunning}}, nil
}

func (r *fakeBizRepo) CountBatches(_ context.Context, instanceID int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, b := range r.batches {
		if b.InstanceID == instanceID {
			count++
		}
	}
	return count, nil
}

func (r *fakeBizRepo) SumBatchRecords(context.Context, int64) (int, error) {
	return 0, errors.New("not implemented")
}

func (r *fakeBizRepo) FindPendingBatches(_ context.Context, instanceID int64, maxRetry, limit int) ([]BizTaskBatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]BizTaskBatch, 0, limit)
	for _, b := range r.batches {
		if b.InstanceID == instanceID && b.Status == StatusPending && b.RetryCount <= maxRetry {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BatchIndex < out[j].BatchIndex })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeBizRepo) MarkBatchUploaded(_ context.Context, batchID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.batches {
		if r.batches[i].ID == batchID {
			r.batches[i].Status = StatusUploaded
			return nil
		}
	}
	return fmt.Errorf("batch not found: %d", batchID)
}

func (r *fakeBizRepo) MarkBatchRetryOrFailed(_ context.Context, batchID int64, maxRetry int, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.markRetryErr != nil {
		return r.markRetryErr
	}
	for i := range r.batches {
		if r.batches[i].ID == batchID {
			r.batches[i].RetryCount++
			r.batches[i].ErrMsg = errMsg
			if r.batches[i].RetryCount > maxRetry {
				r.batches[i].Status = StatusFailed
			}
			return nil
		}
	}
	return fmt.Errorf("batch not found: %d", batchID)
}

func (r *fakeBizRepo) CountUploadedBatches(_ context.Context, instanceID int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, b := range r.batches {
		if b.InstanceID == instanceID && b.Status == StatusUploaded {
			count++
		}
	}
	return count, nil
}

func (r *fakeBizRepo) FinalizeInstance(_ context.Context, instanceID int64, finishedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID != r.instance.ID {
		return fmt.Errorf("unexpected instance id: %d", instanceID)
	}
	total := 0
	uploaded := 0
	failed := 0
	for _, b := range r.batches {
		if b.InstanceID != instanceID {
			continue
		}
		total++
		switch b.Status {
		case StatusUploaded:
			uploaded++
		case StatusFailed:
			failed++
		}
	}
	r.instance.TotalBatches = total
	r.instance.UploadedBatches = uploaded
	switch {
	case total == 0 || uploaded >= total:
		r.instance.Status = StatusUploaded
		r.instance.FinishedAt = &finishedAt
		r.completed = true
		r.failed = false
	case failed > 0 && uploaded+failed >= total:
		r.instance.Status = StatusFailed
		r.instance.FinishedAt = &finishedAt
		r.completed = false
		r.failed = true
	default:
		r.instance.Status = StatusRunning
		r.instance.FinishedAt = nil
		r.completed = false
		r.failed = false
	}
	return nil
}

func (r *fakeBizRepo) UpdateUploadedBatches(_ context.Context, instanceID int64, uploaded int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID != r.instance.ID {
		return fmt.Errorf("unexpected instance id: %d", instanceID)
	}
	r.instance.UploadedBatches = uploaded
	r.uploadedTrace = append(r.uploadedTrace, uploaded)
	return nil
}

func (r *fakeBizRepo) uploadedHistory() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.uploadedTrace))
	copy(out, r.uploadedTrace)
	return out
}

func (r *fakeBizRepo) batchByID(id int64) BizTaskBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, batch := range r.batches {
		if batch.ID == id {
			return batch
		}
	}
	return BizTaskBatch{}
}

type fakeBackupStore struct {
	data map[string][]byte
}

func (b *fakeBackupStore) Save(context.Context, string, string, []byte) (string, error) {
	return "", errors.New("not implemented")
}

func (b *fakeBackupStore) Read(_ context.Context, backupPath string) ([]byte, error) {
	data, ok := b.data[backupPath]
	if !ok {
		return nil, fmt.Errorf("missing backup path: %s", backupPath)
	}
	return data, nil
}

type fakeReporter struct {
	mu         sync.Mutex
	failOnFile map[string]struct{}
	uploaded   []string
}

func (r *fakeReporter) Upload(_ context.Context, _ TaskConfig, item UploadItem) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, fail := r.failOnFile[item.FileName]; fail {
		return fmt.Errorf("upload failed for file %s", item.FileName)
	}
	r.uploaded = append(r.uploaded, item.FileName)
	return nil
}

func (r *fakeReporter) uploadedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.uploaded)
}

type recordingHooks struct {
	beforeCount int
	afterCount  int
	errorCount  int
}

func (h *recordingHooks) BeforeUpload(ctx context.Context, _ TaskConfig, item UploadItem) (context.Context, UploadItem, error) {
	h.beforeCount++
	return ctx, item, nil
}

func (h *recordingHooks) AfterUpload(context.Context, TaskConfig, UploadItem) {
	h.afterCount++
}

func (h *recordingHooks) OnUploadError(context.Context, TaskConfig, UploadItem, error) {
	h.errorCount++
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
