package reliableupload

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestUploadPendingBigBatches_PaginatesUntilDrainedAndUpdatesUploadedBatches(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", MaxRetry: 3}

	repo := newFakeBigRepo("big_demo", 1, 5)
	backup := &fakeBackupStore{data: map[string][]byte{}}
	for i := 1; i <= 5; i++ {
		path := fmt.Sprintf("p%d", i)
		backup.data[path] = []byte(fmt.Sprintf("data-%d", i))
	}
	reporter := &fakeReporter{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, nil, repo, nil, backup, WithPendingLimit(2))
	if err := engine.uploadPendingBigBatches(ctx, cfg, 1); err != nil {
		t.Fatalf("uploadPendingBigBatches returned error: %v", err)
	}

	if reporter.uploadedCount() != 5 {
		t.Fatalf("expected 5 uploaded files, got %d", reporter.uploadedCount())
	}
	if got := repo.uploadedHistory(); !equalInts(got, []int{1, 2, 3, 4, 5}) {
		t.Fatalf("expected uploaded_batches history [1 2 3 4 5], got %v", got)
	}
	if !repo.completed {
		t.Fatalf("expected instance marked completed")
	}
}

func TestUploadPendingBigBatches_UpdatesUploadedBatchesBeforeFailure(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", MaxRetry: 3}

	repo := newFakeBigRepo("big_demo", 1, 3)
	backup := &fakeBackupStore{data: map[string][]byte{
		"p1": []byte("data-1"),
		"p2": []byte("data-2"),
		"p3": []byte("data-3"),
	}}
	reporter := &fakeReporter{failOnFile: map[string]struct{}{"big_demo_002.dat": {}}}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, nil, repo, nil, backup, WithPendingLimit(2))
	err := engine.uploadPendingBigBatches(ctx, cfg, 1)
	if err == nil {
		t.Fatalf("expected error when reporter fails")
	}

	if got := repo.uploadedHistory(); !equalInts(got, []int{1}) {
		t.Fatalf("expected uploaded_batches history [1], got %v", got)
	}
	if repo.completed {
		t.Fatalf("did not expect instance completion on partial upload")
	}
}

type fakeBigRepo struct {
	taskCode string

	mu            sync.Mutex
	batches       []BigTaskBatch
	uploadedTrace []int
	completed     bool
}

func newFakeBigRepo(taskCode string, instanceID int64, total int) *fakeBigRepo {
	out := &fakeBigRepo{taskCode: taskCode}
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

func (r *fakeBigRepo) IncrBatchRetry(_ context.Context, batchID int64, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.batches {
		if r.batches[i].ID == batchID {
			r.batches[i].RetryCount++
			r.batches[i].ErrMsg = errMsg
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

func (r *fakeBigRepo) MarkInstanceCompleted(_ context.Context, instanceID int64, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID != 1 {
		return fmt.Errorf("unexpected instance id: %d", instanceID)
	}
	r.completed = true
	return nil
}

func (r *fakeBigRepo) UpdateUploadedBatches(_ context.Context, instanceID int64, uploaded int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID != 1 {
		return fmt.Errorf("unexpected instance id: %d", instanceID)
	}
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

func TestUploadPendingBizBatches_PaginatesUntilDrainedAndUpdatesUploadedBatches(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "biz_demo", MaxRetry: 3}

	repo := newFakeBizRepo("biz_demo", 1, 5)
	backup := &fakeBackupStore{data: map[string][]byte{}}
	for i := 1; i <= 5; i++ {
		path := fmt.Sprintf("bp%d", i)
		backup.data[path] = []byte(fmt.Sprintf("biz-data-%d", i))
	}
	reporter := &fakeReporter{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, nil, nil, repo, backup, WithPendingLimit(2))
	if err := engine.uploadPendingBizBatches(ctx, cfg, 1); err != nil {
		t.Fatalf("uploadPendingBizBatches returned error: %v", err)
	}

	if reporter.uploadedCount() != 5 {
		t.Fatalf("expected 5 uploaded files, got %d", reporter.uploadedCount())
	}
	if got := repo.uploadedHistory(); !equalInts(got, []int{1, 2, 3, 4, 5}) {
		t.Fatalf("expected uploaded_batches history [1 2 3 4 5], got %v", got)
	}
	if !repo.completed {
		t.Fatalf("expected biz instance marked completed")
	}
}

func TestUploadPendingBizBatches_UpdatesUploadedBatchesBeforeFailure(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "biz_demo", MaxRetry: 3}

	repo := newFakeBizRepo("biz_demo", 1, 3)
	backup := &fakeBackupStore{data: map[string][]byte{
		"bp1": []byte("biz-data-1"),
		"bp2": []byte("biz-data-2"),
		"bp3": []byte("biz-data-3"),
	}}
	reporter := &fakeReporter{failOnFile: map[string]struct{}{"biz_demo_002.dat": {}}}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, nil, nil, repo, backup, WithPendingLimit(2))
	err := engine.uploadPendingBizBatches(ctx, cfg, 1)
	if err == nil {
		t.Fatalf("expected error when reporter fails")
	}

	if got := repo.uploadedHistory(); !equalInts(got, []int{1}) {
		t.Fatalf("expected uploaded_batches history [1], got %v", got)
	}
	if repo.completed {
		t.Fatalf("did not expect biz instance completion on partial upload")
	}
}

type fakeBizRepo struct {
	taskCode string

	mu            sync.Mutex
	batches       []BizTaskBatch
	uploadedTrace []int
	completed     bool
}

func newFakeBizRepo(taskCode string, instanceID int64, total int) *fakeBizRepo {
	out := &fakeBizRepo{taskCode: taskCode}
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

func (r *fakeBizRepo) IncrBatchRetry(_ context.Context, batchID int64, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.batches {
		if r.batches[i].ID == batchID {
			r.batches[i].RetryCount++
			r.batches[i].ErrMsg = errMsg
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

func (r *fakeBizRepo) MarkInstanceCompleted(_ context.Context, instanceID int64, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID != 1 {
		return fmt.Errorf("unexpected instance id: %d", instanceID)
	}
	r.completed = true
	return nil
}

func (r *fakeBizRepo) UpdateUploadedBatches(_ context.Context, instanceID int64, uploaded int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID != 1 {
		return fmt.Errorf("unexpected instance id: %d", instanceID)
	}
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

func TestUploadPendingBigBatches_ContinueOnErrorProcessesLaterBatches(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", MaxRetry: 3}

	repo := newFakeBigRepo("big_demo", 1, 3)
	backup := &fakeBackupStore{data: map[string][]byte{
		"p1": []byte("data-1"),
		"p2": []byte("data-2"),
		"p3": []byte("data-3"),
	}}
	reporter := &fakeReporter{failOnFile: map[string]struct{}{"big_demo_001.dat": {}}}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(
		reg,
		nil,
		nil,
		repo,
		nil,
		backup,
		WithPendingLimit(2),
		WithUploadFailureStrategy(UploadFailureContinue),
	)
	err := engine.uploadPendingBigBatches(ctx, cfg, 1)
	if err == nil {
		t.Fatalf("expected aggregated error when at least one batch failed")
	}

	if reporter.uploadedCount() != 2 {
		t.Fatalf("expected 2 successful uploads, got %d", reporter.uploadedCount())
	}
	if got := repo.uploadedHistory(); !equalInts(got, []int{1, 2}) {
		t.Fatalf("expected uploaded_batches history [1 2], got %v", got)
	}
	if repo.completed {
		t.Fatalf("did not expect completion with remaining failed batch")
	}
}

func TestUploadPendingBigBatches_CallsUploadHooks(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", MaxRetry: 3}

	repo := newFakeBigRepo("big_demo", 1, 1)
	backup := &fakeBackupStore{data: map[string][]byte{"p1": []byte("data-1")}}
	reporter := &fakeReporter{}
	hooks := &recordingHooks{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(
		reg,
		nil,
		nil,
		repo,
		nil,
		backup,
		WithUploadHooks(hooks),
	)
	if err := engine.uploadPendingBigBatches(ctx, cfg, 1); err != nil {
		t.Fatalf("uploadPendingBigBatches returned error: %v", err)
	}
	if hooks.beforeCount != 1 || hooks.afterCount != 1 || hooks.errorCount != 0 {
		t.Fatalf("unexpected hook calls before=%d after=%d error=%d", hooks.beforeCount, hooks.afterCount, hooks.errorCount)
	}
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
