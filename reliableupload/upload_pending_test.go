package reliableupload

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestUploadMinuteByTaskCode_MarksFailedWhenRetryLimitExceeded(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "minute_demo", MaxRetry: 0}

	logRepo := newFakeUploadLogRepo(cfg.TaskCode, 1)
	backup := &fakeBackupStore{data: map[string][]byte{}}
	reporter := &fakeReporter{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, logRepo, nil, nil, backup)
	err := engine.uploadMinuteByTaskCode(ctx, cfg)
	if err == nil {
		t.Fatalf("expected error when backup read fails")
	}

	log := logRepo.logByID(1)
	if log.Status != StatusFailed {
		t.Fatalf("expected log status failed, got %v", log.Status)
	}
	if log.RetryCount != 1 {
		t.Fatalf("expected retry count 1, got %d", log.RetryCount)
	}
}

func TestUploadMinuteByTaskCode_KeepsPendingWhenRetryHeadroomRemains(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "minute_demo", MaxRetry: 1}

	logRepo := newFakeUploadLogRepo(cfg.TaskCode, 1)
	backup := &fakeBackupStore{data: map[string][]byte{}}
	reporter := &fakeReporter{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, logRepo, nil, nil, backup)
	err := engine.uploadMinuteByTaskCode(ctx, cfg)
	if err == nil {
		t.Fatalf("expected error when backup read fails")
	}

	log := logRepo.logByID(1)
	if log.Status != StatusPending {
		t.Fatalf("expected log status pending, got %v", log.Status)
	}
	if log.RetryCount != 1 {
		t.Fatalf("expected retry count 1, got %d", log.RetryCount)
	}
}

func TestUploadPendingBigBatches_PaginatesUntilDrainedAndUpdatesUploadedBatches(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", MaxRetry: 3}

	repo := newFakeBigRepo("big_demo", 1, 5)
	backup := &fakeBackupStore{data: map[string][]byte{
		"p1": []byte("data-1"),
		"p2": []byte("data-2"),
		"p3": []byte("data-3"),
		"p4": []byte("data-4"),
		"p5": []byte("data-5"),
	}}
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
	if repo.instance.Status != StatusUploaded {
		t.Fatalf("expected uploaded status, got %v", repo.instance.Status)
	}
}

func TestUploadPendingBigBatches_FinalizesFailedWhenRetryLimitExceeded(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", MaxRetry: 0}

	repo := newFakeBigRepo("big_demo", 1, 2)
	repo.batches[0].Status = StatusUploaded
	backup := &fakeBackupStore{data: map[string][]byte{"p1": []byte("data-1")}}
	reporter := &fakeReporter{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, nil, repo, nil, backup)
	err := engine.uploadPendingBigBatches(ctx, cfg, 1)
	if err == nil {
		t.Fatalf("expected error when backup read fails")
	}

	batch := repo.batchByID(2)
	if batch.Status != StatusFailed {
		t.Fatalf("expected failed batch, got %v", batch.Status)
	}
	if repo.instance.Status != StatusFailed {
		t.Fatalf("expected failed instance, got %v", repo.instance.Status)
	}
	if repo.instance.FinishedAt == nil {
		t.Fatalf("expected finished_at to be set")
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
	if repo.instance.Status != StatusRunning {
		t.Fatalf("expected running instance, got %v", repo.instance.Status)
	}
}

func TestUploadPendingBizBatches_PaginatesUntilDrainedAndUpdatesUploadedBatches(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "biz_demo", MaxRetry: 3}

	repo := newFakeBizRepo("biz_demo", 1, 5)
	backup := &fakeBackupStore{data: map[string][]byte{
		"bp1": []byte("biz-data-1"),
		"bp2": []byte("biz-data-2"),
		"bp3": []byte("biz-data-3"),
		"bp4": []byte("biz-data-4"),
		"bp5": []byte("biz-data-5"),
	}}
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
	if repo.instance.Status != StatusUploaded {
		t.Fatalf("expected uploaded status, got %v", repo.instance.Status)
	}
}

func TestUploadPendingBizBatches_FinalizesFailedWhenRetryLimitExceeded(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "biz_demo", MaxRetry: 0}

	repo := newFakeBizRepo("biz_demo", 1, 2)
	repo.batches[0].Status = StatusUploaded
	backup := &fakeBackupStore{data: map[string][]byte{"bp1": []byte("biz-data-1")}}
	reporter := &fakeReporter{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, nil, nil, repo, backup)
	err := engine.uploadPendingBizBatches(ctx, cfg, 1)
	if err == nil {
		t.Fatalf("expected error when backup read fails")
	}

	batch := repo.batchByID(2)
	if batch.Status != StatusFailed {
		t.Fatalf("expected failed batch, got %v", batch.Status)
	}
	if repo.instance.Status != StatusFailed {
		t.Fatalf("expected failed instance, got %v", repo.instance.Status)
	}
	if repo.instance.FinishedAt == nil {
		t.Fatalf("expected finished_at to be set")
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
	if repo.instance.Status != StatusRunning {
		t.Fatalf("expected running instance, got %v", repo.instance.Status)
	}
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

func TestUploadMinuteByTaskCode_ReturnsStatusUpdateErrorWhenMarkRetryFails(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "minute_demo", MaxRetry: 1}

	logRepo := newFakeUploadLogRepo(cfg.TaskCode, 1)
	logRepo.markRetryErr = errors.New("mark retry failed")
	backup := &fakeBackupStore{data: map[string][]byte{}}
	reporter := &fakeReporter{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, logRepo, nil, nil, backup)
	err := engine.uploadMinuteByTaskCode(ctx, cfg)
	if err == nil || !strings.Contains(err.Error(), "mark retry failed") {
		t.Fatalf("expected mark retry error, got %v", err)
	}
}

func TestUploadPendingBigBatches_ReturnsStatusUpdateErrorWhenMarkRetryFails(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", MaxRetry: 3}

	repo := newFakeBigRepo("big_demo", 1, 1)
	repo.markRetryErr = errors.New("mark batch retry failed")
	backup := &fakeBackupStore{data: map[string][]byte{"p1": []byte("data-1")}}
	reporter := &fakeReporter{failOnFile: map[string]struct{}{"big_demo_001.dat": {}}}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, nil, repo, nil, backup)
	err := engine.uploadPendingBigBatches(ctx, cfg, 1)
	if err == nil || !strings.Contains(err.Error(), "mark batch retry failed") {
		t.Fatalf("expected mark retry error, got %v", err)
	}
}

func TestUploadMinuteByTaskCode_DoesNotDuplicateUploadAcrossWorkers(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "minute_demo", MaxRetry: 3}

	logRepo := newFakeUploadLogRepo(cfg.TaskCode, 1)
	backup := &fakeBackupStore{data: map[string][]byte{"lp1": []byte("payload")}}
	reporter := &fakeReporter{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine1 := NewEngine(reg, nil, logRepo, nil, nil, backup, WithWorkerID("worker-1"))
	engine2 := NewEngine(reg, nil, logRepo, nil, nil, backup, WithWorkerID("worker-2"))

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errCh <- engine1.uploadMinuteByTaskCode(ctx, cfg)
	}()
	go func() {
		defer wg.Done()
		errCh <- engine2.uploadMinuteByTaskCode(ctx, cfg)
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("unexpected upload error: %v", err)
		}
	}

	if got := reporter.uploadedCount(); got != 1 {
		t.Fatalf("expected exactly one upload across workers, got %d", got)
	}
	log := logRepo.logByID(1)
	if log.Status != StatusUploaded {
		t.Fatalf("expected final status uploaded, got %v", log.Status)
	}
}

func TestUploadMinuteByTaskCode_CallsUploadHooks(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "minute_demo", MaxRetry: 3}

	logRepo := newFakeUploadLogRepo(cfg.TaskCode, 1)
	backup := &fakeBackupStore{data: map[string][]byte{"lp1": []byte("payload")}}
	reporter := &fakeReporter{}
	hooks := &recordingHooks{}

	reg := NewRegistry()
	reg.RegisterReporter(cfg.TaskCode, reporter)

	engine := NewEngine(reg, nil, logRepo, nil, nil, backup, WithUploadHooks(hooks))
	if err := engine.uploadMinuteByTaskCode(ctx, cfg); err != nil {
		t.Fatalf("uploadMinuteByTaskCode returned error: %v", err)
	}

	if hooks.beforeCount != 1 || hooks.afterCount != 1 || hooks.errorCount != 0 {
		t.Fatalf("unexpected hook calls before=%d after=%d error=%d", hooks.beforeCount, hooks.afterCount, hooks.errorCount)
	}
}
