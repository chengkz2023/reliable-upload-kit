package reliableupload

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunInParallel_RespectsMaxParallelLimit(t *testing.T) {
	engine := NewEngine(nil, nil, nil, nil, nil, nil, WithMaxParallel(1))
	configs := []TaskConfig{
		{TaskCode: "a"},
		{TaskCode: "b"},
		{TaskCode: "c"},
		{TaskCode: "d"},
	}

	var active int32
	var maxSeen int32
	err := engine.runInParallel(configs, func(_ TaskConfig) error {
		curr := atomic.AddInt32(&active, 1)
		for {
			old := atomic.LoadInt32(&maxSeen)
			if curr <= old {
				break
			}
			if atomic.CompareAndSwapInt32(&maxSeen, old, curr) {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return nil
	})
	if err != nil {
		t.Fatalf("runInParallel returned error: %v", err)
	}
	if got := atomic.LoadInt32(&maxSeen); got > 1 {
		t.Fatalf("expected max parallel <= 1, got %d", got)
	}
}

func TestRecoverMinuteGaps_RespectsStartupBackfillLimit(t *testing.T) {
	now := time.Date(2026, 4, 11, 10, 10, 30, 0, time.Local)
	cfg := TaskConfig{
		TaskCode:        "minute_demo",
		TaskType:        TaskTypeMinute,
		IntervalMinutes: 1,
		DelaySeconds:    0,
		MaxRetry:        3,
		FilePrefix:      "minute_demo",
	}
	logRepo := &backfillLimitUploadLogRepo{lastEnd: now.Add(-5 * time.Minute).Truncate(time.Minute)}
	reg := NewRegistry()
	reg.RegisterDataSource(cfg.TaskCode, zeroChunkDataSource{})
	engine := NewEngine(reg, nil, logRepo, nil, nil, nil, WithClock(fixedClock{now}), WithStartupBackfillLimit(2))

	if err := engine.recoverMinuteGaps(context.Background(), cfg); err != nil {
		t.Fatalf("recoverMinuteGaps returned error: %v", err)
	}
	if got := logRepo.createdCount(); got != 2 {
		t.Fatalf("expected exactly 2 backfilled windows, got %d", got)
	}
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

type zeroChunkDataSource struct{}

func (zeroChunkDataSource) CountChunks(context.Context, TaskConfig, time.Time, time.Time) (int, error) {
	return 0, nil
}

func (zeroChunkDataSource) FetchChunk(context.Context, TaskConfig, time.Time, time.Time, int) (Chunk, error) {
	return Chunk{}, nil
}

type backfillLimitUploadLogRepo struct {
	mu      sync.Mutex
	lastEnd time.Time
	created int
}

func (r *backfillLimitUploadLogRepo) ExistsByTaskAndTimeRange(context.Context, string, time.Time, time.Time) (bool, error) {
	return false, nil
}

func (r *backfillLimitUploadLogRepo) Create(context.Context, UploadLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.created++
	return nil
}

func (r *backfillLimitUploadLogRepo) FindDistinctPendingTaskCodes(context.Context) ([]string, error) {
	return nil, nil
}

func (r *backfillLimitUploadLogRepo) ClaimPendingByCode(context.Context, string, int, int, string, time.Time) ([]UploadLog, error) {
	return nil, nil
}

func (r *backfillLimitUploadLogRepo) MarkUploaded(context.Context, int64, string) error {
	return nil
}

func (r *backfillLimitUploadLogRepo) MarkRetryOrFailed(context.Context, int64, string, int, string, time.Time) error {
	return nil
}

func (r *backfillLimitUploadLogRepo) GetLastTimeEndByCode(context.Context, string) (time.Time, bool, error) {
	return r.lastEnd, true, nil
}

func (r *backfillLimitUploadLogRepo) createdCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.created
}
