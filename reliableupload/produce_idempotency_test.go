package reliableupload

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProduceRange_IgnoresAlreadyExistsForZeroChunkMarker(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "minute_demo", FilePrefix: "minute_demo"}
	start := time.Date(2026, 4, 11, 10, 0, 0, 0, time.Local)
	end := start.Add(time.Minute)

	reg := NewRegistry()
	reg.RegisterDataSource(cfg.TaskCode, fixedCountDataSource{count: 0})
	logRepo := &idempotentLogRepo{createErr: ErrAlreadyExists}
	engine := NewEngine(reg, nil, logRepo, nil, nil, &noopBackupStore{})

	if err := engine.produceRange(ctx, cfg, start, end); err != nil {
		t.Fatalf("expected nil when marker already exists, got %v", err)
	}
}

func TestProduceRange_IgnoresAlreadyExistsForChunkLog(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "minute_demo", FilePrefix: "minute_demo"}
	start := time.Date(2026, 4, 11, 10, 0, 0, 0, time.Local)
	end := start.Add(time.Minute)

	reg := NewRegistry()
	reg.RegisterDataSource(cfg.TaskCode, fixedCountDataSource{
		count: 1,
		chunk: Chunk{Data: []byte("payload"), RecordCount: 10},
	})
	logRepo := &idempotentLogRepo{createErr: ErrAlreadyExists}
	engine := NewEngine(reg, nil, logRepo, nil, nil, &noopBackupStore{})

	if err := engine.produceRange(ctx, cfg, start, end); err != nil {
		t.Fatalf("expected nil when chunk log already exists, got %v", err)
	}
}

func TestProduceBig_IgnoresAlreadyExistsBatchAndUsesLatestRecordSum(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", TaskType: TaskTypeBig, FilePrefix: "big_demo"}
	inst := BigTaskInstance{
		ID:          1,
		TaskCode:    cfg.TaskCode,
		WindowStart: time.Date(2026, 4, 11, 9, 0, 0, 0, time.Local),
		WindowEnd:   time.Date(2026, 4, 11, 10, 0, 0, 0, time.Local),
	}

	reg := NewRegistry()
	reg.RegisterDataSource(cfg.TaskCode, fixedCountDataSource{
		count: 1,
		chunk: Chunk{Data: []byte("payload"), RecordCount: 100},
	})
	bigRepo := &idempotentBigRepo{
		countBatches:       0,
		sumBatchRecordsSeq: []int{100},
		createBatchErr:     ErrAlreadyExists,
	}
	engine := NewEngine(reg, nil, nil, bigRepo, nil, &noopBackupStore{})

	if err := engine.produceBig(ctx, cfg, inst); err != nil {
		t.Fatalf("expected nil when batch already exists, got %v", err)
	}
	if bigRepo.updatedTotalBatches != 1 || bigRepo.updatedTotalRecords != 100 {
		t.Fatalf("unexpected produced meta totalBatches=%d totalRecords=%d", bigRepo.updatedTotalBatches, bigRepo.updatedTotalRecords)
	}
}

func TestProduceBiz_IgnoresAlreadyExistsBatchAndUsesLatestRecordSum(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "biz_demo", TaskType: TaskTypeBiz, FilePrefix: "biz_demo"}
	inst := BizTaskInstance{
		ID:             1,
		TaskCode:       cfg.TaskCode,
		TriggerKey:     "key-1",
		TriggerPayload: "{}",
		StartedAt:      time.Date(2026, 4, 11, 10, 0, 0, 0, time.Local),
	}

	reg := NewRegistry()
	reg.RegisterDataSource(cfg.TaskCode, fixedCountDataSource{
		count: 1,
		chunk: Chunk{Data: []byte("payload"), RecordCount: 50},
	})
	bizRepo := &idempotentBizRepo{
		countBatches:       0,
		sumBatchRecordsSeq: []int{50},
		createBatchErr:     ErrAlreadyExists,
	}
	engine := NewEngine(reg, nil, nil, nil, bizRepo, &noopBackupStore{})

	if err := engine.produceBiz(ctx, cfg, inst); err != nil {
		t.Fatalf("expected nil when batch already exists, got %v", err)
	}
	if bizRepo.updatedTotalBatches != 1 || bizRepo.updatedTotalRecords != 50 {
		t.Fatalf("unexpected produced meta totalBatches=%d totalRecords=%d", bizRepo.updatedTotalBatches, bizRepo.updatedTotalRecords)
	}
}

type fixedCountDataSource struct {
	count int
	chunk Chunk
}

func (d fixedCountDataSource) CountChunks(context.Context, TaskConfig, time.Time, time.Time) (int, error) {
	return d.count, nil
}

func (d fixedCountDataSource) FetchChunk(context.Context, TaskConfig, time.Time, time.Time, int) (Chunk, error) {
	return d.chunk, nil
}

type noopBackupStore struct{}

func (noopBackupStore) Save(context.Context, string, string, []byte) (string, error) {
	return "backup/path", nil
}

func (noopBackupStore) Read(context.Context, string) ([]byte, error) {
	return nil, errors.New("not implemented")
}

type idempotentLogRepo struct {
	createErr error
}

func (r *idempotentLogRepo) ExistsByTaskAndTimeRange(context.Context, string, time.Time, time.Time) (bool, error) {
	return false, nil
}

func (r *idempotentLogRepo) Create(context.Context, UploadLog) error {
	return r.createErr
}

func (r *idempotentLogRepo) FindDistinctPendingTaskCodes(context.Context) ([]string, error) {
	return nil, nil
}

func (r *idempotentLogRepo) ClaimPendingByCode(context.Context, string, int, int, string, time.Time) ([]UploadLog, error) {
	return nil, nil
}

func (r *idempotentLogRepo) MarkUploaded(context.Context, int64, string) error {
	return nil
}

func (r *idempotentLogRepo) MarkRetryOrFailed(context.Context, int64, string, int, string, time.Time) error {
	return nil
}

func (r *idempotentLogRepo) GetLastTimeEndByCode(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

type idempotentBigRepo struct {
	countBatches       int
	sumBatchRecordsSeq []int
	sumIdx             int
	createBatchErr     error

	updatedTotalBatches int
	updatedTotalRecords int
}

func (r *idempotentBigRepo) GetOrCreateInstance(context.Context, string, time.Time, time.Time) (BigTaskInstance, error) {
	return BigTaskInstance{}, errors.New("not implemented")
}

func (r *idempotentBigRepo) UpdateProducedMeta(_ context.Context, _ int64, totalBatches, totalRecords int) error {
	r.updatedTotalBatches = totalBatches
	r.updatedTotalRecords = totalRecords
	return nil
}

func (r *idempotentBigRepo) CreateBatch(context.Context, BigTaskBatch) error {
	return r.createBatchErr
}

func (r *idempotentBigRepo) FindRunningInstances(context.Context) ([]BigTaskInstance, error) {
	return nil, nil
}

func (r *idempotentBigRepo) CountBatches(context.Context, int64) (int, error) {
	return r.countBatches, nil
}

func (r *idempotentBigRepo) SumBatchRecords(context.Context, int64) (int, error) {
	if len(r.sumBatchRecordsSeq) == 0 {
		return 0, nil
	}
	if r.sumIdx >= len(r.sumBatchRecordsSeq) {
		return r.sumBatchRecordsSeq[len(r.sumBatchRecordsSeq)-1], nil
	}
	v := r.sumBatchRecordsSeq[r.sumIdx]
	r.sumIdx++
	return v, nil
}

func (r *idempotentBigRepo) ClaimPendingBatches(context.Context, int64, int, int, string, time.Time) ([]BigTaskBatch, error) {
	return nil, nil
}

func (r *idempotentBigRepo) MarkBatchUploaded(context.Context, int64, string) error {
	return nil
}

func (r *idempotentBigRepo) MarkBatchRetryOrFailed(context.Context, int64, string, int, string, time.Time) error {
	return nil
}

func (r *idempotentBigRepo) CountUploadedBatches(context.Context, int64) (int, error) {
	return 0, nil
}

func (r *idempotentBigRepo) UpdateUploadedBatches(context.Context, int64, int) error {
	return nil
}

func (r *idempotentBigRepo) FinalizeInstance(context.Context, int64, time.Time) error {
	return nil
}

type idempotentBizRepo struct {
	countBatches       int
	sumBatchRecordsSeq []int
	sumIdx             int
	createBatchErr     error

	updatedTotalBatches int
	updatedTotalRecords int
}

func (r *idempotentBizRepo) GetOrCreateInstance(context.Context, string, string, string) (BizTaskInstance, error) {
	return BizTaskInstance{}, errors.New("not implemented")
}

func (r *idempotentBizRepo) UpdateProducedMeta(_ context.Context, _ int64, totalBatches, totalRecords int) error {
	r.updatedTotalBatches = totalBatches
	r.updatedTotalRecords = totalRecords
	return nil
}

func (r *idempotentBizRepo) CreateBatch(context.Context, BizTaskBatch) error {
	return r.createBatchErr
}

func (r *idempotentBizRepo) FindRunningInstances(context.Context) ([]BizTaskInstance, error) {
	return nil, nil
}

func (r *idempotentBizRepo) CountBatches(context.Context, int64) (int, error) {
	return r.countBatches, nil
}

func (r *idempotentBizRepo) SumBatchRecords(context.Context, int64) (int, error) {
	if len(r.sumBatchRecordsSeq) == 0 {
		return 0, nil
	}
	if r.sumIdx >= len(r.sumBatchRecordsSeq) {
		return r.sumBatchRecordsSeq[len(r.sumBatchRecordsSeq)-1], nil
	}
	v := r.sumBatchRecordsSeq[r.sumIdx]
	r.sumIdx++
	return v, nil
}

func (r *idempotentBizRepo) ClaimPendingBatches(context.Context, int64, int, int, string, time.Time) ([]BizTaskBatch, error) {
	return nil, nil
}

func (r *idempotentBizRepo) MarkBatchUploaded(context.Context, int64, string) error {
	return nil
}

func (r *idempotentBizRepo) MarkBatchRetryOrFailed(context.Context, int64, string, int, string, time.Time) error {
	return nil
}

func (r *idempotentBizRepo) CountUploadedBatches(context.Context, int64) (int, error) {
	return 0, nil
}

func (r *idempotentBizRepo) UpdateUploadedBatches(context.Context, int64, int) error {
	return nil
}

func (r *idempotentBizRepo) FinalizeInstance(context.Context, int64, time.Time) error {
	return nil
}
