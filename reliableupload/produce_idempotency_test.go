package reliableupload

import (
	"context"
	"errors"
	"sync"
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

func TestProduceBig_ProductionParallelismFetchesConcurrentlyAndCreatesInOrder(t *testing.T) {
	ctx := context.Background()
	cfg := TaskConfig{TaskCode: "big_demo", TaskType: TaskTypeBig, FilePrefix: "big_demo", ProductionParallelism: 3}
	inst := BigTaskInstance{
		ID:          1,
		TaskCode:    cfg.TaskCode,
		WindowStart: time.Date(2026, 4, 11, 9, 0, 0, 0, time.Local),
		WindowEnd:   time.Date(2026, 4, 11, 10, 0, 0, 0, time.Local),
	}

	ds := newBlockingProductionDataSource(6)
	reg := NewRegistry()
	reg.RegisterDataSource(cfg.TaskCode, ds)
	bigRepo := &recordingProduceBigRepo{}
	engine := NewEngine(reg, nil, nil, bigRepo, nil, &noopBackupStore{})

	errCh := make(chan error, 1)
	go func() {
		errCh <- engine.produceBig(ctx, cfg, inst)
	}()

	if !ds.waitForMaxConcurrent(3, time.Second) {
		ds.releaseAll()
		t.Fatalf("expected 3 concurrent fetches, got max %d", ds.maxConcurrent())
	}
	ds.releaseAll()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("produceBig returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("produceBig did not finish")
	}

	if got := bigRepo.createdIndices(); !equalInts(got, []int{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("expected ordered batch creation [1 2 3 4 5 6], got %v", got)
	}
	if bigRepo.updatedTotalBatches != 6 || bigRepo.updatedTotalRecords != 60 {
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

type blockingProductionDataSource struct {
	total   int
	entered chan struct{}
	release chan struct{}

	mu       sync.Mutex
	inFlight int
	max      int
	once     sync.Once
}

func newBlockingProductionDataSource(total int) *blockingProductionDataSource {
	return &blockingProductionDataSource{
		total:   total,
		entered: make(chan struct{}, total),
		release: make(chan struct{}),
	}
}

func (d *blockingProductionDataSource) CountChunks(context.Context, TaskConfig, time.Time, time.Time) (int, error) {
	return d.total, nil
}

func (d *blockingProductionDataSource) FetchChunk(context.Context, TaskConfig, time.Time, time.Time, int) (Chunk, error) {
	d.mu.Lock()
	d.inFlight++
	if d.inFlight > d.max {
		d.max = d.inFlight
	}
	d.mu.Unlock()

	d.entered <- struct{}{}
	<-d.release

	d.mu.Lock()
	d.inFlight--
	d.mu.Unlock()
	return Chunk{Data: []byte("payload"), RecordCount: 10}, nil
}

func (d *blockingProductionDataSource) waitForMaxConcurrent(target int, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if d.maxConcurrent() >= target {
			return true
		}
		select {
		case <-d.entered:
		case <-timer.C:
			return false
		}
	}
}

func (d *blockingProductionDataSource) maxConcurrent() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.max
}

func (d *blockingProductionDataSource) releaseAll() {
	d.once.Do(func() { close(d.release) })
}

type recordingProduceBigRepo struct {
	idempotentBigRepo

	mu                  sync.Mutex
	batches             []BigTaskBatch
	updatedTotalBatches int
	updatedTotalRecords int
}

func (r *recordingProduceBigRepo) CountBatches(context.Context, int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.batches), nil
}

func (r *recordingProduceBigRepo) CreateBatch(_ context.Context, batch BigTaskBatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, batch)
	return nil
}

func (r *recordingProduceBigRepo) SumBatchRecords(context.Context, int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, batch := range r.batches {
		total += batch.RecordCount
	}
	return total, nil
}

func (r *recordingProduceBigRepo) UpdateProducedMeta(_ context.Context, _ int64, totalBatches, totalRecords int) error {
	r.updatedTotalBatches = totalBatches
	r.updatedTotalRecords = totalRecords
	return nil
}

func (r *recordingProduceBigRepo) createdIndices() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, 0, len(r.batches))
	for _, batch := range r.batches {
		out = append(out, batch.BatchIndex)
	}
	return out
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
