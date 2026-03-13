package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"smart-upload/reliableupload"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type uploadLogModel struct {
	ID         int64     `gorm:"column:id;primaryKey;autoIncrement"`
	TaskCode   string    `gorm:"column:task_code;type:varchar(64);not null;index:idx_scan,priority:1"`
	TimeStart  time.Time `gorm:"column:time_start;not null;index:idx_scan,priority:3"`
	TimeEnd    time.Time `gorm:"column:time_end;not null"`
	FileName   string    `gorm:"column:file_name;type:varchar(255);not null;uniqueIndex:uk_file_name"`
	BizKey     string    `gorm:"column:biz_key;type:varchar(128)"`
	MetaJSON   string    `gorm:"column:meta_json;type:text"`
	Status     uint8     `gorm:"column:status;not null;default:0;index:idx_scan,priority:2"`
	BackupPath string    `gorm:"column:backup_path;type:varchar(512)"`
	RetryCount int       `gorm:"column:retry_count;not null;default:0"`
	ErrMsg     string    `gorm:"column:err_msg;type:varchar(1024)"`
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt  time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (uploadLogModel) TableName() string { return "uploadlog" }

type bigTaskInstanceModel struct {
	ID              int64      `gorm:"column:id;primaryKey;autoIncrement"`
	TaskCode        string     `gorm:"column:task_code;type:varchar(64);not null;uniqueIndex:uk_task_window,priority:1;index:idx_status,priority:1"`
	WindowStart     time.Time  `gorm:"column:window_start;not null;uniqueIndex:uk_task_window,priority:2"`
	WindowEnd       time.Time  `gorm:"column:window_end;not null;uniqueIndex:uk_task_window,priority:3"`
	Status          uint8      `gorm:"column:status;not null;default:0;index:idx_status,priority:2"`
	TotalBatches    int        `gorm:"column:total_batches"`
	UploadedBatches int        `gorm:"column:uploaded_batches;not null;default:0"`
	TotalRecords    int        `gorm:"column:total_records"`
	StartedAt       time.Time  `gorm:"column:started_at"`
	FinishedAt      *time.Time `gorm:"column:finished_at"`
}

func (bigTaskInstanceModel) TableName() string { return "big_task_instance" }

type bigTaskBatchModel struct {
	ID          int64     `gorm:"column:id;primaryKey;autoIncrement"`
	InstanceID  int64     `gorm:"column:instance_id;not null;uniqueIndex:uk_instance_batch,priority:1;index:idx_instance_status,priority:1"`
	BatchIndex  int       `gorm:"column:batch_index;not null;uniqueIndex:uk_instance_batch,priority:2;index:idx_instance_status,priority:3"`
	FileName    string    `gorm:"column:file_name;type:varchar(255);not null;uniqueIndex:uk_file_name"`
	RecordCount int       `gorm:"column:record_count;not null;default:0"`
	BizKey      string    `gorm:"column:biz_key;type:varchar(128)"`
	MetaJSON    string    `gorm:"column:meta_json;type:text"`
	BackupPath  string    `gorm:"column:backup_path;type:varchar(512)"`
	Status      uint8     `gorm:"column:status;not null;default:0;index:idx_instance_status,priority:2"`
	RetryCount  int       `gorm:"column:retry_count;not null;default:0"`
	ErrMsg      string    `gorm:"column:err_msg;type:varchar(1024)"`
	CreatedAt   time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt   time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (bigTaskBatchModel) TableName() string { return "big_task_batch" }

func openMySQL(dsn string) (*gorm.DB, error) {
	return gorm.Open(mysql.Open(dsn), &gorm.Config{})
}

func ensureMySQLDatabase(dsn string) error {
	adminDSN, dbName, err := splitMySQLDSN(dsn)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", adminDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", dbName)
	_, err = db.Exec(query)
	return err
}

func splitMySQLDSN(dsn string) (adminDSN, dbName string, err error) {
	idxSlash := strings.LastIndex(dsn, "/")
	if idxSlash <= 0 || idxSlash == len(dsn)-1 {
		return "", "", fmt.Errorf("invalid mysql dsn: %s", dsn)
	}
	rest := dsn[idxSlash+1:]
	idxQ := strings.Index(rest, "?")
	if idxQ >= 0 {
		dbName = rest[:idxQ]
		adminDSN = dsn[:idxSlash+1] + "mysql?" + rest[idxQ+1:]
	} else {
		dbName = rest
		adminDSN = dsn[:idxSlash+1] + "mysql"
	}
	if dbName == "" {
		return "", "", fmt.Errorf("invalid mysql dsn db name: %s", dsn)
	}
	return adminDSN, dbName, nil
}

func initMySQLSchema(db *gorm.DB) error {
	return db.AutoMigrate(&uploadLogModel{}, &bigTaskInstanceModel{}, &bigTaskBatchModel{})
}

type mysqlUploadLogRepo struct {
	db *gorm.DB
}

func newMySQLUploadLogRepo(db *gorm.DB) *mysqlUploadLogRepo {
	return &mysqlUploadLogRepo{db: db}
}

func (r *mysqlUploadLogRepo) ExistsByTaskAndTimeRange(ctx context.Context, taskCode string, start, end time.Time) (bool, error) {
	var cnt int64
	err := r.db.WithContext(ctx).
		Model(&uploadLogModel{}).
		Where("task_code = ? AND time_start = ? AND time_end = ?", taskCode, start, end).
		Count(&cnt).Error
	return cnt > 0, err
}

func (r *mysqlUploadLogRepo) Create(ctx context.Context, log reliableupload.UploadLog) error {
	m := uploadLogModel{
		TaskCode:   log.TaskCode,
		TimeStart:  log.TimeStart,
		TimeEnd:    log.TimeEnd,
		FileName:   log.FileName,
		BizKey:     log.BizKey,
		MetaJSON:   log.MetaJSON,
		Status:     uint8(log.Status),
		BackupPath: log.BackupPath,
		RetryCount: log.RetryCount,
		ErrMsg:     log.ErrMsg,
		CreatedAt:  log.CreatedAt,
		UpdatedAt:  log.UpdatedAt,
	}
	return r.db.WithContext(ctx).Create(&m).Error
}

func (r *mysqlUploadLogRepo) FindDistinctPendingTaskCodes(ctx context.Context) ([]string, error) {
	var codes []string
	err := r.db.WithContext(ctx).
		Model(&uploadLogModel{}).
		Where("status = ?", uint8(reliableupload.StatusPending)).
		Distinct("task_code").
		Order("task_code ASC").
		Pluck("task_code", &codes).Error
	return codes, err
}

func (r *mysqlUploadLogRepo) FindPendingByCode(ctx context.Context, taskCode string, maxRetry, limit int) ([]reliableupload.UploadLog, error) {
	var rows []uploadLogModel
	err := r.db.WithContext(ctx).
		Model(&uploadLogModel{}).
		Where("task_code = ? AND status = ? AND retry_count <= ?", taskCode, uint8(reliableupload.StatusPending), maxRetry).
		Order("time_start ASC, id ASC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]reliableupload.UploadLog, 0, len(rows))
	for _, row := range rows {
		out = append(out, reliableupload.UploadLog{
			ID:         row.ID,
			TaskCode:   row.TaskCode,
			TimeStart:  row.TimeStart,
			TimeEnd:    row.TimeEnd,
			FileName:   row.FileName,
			BizKey:     row.BizKey,
			MetaJSON:   row.MetaJSON,
			Status:     reliableupload.Status(row.Status),
			BackupPath: row.BackupPath,
			RetryCount: row.RetryCount,
			ErrMsg:     row.ErrMsg,
			CreatedAt:  row.CreatedAt,
			UpdatedAt:  row.UpdatedAt,
		})
	}
	return out, nil
}

func (r *mysqlUploadLogRepo) MarkUploaded(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).
		Model(&uploadLogModel{}).
		Where("id = ?", id).
		Updates(map[string]any{"status": uint8(reliableupload.StatusUploaded), "updated_at": time.Now()}).
		Error
}

func (r *mysqlUploadLogRepo) IncrRetry(ctx context.Context, id int64, errMsg string) error {
	return r.db.WithContext(ctx).
		Model(&uploadLogModel{}).
		Where("id = ?", id).
		Updates(map[string]any{"retry_count": gorm.Expr("retry_count + 1"), "err_msg": errMsg, "updated_at": time.Now()}).
		Error
}

func (r *mysqlUploadLogRepo) GetLastTimeEndByCode(ctx context.Context, taskCode string) (time.Time, bool, error) {
	var row uploadLogModel
	err := r.db.WithContext(ctx).
		Model(&uploadLogModel{}).
		Where("task_code = ?", taskCode).
		Order("time_end DESC").
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return row.TimeEnd, true, nil
}

type mysqlBigRepo struct {
	db *gorm.DB
}

func newMySQLBigRepo(db *gorm.DB) *mysqlBigRepo {
	return &mysqlBigRepo{db: db}
}

func (r *mysqlBigRepo) GetOrCreateInstance(ctx context.Context, taskCode string, windowStart, windowEnd time.Time) (reliableupload.BigTaskInstance, error) {
	inst := bigTaskInstanceModel{
		TaskCode:    taskCode,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		Status:      uint8(reliableupload.StatusRunning),
		StartedAt:   time.Now(),
	}
	err := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "task_code"}, {Name: "window_start"}, {Name: "window_end"}}, DoNothing: true}).
		Create(&inst).Error
	if err != nil {
		return reliableupload.BigTaskInstance{}, err
	}

	var got bigTaskInstanceModel
	err = r.db.WithContext(ctx).
		Where("task_code = ? AND window_start = ? AND window_end = ?", taskCode, windowStart, windowEnd).
		First(&got).Error
	if err != nil {
		return reliableupload.BigTaskInstance{}, err
	}
	return toBigInstance(got), nil
}

func (r *mysqlBigRepo) UpdateProducedMeta(ctx context.Context, instanceID int64, totalBatches, totalRecords int) error {
	return r.db.WithContext(ctx).
		Model(&bigTaskInstanceModel{}).
		Where("id = ?", instanceID).
		Updates(map[string]any{"total_batches": totalBatches, "total_records": totalRecords}).
		Error
}

func (r *mysqlBigRepo) CreateBatch(ctx context.Context, batch reliableupload.BigTaskBatch) error {
	m := bigTaskBatchModel{
		InstanceID:  batch.InstanceID,
		BatchIndex:  batch.BatchIndex,
		FileName:    batch.FileName,
		RecordCount: batch.RecordCount,
		BizKey:      batch.BizKey,
		MetaJSON:    batch.MetaJSON,
		BackupPath:  batch.BackupPath,
		Status:      uint8(batch.Status),
		RetryCount:  batch.RetryCount,
		ErrMsg:      batch.ErrMsg,
		CreatedAt:   batch.CreatedAt,
		UpdatedAt:   batch.UpdatedAt,
	}
	return r.db.WithContext(ctx).Create(&m).Error
}

func (r *mysqlBigRepo) FindRunningInstances(ctx context.Context) ([]reliableupload.BigTaskInstance, error) {
	var rows []bigTaskInstanceModel
	err := r.db.WithContext(ctx).
		Where("status = ?", uint8(reliableupload.StatusRunning)).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]reliableupload.BigTaskInstance, 0, len(rows))
	for _, row := range rows {
		out = append(out, toBigInstance(row))
	}
	return out, nil
}

func (r *mysqlBigRepo) CountBatches(ctx context.Context, instanceID int64) (int, error) {
	var cnt int64
	err := r.db.WithContext(ctx).
		Model(&bigTaskBatchModel{}).
		Where("instance_id = ?", instanceID).
		Count(&cnt).Error
	return int(cnt), err
}

func (r *mysqlBigRepo) SumBatchRecords(ctx context.Context, instanceID int64) (int, error) {
	var total sql.NullInt64
	err := r.db.WithContext(ctx).
		Model(&bigTaskBatchModel{}).
		Where("instance_id = ?", instanceID).
		Select("COALESCE(SUM(record_count), 0)").
		Scan(&total).Error
	if err != nil {
		return 0, err
	}
	if !total.Valid {
		return 0, nil
	}
	return int(total.Int64), nil
}

func (r *mysqlBigRepo) FindPendingBatches(ctx context.Context, instanceID int64, maxRetry, limit int) ([]reliableupload.BigTaskBatch, error) {
	var rows []bigTaskBatchModel
	err := r.db.WithContext(ctx).
		Model(&bigTaskBatchModel{}).
		Where("instance_id = ? AND status = ? AND retry_count <= ?", instanceID, uint8(reliableupload.StatusPending), maxRetry).
		Order("batch_index ASC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]reliableupload.BigTaskBatch, 0, len(rows))
	for _, row := range rows {
		out = append(out, reliableupload.BigTaskBatch{
			ID:          row.ID,
			InstanceID:  row.InstanceID,
			BatchIndex:  row.BatchIndex,
			FileName:    row.FileName,
			RecordCount: row.RecordCount,
			BizKey:      row.BizKey,
			MetaJSON:    row.MetaJSON,
			BackupPath:  row.BackupPath,
			Status:      reliableupload.Status(row.Status),
			RetryCount:  row.RetryCount,
			ErrMsg:      row.ErrMsg,
			CreatedAt:   row.CreatedAt,
			UpdatedAt:   row.UpdatedAt,
		})
	}
	return out, nil
}

func (r *mysqlBigRepo) MarkBatchUploaded(ctx context.Context, batchID int64) error {
	return r.db.WithContext(ctx).
		Model(&bigTaskBatchModel{}).
		Where("id = ?", batchID).
		Updates(map[string]any{"status": uint8(reliableupload.StatusUploaded), "updated_at": time.Now()}).
		Error
}

func (r *mysqlBigRepo) IncrBatchRetry(ctx context.Context, batchID int64, errMsg string) error {
	return r.db.WithContext(ctx).
		Model(&bigTaskBatchModel{}).
		Where("id = ?", batchID).
		Updates(map[string]any{"retry_count": gorm.Expr("retry_count + 1"), "err_msg": errMsg, "updated_at": time.Now()}).
		Error
}

func (r *mysqlBigRepo) CountUploadedBatches(ctx context.Context, instanceID int64) (int, error) {
	var cnt int64
	err := r.db.WithContext(ctx).
		Model(&bigTaskBatchModel{}).
		Where("instance_id = ? AND status = ?", instanceID, uint8(reliableupload.StatusUploaded)).
		Count(&cnt).Error
	return int(cnt), err
}

func (r *mysqlBigRepo) MarkInstanceCompleted(ctx context.Context, instanceID int64, finishedAt time.Time) error {
	uploaded, err := r.CountUploadedBatches(ctx, instanceID)
	if err != nil {
		return err
	}
	return r.db.WithContext(ctx).
		Model(&bigTaskInstanceModel{}).
		Where("id = ?", instanceID).
		Updates(map[string]any{
			"status":           uint8(reliableupload.StatusUploaded),
			"uploaded_batches": uploaded,
			"finished_at":      finishedAt,
		}).Error
}

func toBigInstance(m bigTaskInstanceModel) reliableupload.BigTaskInstance {
	return reliableupload.BigTaskInstance{
		ID:              m.ID,
		TaskCode:        m.TaskCode,
		WindowStart:     m.WindowStart,
		WindowEnd:       m.WindowEnd,
		Status:          reliableupload.Status(m.Status),
		TotalBatches:    m.TotalBatches,
		UploadedBatches: m.UploadedBatches,
		TotalRecords:    m.TotalRecords,
		StartedAt:       m.StartedAt,
		FinishedAt:      m.FinishedAt,
	}
}
