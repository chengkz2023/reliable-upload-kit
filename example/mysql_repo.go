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
	Status     uint8     `gorm:"column:status;not null;default:0;index:idx_scan,priority:2"`
	BackupPath string    `gorm:"column:backup_path;type:varchar(512)"`
	RetryCount int       `gorm:"column:retry_count;not null;default:0"`
	ErrMsg     string    `gorm:"column:err_msg;type:varchar(1024)"`
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt  time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (uploadLogModel) TableName() string { return "uploadlog" }

type dailyTaskInstanceModel struct {
	ID              int64      `gorm:"column:id;primaryKey;autoIncrement"`
	TaskCode        string     `gorm:"column:task_code;type:varchar(64);not null;uniqueIndex:uk_task,priority:1;index:idx_status,priority:1"`
	TaskDate        time.Time  `gorm:"column:task_date;type:date;not null;uniqueIndex:uk_task,priority:2"`
	Status          uint8      `gorm:"column:status;not null;default:0;index:idx_status,priority:2"`
	TotalBatches    int        `gorm:"column:total_batches"`
	UploadedBatches int        `gorm:"column:uploaded_batches;not null;default:0"`
	TotalRecords    int        `gorm:"column:total_records"`
	StartedAt       time.Time  `gorm:"column:started_at"`
	FinishedAt      *time.Time `gorm:"column:finished_at"`
}

func (dailyTaskInstanceModel) TableName() string { return "daily_task_instance" }

type dailyTaskBatchModel struct {
	ID         int64     `gorm:"column:id;primaryKey;autoIncrement"`
	InstanceID int64     `gorm:"column:instance_id;not null;uniqueIndex:uk_instance_batch,priority:1;index:idx_instance_status,priority:1"`
	BatchIndex int       `gorm:"column:batch_index;not null;uniqueIndex:uk_instance_batch,priority:2;index:idx_instance_status,priority:3"`
	FileName   string    `gorm:"column:file_name;type:varchar(255);not null;uniqueIndex:uk_file_name"`
	BackupPath string    `gorm:"column:backup_path;type:varchar(512)"`
	Status     uint8     `gorm:"column:status;not null;default:0;index:idx_instance_status,priority:2"`
	RetryCount int       `gorm:"column:retry_count;not null;default:0"`
	ErrMsg     string    `gorm:"column:err_msg;type:varchar(1024)"`
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt  time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (dailyTaskBatchModel) TableName() string { return "daily_task_batch" }

func openMySQL(dsn string) (*gorm.DB, error) {
	return gorm.Open(mysql.Open(dsn), &gorm.Config{})
}

func ensureMySQLDatabase(dsn string) error {
	// 用 admin DSN 连接 mysql 系统库，确保业务库存在。
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
	// 初始化或迁移三张核心状态表。
	return db.AutoMigrate(&uploadLogModel{}, &dailyTaskInstanceModel{}, &dailyTaskBatchModel{})
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

type mysqlDailyRepo struct {
	db *gorm.DB
}

func newMySQLDailyRepo(db *gorm.DB) *mysqlDailyRepo {
	return &mysqlDailyRepo{db: db}
}

func (r *mysqlDailyRepo) GetOrCreateInstance(ctx context.Context, taskCode string, taskDate time.Time) (reliableupload.DailyTaskInstance, error) {
	d := time.Date(taskDate.Year(), taskDate.Month(), taskDate.Day(), 0, 0, 0, 0, taskDate.Location())
	inst := dailyTaskInstanceModel{
		TaskCode:  taskCode,
		TaskDate:  d,
		Status:    uint8(reliableupload.StatusRunning),
		StartedAt: time.Now(),
	}
	// 幂等创建: (task_code, task_date) 已存在则忽略插入。
	err := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "task_code"}, {Name: "task_date"}}, DoNothing: true}).
		Create(&inst).Error
	if err != nil {
		return reliableupload.DailyTaskInstance{}, err
	}

	var got dailyTaskInstanceModel
	err = r.db.WithContext(ctx).
		Where("task_code = ? AND task_date = ?", taskCode, d).
		First(&got).Error
	if err != nil {
		return reliableupload.DailyTaskInstance{}, err
	}
	return toDailyInstance(got), nil
}

func (r *mysqlDailyRepo) UpdateProducedMeta(ctx context.Context, instanceID int64, totalBatches, totalRecords int) error {
	return r.db.WithContext(ctx).
		Model(&dailyTaskInstanceModel{}).
		Where("id = ?", instanceID).
		Updates(map[string]any{"total_batches": totalBatches, "total_records": totalRecords}).
		Error
}

func (r *mysqlDailyRepo) CreateBatch(ctx context.Context, batch reliableupload.DailyTaskBatch) error {
	m := dailyTaskBatchModel{
		InstanceID: batch.InstanceID,
		BatchIndex: batch.BatchIndex,
		FileName:   batch.FileName,
		BackupPath: batch.BackupPath,
		Status:     uint8(batch.Status),
		RetryCount: batch.RetryCount,
		ErrMsg:     batch.ErrMsg,
		CreatedAt:  batch.CreatedAt,
		UpdatedAt:  batch.UpdatedAt,
	}
	return r.db.WithContext(ctx).Create(&m).Error
}

func (r *mysqlDailyRepo) FindRunningInstances(ctx context.Context) ([]reliableupload.DailyTaskInstance, error) {
	var rows []dailyTaskInstanceModel
	err := r.db.WithContext(ctx).
		Where("status = ?", uint8(reliableupload.StatusRunning)).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]reliableupload.DailyTaskInstance, 0, len(rows))
	for _, row := range rows {
		out = append(out, toDailyInstance(row))
	}
	return out, nil
}

func (r *mysqlDailyRepo) CountBatches(ctx context.Context, instanceID int64) (int, error) {
	var cnt int64
	err := r.db.WithContext(ctx).
		Model(&dailyTaskBatchModel{}).
		Where("instance_id = ?", instanceID).
		Count(&cnt).Error
	return int(cnt), err
}

func (r *mysqlDailyRepo) FindPendingBatches(ctx context.Context, instanceID int64, maxRetry, limit int) ([]reliableupload.DailyTaskBatch, error) {
	var rows []dailyTaskBatchModel
	err := r.db.WithContext(ctx).
		Model(&dailyTaskBatchModel{}).
		Where("instance_id = ? AND status = ? AND retry_count <= ?", instanceID, uint8(reliableupload.StatusPending), maxRetry).
		Order("batch_index ASC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]reliableupload.DailyTaskBatch, 0, len(rows))
	for _, row := range rows {
		out = append(out, reliableupload.DailyTaskBatch{
			ID:         row.ID,
			InstanceID: row.InstanceID,
			BatchIndex: row.BatchIndex,
			FileName:   row.FileName,
			BackupPath: row.BackupPath,
			Status:     reliableupload.Status(row.Status),
			RetryCount: row.RetryCount,
			ErrMsg:     row.ErrMsg,
			CreatedAt:  row.CreatedAt,
			UpdatedAt:  row.UpdatedAt,
		})
	}
	return out, nil
}

func (r *mysqlDailyRepo) MarkBatchUploaded(ctx context.Context, batchID int64) error {
	return r.db.WithContext(ctx).
		Model(&dailyTaskBatchModel{}).
		Where("id = ?", batchID).
		Updates(map[string]any{"status": uint8(reliableupload.StatusUploaded), "updated_at": time.Now()}).
		Error
}

func (r *mysqlDailyRepo) IncrBatchRetry(ctx context.Context, batchID int64, errMsg string) error {
	return r.db.WithContext(ctx).
		Model(&dailyTaskBatchModel{}).
		Where("id = ?", batchID).
		Updates(map[string]any{"retry_count": gorm.Expr("retry_count + 1"), "err_msg": errMsg, "updated_at": time.Now()}).
		Error
}

func (r *mysqlDailyRepo) CountUploadedBatches(ctx context.Context, instanceID int64) (int, error) {
	var cnt int64
	err := r.db.WithContext(ctx).
		Model(&dailyTaskBatchModel{}).
		Where("instance_id = ? AND status = ?", instanceID, uint8(reliableupload.StatusUploaded)).
		Count(&cnt).Error
	return int(cnt), err
}

func (r *mysqlDailyRepo) MarkInstanceCompleted(ctx context.Context, instanceID int64, finishedAt time.Time) error {
	uploaded, err := r.CountUploadedBatches(ctx, instanceID)
	if err != nil {
		return err
	}
	return r.db.WithContext(ctx).
		Model(&dailyTaskInstanceModel{}).
		Where("id = ?", instanceID).
		Updates(map[string]any{
			"status":           uint8(reliableupload.StatusUploaded),
			"uploaded_batches": uploaded,
			"finished_at":      finishedAt,
		}).Error
}

func toDailyInstance(m dailyTaskInstanceModel) reliableupload.DailyTaskInstance {
	return reliableupload.DailyTaskInstance{
		ID:              m.ID,
		TaskCode:        m.TaskCode,
		TaskDate:        m.TaskDate,
		Status:          reliableupload.Status(m.Status),
		TotalBatches:    m.TotalBatches,
		UploadedBatches: m.UploadedBatches,
		TotalRecords:    m.TotalRecords,
		StartedAt:       m.StartedAt,
		FinishedAt:      m.FinishedAt,
	}
}
