# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
go test ./...

# Run tests in a specific package
go test ./reliableupload/...

# Run a single test by name
go test ./reliableupload/... -run TestUploadPendingBigBatches_PaginatesUntilDrained

# Run the example (requires MySQL or override DSN)
go run ./example

# Tidy dependencies
go mod tidy
```

## Architecture

This is a Go library (`smart-upload`) for reliable, production-grade upload reporting. All core logic lives in the `reliableupload` package. The `example/` directory shows MySQL/GORM integration but is not part of the library.

### Three task types

| Type | `TaskType` | Trigger | Window |
|------|-----------|---------|--------|
| Minute task | `TaskTypeMinute=1` | Cron A (producer) + Cron B (uploader) | Auto-computed minute interval |
| Big task | `TaskTypeBig=2` | `RunBigTask(ctx, code, start, end)` | Caller-specified time range |
| Biz task | `TaskTypeBiz=3` | `RunBizTask(ctx, code, triggerKey, payload)` | No time window; keyed by `trigger_key` |

### Data flow

**Production phase** (Cron A / explicit call):
1. `DataSource.CountChunks` → determines how many chunks exist
2. `DataSource.FetchChunk` (per index) → returns `Chunk{Data, RecordCount, BizKey, Meta}`
3. `BackupStore.Save` → persists bytes to disk (or custom store); returns `backupPath`
4. Repo creates a log/batch record with `status=pending` pointing to `backupPath`

**Upload phase** (Cron B / explicit call):
1. Repo loads pending records
2. `BackupStore.Read(backupPath)` → retrieves bytes (backup is the single source of truth for retries)
3. `UploadHooks.BeforeUpload` → optional context/item mutation
4. `Reporter.Upload` → performs actual upload
5. `UploadHooks.AfterUpload` / `OnUploadError` → post-upload hooks
6. Repo marks record uploaded or increments retry count

### Key types

- **`Engine`** (`engine.go`) — orchestrates all flows; constructed via `NewEngine`
- **`Registry`** (`registry.go`) — maps `task_code` → `DataSource`, `Reporter`, `FileNamer` (all per-task)
- **`BackupStore`** — interface; `FSBackupStore` (`backup_fs.go`) is the built-in filesystem implementation
- **`UploadHooks`** / `UploadHookFuncs` — optional before/after/error callbacks around each upload
- **`UploadFailureStrategy`** — `UploadFailureFailFast` (default) stops on first error; `UploadFailureContinue` processes all batches and aggregates errors
- **`FileNamer`** — per-task or engine-level file naming; default pattern: `{prefix}_{start}_{end}_{index:03d}.dat`
- **`Clock`** — injected for deterministic tests (`WithClock`)

### Engine construction

```go
reg := reliableupload.NewRegistry()
reg.RegisterDataSource("my_task", myDS)
reg.RegisterReporter("my_task", myReporter)
// optional: reg.RegisterFileNamer("my_task", myNamer)

engine := reliableupload.NewEngine(
    reg, cfgRepo, logRepo, bigRepo, bizRepo, backup,
    reliableupload.WithLoggerFuncs(zap.Infof, zap.Errorf),
    reliableupload.WithUploadFailureStrategy(reliableupload.UploadFailureContinue),
    reliableupload.WithUploadHooks(myHooks),
)
```

`bigRepo` and `bizRepo` may be `nil` if those task types are unused.

### Biz task context

Inside `DataSource` implementations for `TaskTypeBiz`, read the trigger key/payload via:
```go
trigger, ok := reliableupload.BizTriggerFromContext(ctx)
```

`trigger_key` must be stable and unique per business event (e.g. approval ID, batch number). Build a unique index on `(task_code, trigger_key)` in the biz instance table for idempotency.

### Startup recovery

Call `engine.OnStartup(ctx)` at process start. It:
- Backfills any minute-task gaps since the last recorded window
- Resumes interrupted `running` big/biz task instances

### Testing patterns

Tests in `reliableupload/` use internal fakes (`fakeBigRepo`, `fakeBizRepo`, `fakeBackupStore`, `fakeReporter`) defined in `upload_test_helpers_test.go`. Add new fakes there rather than creating separate files. Use `WithClock` and `WithPendingLimit` options to make engine behavior deterministic in tests.
