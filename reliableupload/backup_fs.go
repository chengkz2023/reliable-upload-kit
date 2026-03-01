package reliableupload

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

// FSBackupStore stores backup files on local filesystem.
type FSBackupStore struct {
	RootDir string
}

func NewFSBackupStore(rootDir string) *FSBackupStore {
	return &FSBackupStore{RootDir: rootDir}
}

func (s *FSBackupStore) Save(_ context.Context, taskCode, fileName string, data []byte) (string, error) {
	dayDir := time.Now().Format("2006-01-02")
	dir := filepath.Join(s.RootDir, taskCode, dayDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	filePath := filepath.Join(dir, fileName)
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return "", err
	}
	return filePath, nil
}

func (s *FSBackupStore) Read(_ context.Context, backupPath string) ([]byte, error) {
	return os.ReadFile(backupPath)
}
