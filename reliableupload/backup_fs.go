package reliableupload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	if err := validatePathSegment(taskCode, "task code"); err != nil {
		return "", err
	}
	if err := validatePathSegment(fileName, "file name"); err != nil {
		return "", err
	}
	rootAbs, err := filepath.Abs(filepath.Clean(s.RootDir))
	if err != nil {
		return "", err
	}
	dayDir := time.Now().Format("2006-01-02")
	dir := filepath.Join(rootAbs, taskCode, dayDir)
	if err := ensureWithinRoot(rootAbs, dir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	filePath := filepath.Join(dir, fileName)
	if err := ensureWithinRoot(rootAbs, filePath); err != nil {
		return "", err
	}
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return "", err
	}
	return filePath, nil
}

func (s *FSBackupStore) Read(_ context.Context, backupPath string) ([]byte, error) {
	rootAbs, err := filepath.Abs(filepath.Clean(s.RootDir))
	if err != nil {
		return nil, err
	}
	targetAbs, err := filepath.Abs(filepath.Clean(backupPath))
	if err != nil {
		return nil, err
	}
	if err := ensureWithinRoot(rootAbs, targetAbs); err != nil {
		return nil, err
	}
	return os.ReadFile(targetAbs)
}

func validatePathSegment(segment, field string) error {
	trimmed := strings.TrimSpace(segment)
	if trimmed == "" {
		return fmt.Errorf("invalid %s: empty", field)
	}
	if trimmed == "." || trimmed == ".." {
		return fmt.Errorf("invalid %s: %s", field, segment)
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") {
		return fmt.Errorf("invalid %s: %s", field, segment)
	}
	if filepath.Base(trimmed) != trimmed {
		return fmt.Errorf("invalid %s: %s", field, segment)
	}
	return nil
}

func ensureWithinRoot(rootAbs, targetPath string) error {
	targetAbs, err := filepath.Abs(filepath.Clean(targetPath))
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootAbs, targetAbs)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path outside backup root: %s", targetPath)
	}
	return nil
}
