package reliableupload

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFSBackupStore_SaveAndRead_RoundTrip(t *testing.T) {
	root := t.TempDir()
	store := NewFSBackupStore(root)
	ctx := context.Background()

	backupPath, err := store.Save(ctx, "task_a", "file.dat", []byte("payload"))
	if err != nil {
		t.Fatalf("save failed: %v", err)
	}

	data, err := store.Read(ctx, backupPath)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("unexpected data: %s", string(data))
	}
}

func TestFSBackupStore_Save_RejectsTraversalFileName(t *testing.T) {
	root := t.TempDir()
	store := NewFSBackupStore(root)
	ctx := context.Background()

	_, err := store.Save(ctx, "task_a", "..\\evil.dat", []byte("payload"))
	if err == nil {
		t.Fatalf("expected traversal filename to be rejected")
	}
	if !strings.Contains(err.Error(), "invalid file name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFSBackupStore_Read_RejectsPathOutsideRoot(t *testing.T) {
	root := t.TempDir()
	store := NewFSBackupStore(root)
	ctx := context.Background()

	outsideDir := t.TempDir()
	outsideFile := filepath.Join(outsideDir, "outside.dat")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o644); err != nil {
		t.Fatalf("prepare outside file failed: %v", err)
	}

	_, err := store.Read(ctx, outsideFile)
	if err == nil {
		t.Fatalf("expected outside-root read to be rejected")
	}
	if !strings.Contains(err.Error(), "outside backup root") {
		t.Fatalf("unexpected error: %v", err)
	}
}
