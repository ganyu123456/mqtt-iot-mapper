package util

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// EnvOrDefault returns the value of the environment variable key, or fallback if unset.
func EnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvIntOrDefault returns the integer value of key, or fallback if unset or invalid.
func EnvIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// FileLogger is a persistent logger with file size rotation.
// It is safe for concurrent use across goroutines and packages.
type FileLogger struct {
	path       string
	maxSize    int64 // bytes
	maxBackups int

	mu          sync.Mutex
	file        *os.File
	currentSize int64
}

// FileLoggerConfig holds configuration for a FileLogger.
type FileLoggerConfig struct {
	Path       string // full path to the log file
	MaxSizeMB  int    // max file size in MB before rotation
	MaxBackups int    // max number of rotated files to keep
}

// NewFileLogger creates a FileLogger. It creates the parent directory
// if needed and opens (or creates) the log file for append.
func NewFileLogger(cfg FileLoggerConfig) (*FileLogger, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("log file path is empty")
	}
	if cfg.MaxSizeMB <= 0 {
		cfg.MaxSizeMB = 10
	}
	if cfg.MaxBackups <= 0 {
		cfg.MaxBackups = 10
	}

	dir := filepath.Dir(cfg.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir %s: %w", dir, err)
	}

	f, err := os.OpenFile(cfg.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", cfg.Path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &FileLogger{
		path:        cfg.Path,
		maxSize:     int64(cfg.MaxSizeMB) * 1024 * 1024,
		maxBackups:  cfg.MaxBackups,
		file:        f,
		currentSize: fi.Size(),
	}, nil
}

// Write writes a formatted line to the log file. Each call appends one line
// prefixed with a UTC timestamp. If the file exceeds maxSize the current file
// is rotated before the write.
func (l *FileLogger) Write(format string, args ...interface{}) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	line := fmt.Sprintf("%s %s\n",
		time.Now().UTC().Format("2006-01-02T15:04:05.000"),
		fmt.Sprintf(format, args...))

	lineBytes := int64(len(line))

	if l.currentSize+lineBytes > l.maxSize {
		if err := l.rotate(); err != nil {
			return err
		}
	}

	n, err := l.file.WriteString(line)
	if err != nil {
		return err
	}
	l.currentSize += int64(n)
	return l.file.Sync()
}

// Close flushes and closes the underlying file.
func (l *FileLogger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		l.file.Sync()
		l.file.Close()
		l.file = nil
	}
}

// rotate renames the current log file to a timestamped backup and opens a new one.
func (l *FileLogger) rotate() error {
	if l.file != nil {
		l.file.Sync()
		l.file.Close()
		l.file = nil
	}

	ext := filepath.Ext(l.path)
	base := strings.TrimSuffix(l.path, ext)
	backup := fmt.Sprintf("%s-%s%s", base, time.Now().UTC().Format("20060102T150405"), ext)

	if err := os.Rename(l.path, backup); err != nil {
		return fmt.Errorf("rename log for rotation: %w", err)
	}

	// Reopen the original path (now a fresh file).
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("reopen log after rotation: %w", err)
	}

	l.file = f
	l.currentSize = 0

	// Purge old backups beyond maxBackups.
	l.purgeBackups(base, ext)
	return nil
}

func (l *FileLogger) purgeBackups(base, ext string) {
	dir := filepath.Dir(l.path)
	prefix := filepath.Base(base)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	var backups []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, prefix+"-") && strings.HasSuffix(name, ext) {
			backups = append(backups, name)
		}
	}

	if len(backups) <= l.maxBackups {
		return
	}

	sort.Strings(backups)
	toDelete := backups[:len(backups)-l.maxBackups]
	for _, name := range toDelete {
		_ = os.Remove(filepath.Join(dir, name))
	}
}
