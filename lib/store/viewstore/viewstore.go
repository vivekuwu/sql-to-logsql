package viewstore

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode"
)

type ViewStore struct {
	dir string
	mu  sync.RWMutex
}

type ViewOptions struct {
	OrReplace   bool
	IfNotExists bool
}

func NewViewStore(dir string) (*ViewStore, error) {
	cleaned := strings.TrimSpace(dir)
	if cleaned == "" {
		return nil, nil
	}
	if strings.Contains(cleaned, "\x00") {
		return nil, fmt.Errorf("viewstore: invalid views directory")
	}
	return &ViewStore{dir: filepath.Clean(cleaned)}, nil
}

func sanitizeViewFileName(parts []string) (string, string, error) {
	if len(parts) == 0 {
		return "", "", &StoreError{
			Code:    http.StatusBadRequest,
			Message: "viewstore: CREATE VIEW missing name",
		}
	}
	sanitized := make([]string, len(parts))
	for i, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			return "", "", &StoreError{
				Code:    http.StatusBadRequest,
				Message: "viewstore: view name contains empty part",
			}
		}
		for _, r := range trimmed {
			if !isSafeViewRune(r) {
				return "", "", &StoreError{
					Code:    http.StatusBadRequest,
					Message: fmt.Sprintf("viewstore: invalid character %q in view name %q", r, trimmed),
				}
			}
		}
		sanitized[i] = strings.ToLower(trimmed)
	}
	return strings.Join(sanitized, "_"), strings.Join(parts, "."), nil
}

func isSafeViewRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-'
}

func (s *ViewStore) Save(parts []string, query string, opts ViewOptions) (string, error) {
	if s == nil {
		return "", fmt.Errorf("viewstore: CREATE VIEW requires configured views directory")
	}
	fileName, displayName, err := sanitizeViewFileName(parts)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return "", fmt.Errorf("viewstore: ensure views directory: %w", err)
	}
	lockPath := filepath.Join(s.dir, fileName+".lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", &StoreError{
				Code:    http.StatusLocked,
				Message: fmt.Sprintf("viewstore: view %s is locked", displayName),
				Err:     err,
			}
		}
		return "", &StoreError{
			Code:    http.StatusLocked,
			Message: fmt.Sprintf("viewstore: create lock for view %s: %v", displayName, err),
			Err:     err,
		}
	}
	defer func() {
		if err := lockFile.Close(); err != nil {
			fmt.Printf("WARNING: failed to close lock file %s: %v\n", lockPath, err)
		}
		if err := os.Remove(lockPath); err != nil {
			fmt.Printf("WARNING: failed to Remove lock file %s: %v\n", lockPath, err)
		}
	}()

	viewPath := filepath.Join(s.dir, fileName+".logsql")
	info, statErr := os.Stat(viewPath)
	if statErr == nil {
		if info.IsDir() {
			return "", fmt.Errorf("viewstore: expected file for view %s but found directory", displayName)
		}
		if opts.IfNotExists {
			return viewPath, nil
		}
		if !opts.OrReplace {
			return "", &StoreError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("viewstore: view %s already exists", displayName),
			}
		}
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return "", fmt.Errorf("viewstore: stat view %s: %w", displayName, statErr)
	}
	existingView := statErr == nil

	tmpFile, err := os.CreateTemp(s.dir, fileName+"-*.tmp")
	if err != nil {
		return "", fmt.Errorf("viewstore: create temp file for view %s: %w", displayName, err)
	}
	tmpName := tmpFile.Name()
	cleanupTmp := func() {
		tmpFile.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmpFile.WriteString(query); err != nil {
		cleanupTmp()
		return "", fmt.Errorf("viewstore: write view %s: %w", displayName, err)
	}
	if !strings.HasSuffix(query, "\n") {
		if _, err := tmpFile.WriteString("\n"); err != nil {
			cleanupTmp()
			return "", fmt.Errorf("viewstore: finalize view %s: %w", displayName, err)
		}
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("viewstore: flush view %s: %w", displayName, err)
	}
	if existingView {
		if err := os.Remove(viewPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			_ = os.Remove(tmpName)
			return "", fmt.Errorf("viewstore: Remove existing view %s: %w", displayName, err)
		}
	}
	if err := os.Rename(tmpName, viewPath); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("viewstore: replace view %s: %w", displayName, err)
	}
	return viewPath, nil
}

func (s *ViewStore) Load(parts []string) (string, string, bool, error) {
	if s == nil {
		return "", strings.Join(parts, "."), false, fmt.Errorf("viewstore: SELECT from view requires configured views directory")
	}
	fileName, displayName, err := sanitizeViewFileName(parts)
	if err != nil {
		return "", displayName, false, err
	}
	viewPath := filepath.Join(s.dir, fileName+".logsql")
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, statErr := os.Stat(viewPath)
	if statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			return "", displayName, false, nil
		}
		return "", displayName, false, fmt.Errorf("viewstore: stat view %s: %w", displayName, statErr)
	}
	if info.IsDir() {
		return "", displayName, false, fmt.Errorf("viewstore: expected file for view %s but found directory", displayName)
	}
	data, err := os.ReadFile(viewPath)
	if err != nil {
		return "", displayName, false, fmt.Errorf("viewstore: read view %s: %w", displayName, err)
	}
	query := strings.TrimRight(string(data), "\r\n")
	if strings.TrimSpace(query) == "" {
		return "", displayName, false, fmt.Errorf("viewstore: view %s is empty", displayName)
	}
	return query, displayName, true, nil
}

func (s *ViewStore) ListViews() ([]string, error) {
	if s == nil {
		return nil, fmt.Errorf("viewstore: SHOW VIEWS requires configured views directory")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("viewstore: list views: %w", err)
	}
	views := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".logsql") {
			continue
		}
		base := strings.TrimSuffix(name, ".logsql")
		if base == "" {
			continue
		}
		views = append(views, base)
	}
	sort.Strings(views)
	return views, nil
}

// ViewDefinitions returns a map of view file base names to their stored queries.
func (s *ViewStore) ViewDefinitions() (map[string]string, error) {
	if s == nil {
		return nil, fmt.Errorf("viewstore: SHOW VIEWS requires configured views directory")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("viewstore: list views: %w", err)
	}
	defs := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".logsql") {
			continue
		}
		base := strings.TrimSuffix(name, ".logsql")
		if base == "" {
			continue
		}
		viewPath := filepath.Join(s.dir, name)
		data, err := os.ReadFile(viewPath)
		if err != nil {
			return nil, fmt.Errorf("viewstore: read view %s: %w", base, err)
		}
		query := strings.TrimRight(string(data), "\r\n")
		if strings.TrimSpace(query) == "" {
			return nil, fmt.Errorf("viewstore: view %s is empty", base)
		}
		defs[base] = query
	}
	return defs, nil
}

func (s *ViewStore) Remove(parts []string, ifExists bool) error {
	if s == nil {
		return fmt.Errorf("viewstore: DROP VIEW requires configured views directory")
	}
	fileName, displayName, err := sanitizeViewFileName(parts)
	if err != nil {
		return err
	}
	lockPath := filepath.Join(s.dir, fileName+".lock")
	s.mu.Lock()
	defer s.mu.Unlock()
	lockFile, lockErr := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if lockErr != nil {
		if errors.Is(lockErr, fs.ErrExist) {
			return fmt.Errorf("viewstore: view %s is locked", displayName)
		}
		if errors.Is(lockErr, fs.ErrNotExist) {
			if ifExists {
				return nil
			}
			return fmt.Errorf("viewstore: view %s does not exist", displayName)
		}
		return fmt.Errorf("viewstore: create lock for view %s: %w", displayName, lockErr)
	}
	defer func() {
		if err := lockFile.Close(); err != nil {
			fmt.Printf("WARNING: failed to close lock file %s: %v\n", lockPath, err)
		}
		if err := os.Remove(lockPath); err != nil {
			fmt.Printf("WARNING: failed to Remove lock file %s: %v\n", lockPath, err)
		}
	}()

	viewPath := filepath.Join(s.dir, fileName+".logsql")
	info, statErr := os.Stat(viewPath)
	if statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			if ifExists {
				return nil
			}
			return fmt.Errorf("viewstore: view %s does not exist", displayName)
		}
		return fmt.Errorf("viewstore: stat view %s: %w", displayName, statErr)
	}
	if info.IsDir() {
		return fmt.Errorf("viewstore: expected file for view %s but found directory", displayName)
	}
	if err := os.Remove(viewPath); err != nil {
		return fmt.Errorf("viewstore: Remove view %s: %w", displayName, err)
	}
	return nil
}
