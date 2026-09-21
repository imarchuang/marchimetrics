package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// Manifest is the per-partition list of live part names — the analog of
// VictoriaMetrics' parts.json. It is the source of truth for which part
// directories are visible to queries; part dirs not listed here (e.g.
// after a crash between rename and manifest save) are ignored orphans.
type Manifest struct {
	Parts []string `json:"parts"`
}

func loadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return &Manifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("cannot parse manifest %q: %w", path, err)
	}
	return &m, nil
}

// save atomically persists the manifest (temp file + rename).
func (m *Manifest) save(path string) error {
	return writeJSON(path, m)
}
