package vfs

import (
	"context"
	"strconv"
	"strings"
	"sync"
)

// MemorySheets is an in-memory SheetsAPI. Tests and local harnesses use it
// instead of the Google service.
type MemorySheets struct {
	mu    sync.Mutex
	snaps map[string]SheetsSnapshot
}

// NewMemorySheets returns an empty in-memory workbook store.
func NewMemorySheets() *MemorySheets {
	return &MemorySheets{snaps: make(map[string]SheetsSnapshot)}
}

// Seed replaces the snapshot for id (deep-copied).
func (m *MemorySheets) Seed(id string, snap SheetsSnapshot) {
	if snap.SpreadsheetID == "" {
		snap.SpreadsheetID = id
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snaps[id] = cloneSheetsSnapshot(snap)
}

func (m *MemorySheets) Get(ctx context.Context, spreadsheetID string) (SheetsSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return SheetsSnapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.snaps[spreadsheetID]
	if !ok {
		s = SheetsSnapshot{
			SpreadsheetID: spreadsheetID,
			Sheets:        []Sheet{{ID: "0", Title: "Sheet1"}},
		}
		m.snaps[spreadsheetID] = s
	}
	return cloneSheetsSnapshot(s), nil
}

func (m *MemorySheets) BatchUpdate(ctx context.Context, spreadsheetID string, req SheetsBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.loaded(spreadsheetID)
	applyMemoryFormats(&s, req)
	m.snaps[spreadsheetID] = s
	return nil
}

func (m *MemorySheets) BatchUpdateValues(ctx context.Context, spreadsheetID string, req SheetsValuesBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.loaded(spreadsheetID)
	applyMemoryValues(&s, req)
	m.snaps[spreadsheetID] = s
	return nil
}

func (m *MemorySheets) loaded(id string) SheetsSnapshot {
	s, ok := m.snaps[id]
	if !ok {
		return SheetsSnapshot{SpreadsheetID: id, Sheets: []Sheet{{ID: "0", Title: "Sheet1"}}}
	}
	return s
}

func cloneSheetsSnapshot(s SheetsSnapshot) SheetsSnapshot {
	out := s
	out.Sheets = make([]Sheet, len(s.Sheets))
	for i, sh := range s.Sheets {
		out.Sheets[i] = cloneSheet(sh)
	}
	out.Named = append([]NamedRange(nil), s.Named...)
	return out
}

func applyMemoryValues(s *SheetsSnapshot, req SheetsValuesBatch) {
	for _, vr := range req.Data {
		title, a1 := SplitSheetAddr(vr.Range)
		idx := findMemorySheet(s.Sheets, title)
		if idx < 0 {
			s.Sheets = append(s.Sheets, Sheet{Title: title})
			idx = len(s.Sheets) - 1
		}
		r1, c1 := 1, 1
		if a1 != "" {
			if i := strings.Index(a1, ":"); i >= 0 {
				a1 = a1[:i]
			}
			if rr, cc, _, _, err := parseA1(a1); err == nil {
				r1, c1 = rr, cc
			}
		}
		sh := s.Sheets[idx]
		needR := r1 - 1 + len(vr.Values)
		needC := c1 - 1
		for _, row := range vr.Values {
			if c1-1+len(row) > needC {
				needC = c1 - 1 + len(row)
			}
		}
		for len(sh.Cells) < needR {
			sh.Cells = append(sh.Cells, nil)
		}
		for r, row := range vr.Values {
			rr := r1 - 1 + r
			for len(sh.Cells[rr]) < needC {
				sh.Cells[rr] = append(sh.Cells[rr], Cell{})
			}
			for c, val := range row {
				cell := sh.Cells[rr][c1-1+c]
				cell.Input = val
				if !strings.HasPrefix(val, "=") {
					cell.Value = val
				}
				sh.Cells[rr][c1-1+c] = cell
			}
		}
		sh.Rows = len(sh.Cells)
		sh.Cols = 0
		for _, row := range sh.Cells {
			if len(row) > sh.Cols {
				sh.Cols = len(row)
			}
		}
		s.Sheets[idx] = sh
	}
}

func applyMemoryFormats(s *SheetsSnapshot, req SheetsBatch) {
	for _, rc := range req.Requests {
		idx := findMemorySheet(s.Sheets, strconv.FormatInt(rc.SheetID, 10))
		if idx < 0 {
			continue
		}
		sh := s.Sheets[idx]
		needR, needC := rc.EndRow, rc.EndCol
		for len(sh.Cells) < needR {
			sh.Cells = append(sh.Cells, nil)
		}
		for r := rc.StartRow; r < rc.EndRow; r++ {
			for len(sh.Cells[r]) < needC {
				sh.Cells[r] = append(sh.Cells[r], Cell{})
			}
			for c := rc.StartCol; c < rc.EndCol; c++ {
				cell := sh.Cells[r][c]
				cell.Format.Overlay(rc.Format)
				sh.Cells[r][c] = cell
			}
		}
		if len(sh.Cells) > sh.Rows {
			sh.Rows = len(sh.Cells)
		}
		s.Sheets[idx] = sh
	}
}

func findMemorySheet(sheets []Sheet, key string) int {
	for i, sh := range sheets {
		if sh.Title == key || sh.ID == key {
			return i
		}
	}
	if key == "" && len(sheets) == 1 {
		return 0
	}
	return -1
}
