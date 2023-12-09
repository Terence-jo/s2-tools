package celltools

import (
	"fmt"
	"github.com/golang/geo/s2"
	"testing"
)

func TestCellToWKT(t *testing.T) {
	cell := s2.CellFromCellID(s2.CellID(uint64(1152921779484753920)))
	wktString, err := cellToWKT(cell)
	if err != nil {
		t.Fatal(err)
	}
	want := "POLYGON((0.0 0.0, 0.0373201 0.0, 0.0373201 0.0373201, 0.0373201 0.0, 0.0 0.0))"

	if wktString != want {
		t.Errorf("got %s, want %s", wktString, want)
	}
}
