package celltools

import (
	"fmt"

	"github.com/golang/geo/s2"
)

func cellToWKT(cell s2.Cell) (string, error) {
	wkt := "POLYGON(("
	for k := 0; k < 4; k++ {
		latlng := s2.LatLngFromPoint(cell.Vertex(k))
		wkt += fmt.Sprintf("%s %s, ", latlng.Lng, latlng.Lat)
	}
	closingPoint := s2.LatLngFromPoint(cell.Vertex(0))
	wkt += fmt.Sprintf("%s, %s))", closingPoint.Lng, closingPoint.Lat)

	return wkt, nil
}
