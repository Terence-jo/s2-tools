package celltools

import (
	"fmt"
	"math"

	"github.com/airbusgeo/godal"

	"github.com/golang/geo/s2"
)

func cellToWKT(cell s2.Cell) string {
	wkt := "POLYGON(("
	for k := 0; k < 4; k++ {
		latlng := s2.LatLngFromPoint(cell.Vertex(k))
		wkt += fmt.Sprintf("%v %v, ", latlng.Lng.Degrees(), latlng.Lat.Degrees())
	}
	closingPoint := s2.LatLngFromPoint(cell.Vertex(0))
	wkt += fmt.Sprintf("%v %v))", closingPoint.Lng.Degrees(), closingPoint.Lat.Degrees())

	return wkt
}

func wgs84GeomFromString(wkt string) (*godal.Geometry, error) {
	srs, err := godal.NewSpatialRefFromEPSG(4326)
	if err != nil {
		return nil, err
	}
	geom, err := godal.NewGeometryFromWKT(wkt, srs)
	if err != nil {
		return nil, err
	}
	return geom, nil
}

func getUTMSpatialRef(lng float64, lat float64) (*godal.SpatialRef, error) {
	utm := int(math.Ceil((lng + 180) / 6))
	var utmSRS *godal.SpatialRef
	var err error
	if lat >= 0 {
		utmSRS, err = godal.NewSpatialRefFromEPSG(32600 + utm)
	} else {
		utmSRS, err = godal.NewSpatialRefFromEPSG(32700 + utm)
	}
	return utmSRS, err
}
