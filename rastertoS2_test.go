package main

import (
	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
	"os"
	"reflect"
	"testing"
)

func TestPointToS2(t *testing.T) {
	// Create a point
	latLng := s2.LatLngFromDegrees(1.0, 2.0)

	// Create a S2 point
	s2Cell := s2.CellIDFromLatLng(latLng).Parent(11)

	// Compare the two
	desiredCell := s2.CellID(1154732675135700992)
	if s2Cell != desiredCell {
		t.Errorf("S2 cells are not equal, got %v, want %v", s2Cell, desiredCell)
	}
}

func TestRasterBlockToS2(t *testing.T) {
	ds := setUpRaster(t)
	defer ds.Close()
	origin, xRes, yRes, err := getOriginAndResolution(ds)
	if err != nil {
		t.Fatal(err)
	}

	band := BandWithInfo{
		Band:   &ds.Bands()[0],
		Origin: origin,
		XRes:   xRes,
		YRes:   yRes,
	}
	s2Data, err := RasterBlockToS2(band, band.Band.Structure().FirstBlock())
	if err != nil {
		t.Fatal(err)
	}

	want := map[s2.CellID][]float64{
		s2.CellID(1152921779484753920): {1.0},
		s2.CellID(1153105397926592512): {2.0},
		s2.CellID(1921714053521080320): {3.0},
		s2.CellID(1921892174404780032): {4.0},
	}

	// Compare the two
	if !reflect.DeepEqual(s2Data, want) {
		t.Errorf("")
	}
}

// func TestAggregations(t *testing.T) {
// 	// With a faux-S2 table, assert that different aggregation
// 	// functions produce the desired result.
// 	s2Data := map[s2.CellID][]float64{
// 		s2.CellID(1152921779484753920): {1.0, 1.0},
// 		s2.CellID(1153105397926592512): {2.0, 3.0, 4.0},
// 		s2.CellID(1921714053521080320): {3.0, 3.0, 3.0},
// 		s2.CellID(1921892174404780032): {4.0, 10.0},
// 	}
// 	s2Table := S2Table{s2Data}
//
// 	aggResults := []struct {
// 		name string
// 		got  map[s2.CellID]float64
// 		want map[s2.CellID]float64
// 	}{
// 		{
// 			name: "mean",
// 			got:  s2Table.Mean(),
// 			want: map[s2.CellID]float64{
// 				s2.CellID(1152921779484753920): 1.0,
// 				s2.CellID(1153105397926592512): 3.0,
// 				s2.CellID(1921714053521080320): 3.0,
// 				s2.CellID(1921892174404780032): 7.0,
// 			},
// 		},
// 		{
// 			name: "sum",
// 			got:  s2Table.Sum(),
// 			want: map[s2.CellID]float64{
// 				s2.CellID(1152921779484753920): 2.0,
// 				s2.CellID(1153105397926592512): 9.0,
// 				s2.CellID(1921714053521080320): 9.0,
// 				s2.CellID(1921892174404780032): 14.0,
// 			},
// 		},
// 		{
// 			name: "count",
// 			got:  s2Table.Count(),
// 			want: map[s2.CellID]float64{
// 				s2.CellID(1152921779484753920): 2.0,
// 				s2.CellID(1153105397926592512): 3.0,
// 				s2.CellID(1921714053521080320): 3.0,
// 				s2.CellID(1921892174404780032): 2.0,
// 			},
// 		},
// 		{
// 			name: "majority",
// 			got:  s2Table.Majority(),
// 			want: map[s2.CellID]float64{
// 				s2.CellID(1152921779484753920): 1.0,
// 				s2.CellID(1153105397926592512): 2.0,
// 				s2.CellID(1921714053521080320): 3.0,
// 				s2.CellID(1921892174404780032): 4.0}},
// 	}
//
// 	for _, tt := range aggResults {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if !reflect.DeepEqual(tt.got, tt.want) {
// 				t.Errorf("got %#v, want %#v", tt.got, tt.want)
// 			}
// 		})
// 	}
//
// }

func setUpRaster(t testing.TB) *godal.Dataset {
	godal.RegisterAll()
	t.Helper()

	tmpFile, _ := os.CreateTemp("", "")
	if err := tmpFile.Close(); err != nil {
		t.Fatal(err)
	}
	dsFile := tmpFile.Name()
	defer os.Remove(dsFile)

	// Create a raster
	ds, _ := godal.Create(
		godal.GTiff,
		dsFile,
		1,
		godal.Byte,
		2,
		2,
		godal.CreationOption("TILED=YES", "BLOCKXSIZE=16", "BLOCKYSIZE=16"),
	)
	if err := ds.SetGeoTransform([6]float64{0.0, 1.0, 0.0, 0.0, 0.0, -1.0}); err != nil {
		t.Fatal(err)
	}

	// fill band with random data
	buf := []byte{1, 2, 3, 4}
	bands := ds.Bands()

	if err := bands[0].Write(0, 0, buf, 2, 2); err != nil {
		t.Fatal(err)
	}
	return ds
}
