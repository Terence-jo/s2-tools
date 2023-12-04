package rastertoS2

import (
	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
	"os"
	"reflect"
	"sync"
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
	defer func() {
		if err := ds.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	origin, xRes, yRes, err := getOriginAndResolution(ds)
	if err != nil {
		t.Fatal(err)
	}

	band := BandContainer{
		Band:   &ds.Bands()[0],
		Origin: origin,
		XRes:   xRes,
		YRes:   yRes,
		mu:     sync.Mutex{},
	}
	dataCh := make(chan S2CellData)
	go func() {
		err = rasterBlockToS2(&band, band.Band.Structure().FirstBlock(), dataCh)
	}()
	if err != nil {
		t.Fatal(err)
	}

	var s2Data []S2CellData
	for data := range dataCh {
		s2Data = append(s2Data, data)
	}

	want := []S2CellData{
		{s2.CellID(1152921779484753920), 1.0},
		{s2.CellID(1153105397926592512), 2.0},
		{s2.CellID(1921714053521080320), 3.0},
		{s2.CellID(1921892174404780032), 4.0},
	}

	// Compare the two
	if !reflect.DeepEqual(dataCh, want) {
		t.Errorf("got %v, want %v", dataCh, want)
	}
}

func setUpRaster(t testing.TB) *godal.Dataset {
	godal.RegisterAll()
	t.Helper()

	tmpFile, _ := os.CreateTemp("", "")
	if err := tmpFile.Close(); err != nil {
		t.Fatal(err)
	}
	dsFile := tmpFile.Name()
	defer func() {
		err := os.Remove(dsFile)
		if err != nil {
			t.Fatal(err)
		}
	}()

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
