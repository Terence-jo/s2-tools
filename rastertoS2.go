package main

import (
	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
	"github.com/sirupsen/logrus"
)

// TODO: Make level configurable on command line
const s2Lvl int = 11

type Point struct {
	Lat float64
	Lng float64
}

type BandWithInfo struct {
	Band   *godal.Band
	Origin Point
	XRes   float64
	YRes   float64
}

// TODO Investigate further possibilities for data structures here.
// My map of slices implementation is servicable, but feels unwieldy.
type S2CellData struct {
	cell s2.CellID
	data []float64
}
type S2Table map[s2.CellID][]float64

type MapFunction func(...float64) float64

func RasterToS2(path string) (S2Table, error) {
	godal.RegisterAll()

	ds, err := godal.Open(path)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	defer ds.Close()

	origin, xRes, yRes, err := getOriginAndResolution(ds)
	if err != nil {
		return nil, err
	}

	band := &ds.Bands()[0]
	firstBlock := band.Structure().FirstBlock()
	bandWithInfo := BandWithInfo{band, origin, xRes, yRes}
	s2Data, err := IndexBand(bandWithInfo, firstBlock)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	return s2Data, nil
}

func IndexBand(bandWithInfo BandWithInfo, firstBlock godal.Block) (S2Table, error) {
	s2Data := make(map[s2.CellID][]float64)
	for rasterBlock, ok := firstBlock, true; ok; rasterBlock, ok = rasterBlock.Next() {
		//TODO: Iterate through blocks with a goroutine. For now just loop.
		logrus.Infof("Processing block at %v, %v", rasterBlock.X0, rasterBlock.Y0)
		newData, err := RasterBlockToS2(bandWithInfo, rasterBlock)
		if err != nil {
			logrus.Error(err)
			return nil, err
		}
		for cell, values := range newData {
			s2Data = appendToS2Map(s2Data, cell, values...)
		}
	}
	return s2Data, nil
}

func RasterBlockToS2(band BandWithInfo, block godal.Block) (map[s2.CellID][]float64, error) {
	blockOrigin, err := blockOrigin(block, band.XRes, band.Origin)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	blockBuf := make([]float64, block.H*block.W)

	// Read band into blockBuf
	if err := band.Band.Read(block.X0, block.Y0, blockBuf, block.W, block.H); err != nil {
		logrus.Error(err)
		return nil, err
	}

	s2Data := make(map[s2.CellID][]float64)

	for pix := 0; pix < block.W*block.H; pix++ {
		value := blockBuf[pix]
		// GDAL is row-major
		row := pix / block.W
		col := pix % block.W

		lat := blockOrigin.Lat + float64(row)*band.YRes
		lng := blockOrigin.Lng + float64(col)*band.XRes

		latLng := s2.LatLngFromDegrees(lat, lng)
		s2Cell := s2.CellIDFromLatLng(latLng).Parent(s2Lvl)
		s2Data = appendToS2Map(s2Data, s2Cell, value)
	}
	return s2Data, nil
}

// TODO: Make this a concurrent pipeline stage
func AggWithinCells(s2Data S2Table, fun MapFunction) map[s2.CellID]float64 {
	out := make(map[s2.CellID]float64, len(s2Data))
	for cell, data := range s2Data {
		out[cell] = fun(data...)
	}
	return out
}

func appendToS2Map(s2Data S2Table, s2Cell s2.CellID, values ...float64) S2Table {
	oldData, ok := s2Data[s2Cell]
	if !ok {
		s2Data[s2Cell] = values
	} else {
		s2Data[s2Cell] = append(oldData, values...)
	}
	return s2Data
}

func getOriginAndResolution(ds *godal.Dataset) (Point, float64, float64, error) {
	gt, err := ds.GeoTransform()
	if err != nil {
		logrus.Error(err)
		return Point{}, 0, 0, err
	}
	origin := Point{gt[3], gt[0]}
	xRes := gt[1]
	yRes := gt[5]
	return origin, xRes, yRes, nil
}

func blockOrigin(rasterBlock godal.Block, resolution float64, origin Point) (Point, error) {
	originLng := float64(rasterBlock.X0)*resolution + origin.Lng
	originLat := float64(rasterBlock.Y0)*resolution + origin.Lat
	return Point{originLat, originLng}, nil
}
