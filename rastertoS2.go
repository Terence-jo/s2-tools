package main

import (
	"sync"

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
// My map of slices implementation was servicable, but felt unwieldy.
// slices of this will work a bit better, but still has its challenges.
type S2CellData struct {
	cell s2.CellID
	data float64
}

type S2Table map[s2.CellID][]float64

type MapFunction func(...float64) float64

func RasterToS2(path string) (S2Table, error) {
	godal.RegisterAll()

	// TODO: make aggFunc cmd-configurable
	aggFunc := func(inData ...float64) float64 {
		var sum float64
		for _, val := range inData {
			sum += val
		}
		return sum / float64(len(inData))
	}

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
	bandWithInfo := BandWithInfo{band, origin, xRes, yRes}
	s2Data, err := indexBand(bandWithInfo, aggFunc)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	return s2Data, nil
}

func indexBand(bandWithInfo BandWithInfo, aggFunc MapFunction) (S2Table, error) {
	// Set up a done channel that can signal time to close for the whole pipeline.
	// Done will close when the pipeline exits, signalling that it is time to abandon
	// upstream stages. Experiment with and without this.
	done := make(chan struct{})
	defer close(done)
	var wg sync.WaitGroup

	// Asynchronous generation of blocks to be consumed.
	blocks := genBlocks(&bandWithInfo, done)
	// Parallel processing of each block produced above.
	resChan, idChan := processBlocks(&bandWithInfo, blocks, aggFunc, &wg)
	// Need to wait for all blocks to finish here to compute unique IDs.
	s2Results := <-resChan
	s2IDs := <-idChan
	wg.Wait()

	uniqueIDs := Unique(s2IDs)
	aggResults := aggToS2Cell(uniqueIDs, s2Results, aggFunc, &wg)
	return aggResults, nil
}

// Produce blocks from a raster band, putting them in a channel to be consumed
// downstream. The production here is happening serially, but there would be
// very little speedup from parallelizing at this step.
func genBlocks(band *BandWithInfo, done <-chan struct{}) <-chan godal.Block {
	out := make(chan godal.Block)
	firstBlock := band.Band.Structure().FirstBlock()
	go func() {
		defer close(out)
		for block, ok := firstBlock, true; ok; block, ok = block.Next() {
			select {
			case out <- block:
			case <-done:
				return
			}
		}
	}()
	return out
}

func processBlocks(
	band *BandWithInfo,
	blocks <-chan godal.Block,
	aggFunc MapFunction,
	wg *sync.WaitGroup,
) (<-chan S2CellData, chan s2.CellID) {

	results := make(chan S2CellData)
	ids := make(chan s2.CellID)

	// TODO: Figure out how to implement a worker pool pattern here.
	// This could blow-out big time with big rasters.
	for block := range blocks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logrus.Infof("Processing block at %v, %v", block.X0, block.Y0)
			newData, err := rasterBlockToS2(band, block, aggFunc)
			if err != nil {
				logrus.Error(err)
			}
			for _, data := range newData {
				results <- data
			}
		}()
	}
	return results, ids
}

func rasterBlockToS2(band *BandWithInfo, block godal.Block, aggFunc MapFunction) ([]S2CellData, error) {
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

	var s2Data []S2CellData

	for pix := 0; pix < block.W*block.H; pix++ {
		value := blockBuf[pix]
		// GDAL is row-major
		row := pix / block.W
		col := pix % block.W

		lat := blockOrigin.Lat + float64(row)*band.YRes
		lng := blockOrigin.Lng + float64(col)*band.XRes

		latLng := s2.LatLngFromDegrees(lat, lng)
		s2Cell := s2.CellIDFromLatLng(latLng).Parent(s2Lvl)
		cellData := S2CellData{s2Cell, value}
		s2Data = append(s2Data, cellData)
	}
	// TODO: Consider aggregating to cell level here
	return s2Data, nil
}

// This doesn't serve a purpose anymore. I need a different function
// to aggregate across unique IDs.
func aggWithinCells(s2Data S2Table, fun MapFunction) map[s2.CellID]float64 {
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
