package main

import (
	"errors"
	"sync"

	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
	"github.com/sirupsen/logrus"
)

// TODO: Make level configurable on command line
const s2Lvl int = 11
const numWorkers = 8

type Point struct {
	Lat float64
	Lng float64
}

type BandWithTransform struct {
	Band   *godal.Band
	Origin Point
	XRes   float64
	YRes   float64
}

// S2CellData TODO Investigate further possibilities for data structures here.
// My map of slices implementation was serviceable, but felt unwieldy.
// slices of this will work a bit better, but still has its challenges.
type S2CellData struct {
	cell s2.CellID
	data float64
}

type ReduceFunction func(...float64) float64

func RasterToS2(path string) ([]S2CellData, error) {
	godal.RegisterAll()

	// TODO: make aggFunc cmd-line-configurable
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
	defer func() {
		err = errors.Join(err, ds.Close())
	}()

	origin, xRes, yRes, err := getOriginAndResolution(ds)
	if err != nil {
		return nil, err
	}

	band := &ds.Bands()[0]
	bandWithInfo := BandWithTransform{band, origin, xRes, yRes}

	s2Data, err := indexBand(bandWithInfo, aggFunc)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	return s2Data, nil
}

func indexBand(bandWithInfo BandWithTransform, aggFunc ReduceFunction) ([]S2CellData, error) {
	// Set up a done channel that can signal time to close for the whole pipeline.
	// Done will close when the pipeline exits, signalling that it is time to abandon
	// upstream stages. Experiment with and without this.
	done := make(chan struct{})
	defer close(done)
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Asynchronous generation of blocks to be consumed.
	blocks := genBlocks(&bandWithInfo, done)
	// Parallel processing of each block produced above.
	resCh, idCh := processBlocks(&bandWithInfo, blocks, done)

	// Because it just uses the seen map to deduplicate, this doesn't need the full set of
	// cells to be passed in. It can just consume the channel as it comes in.
	uniqueIDCh := Unique(idCh)

	var aggResults []S2CellData
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for cellID := range uniqueIDCh {
				aggResults = append(aggResults, aggToS2Cell(cellID, resCh, aggFunc))
			}
		}()
	}

	wg.Wait()
	return aggResults, nil
}

// Produce blocks from a raster band, putting them in a channel to be consumed
// downstream. The production here is happening serially, but there would be
// very little speedup from parallelising at this step.
func genBlocks(band *BandWithTransform, done <-chan struct{}) <-chan godal.Block {
	logrus.Debug("Entered genBlocks")

	blocks := make(chan godal.Block)
	firstBlock := band.Band.Structure().FirstBlock()
	go func() {
		for block, ok := firstBlock, true; ok; block, ok = block.Next() {
			select {
			case blocks <- block:
			case <-done:
				return
			}
		}
		close(blocks)
	}()
	logrus.Debug("Exited genBlocks")
	return blocks
}

// TODO: Think about implementing the done signal pattern here. Also, consider
// whether aggFunc needs to be passed down to block level. Check performance
// with and without.
func processBlocks(band *BandWithTransform, blocks <-chan godal.Block, done <-chan struct{}) (chan S2CellData, <-chan s2.CellID) {
	// TODO: Put processBlocks in here, and call it above after using this closure
	logrus.Debug("Entered blockProcessor")
	resCh := make(chan S2CellData, 100)
	idCh := make(chan s2.CellID, 100)
	var wg sync.WaitGroup

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go indexBlocks(band, blocks, resCh, idCh, done, &wg)
	}

	go func() {
		wg.Wait()
		close(resCh)
		close(idCh)
	}()

	logrus.Debug("Exited blockProcessor")
	return resCh, idCh
}

// TODO: Tidy this function signature. This is too many params.
func indexBlocks(band *BandWithTransform, blocks <-chan godal.Block, resCh chan<- S2CellData, idCh chan<- s2.CellID, done <-chan struct{}, wg *sync.WaitGroup) {
	logrus.Debug("Entered indexBlocks")
	var blocksData []S2CellData
	for block := range blocks {
		logrus.Infof("Processing block at [%v, %v]", block.X0, block.Y0)
		newData, err := rasterBlockToS2(band, block)
		if err != nil {
			return
		}
		blocksData = append(blocksData, newData...)
	}
	// Consider just passing these channels into rasterBlockToS2, avoiding the extra range
	go func() {
		for _, data := range blocksData {
			select {
			case idCh <- data.cell:
			case <-done:
				return
			}
			select {
			case resCh <- data:
			case <-done:
				return
			}
		}
		wg.Done()
		logrus.Debug("Exited channel write")
	}()
	logrus.Debug("Exited indexBlocks")
}

func rasterBlockToS2(band *BandWithTransform, block godal.Block) ([]S2CellData, error) {
	logrus.Debug("Entered rasterBlockToS2")
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
	// TODO: Consider aggregating to cell level here, but check performance.
	logrus.Debug("Exited rasterBlockToS2")
	return s2Data, nil
}

func aggToS2Cell(cellID s2.CellID, resCh chan S2CellData, aggFunc ReduceFunction) S2CellData {
	// TODO: Verify the validity of this solution (thanks copilot). It seems reasonable overall, but a little weird in places.
	logrus.Debug("Entered aggToS2Cell")
	var values []float64

	go func() {
		for s2Cell := range resCh {
			if s2Cell.cell == cellID {
				values = append(values, s2Cell.data)
			} else {
				resCh <- s2Cell
			}
		}
	}()

	logrus.Debug("Exited aggToS2Cell")
	return S2CellData{cellID, aggFunc(values...)}
}

func Unique(in <-chan s2.CellID) <-chan s2.CellID {
	logrus.Debug("Entered Unique")
	out := make(chan s2.CellID, 100)
	go func() {
		defer close(out)
		seen := make(map[s2.CellID]struct{})
		for cell := range in {
			if _, ok := seen[cell]; !ok {
				seen[cell] = struct{}{}
				out <- cell
			}
		}
	}()
	logrus.Debug("Exited Unique")
	return out
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
