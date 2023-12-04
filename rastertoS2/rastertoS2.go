package rastertoS2

import (
	"errors"
	"fmt"
	"sync"

	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
	"github.com/sirupsen/logrus"
)

type Point struct {
	Lat float64
	Lng float64
}

type BandContainer struct {
	Band   *godal.Band
	Origin Point
	XRes   float64
	YRes   float64
	mu     sync.Mutex
}

type S2CellData struct {
	cell s2.CellID
	data float64
}

func (c S2CellData) String() string {
	return fmt.Sprintf("%v,%v", int64(c.cell), c.data)
}

type AggFunc func(...float64) float64

var numWorkers int
var s2Lvl int

func RasterToS2(path string, aggFunc AggFunc, workers int, cellLevel int) ([]S2CellData, error) {
	godal.RegisterAll()
	// Bad pattern, this is lazy. Figure out something better
	numWorkers = workers
	s2Lvl = cellLevel

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
	bandWithInfo := BandContainer{band, origin, xRes, yRes, sync.Mutex{}}

	s2Data, err := indexBand(&bandWithInfo, aggFunc)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	return s2Data, nil
}

func indexBand(bandWithInfo *BandContainer, aggFunc AggFunc) ([]S2CellData, error) {
	// Set up a done channel that can signal time to close for the whole pipeline.
	// Done will close when the pipeline exits, signalling that it is time to abandon
	// upstream stages. Experiment with and without this.
	done := make(chan struct{})
	defer close(done)
	// var wg sync.WaitGroup // this WaitGroup is redundant. Might still want to figure out a robust way to ensure completion though.

	// Asynchronous generation of blocks to be consumed.
	blocks := genBlocks(bandWithInfo, done)
	// Parallel processing of each block produced above.
	resCh := processBlocks(bandWithInfo, blocks)

	// Because it just uses the seen map to deduplicate, this doesn't need the full set of
	// cells to be passed in. It can just consume the channel as it comes in.
	resMap := groupByCell(resCh)

	// TODO: Figure out how to parallelize the aggregation step. This is the bottleneck.
	aggResults := aggCellResults(resMap, aggFunc)

	// wg.Wait() // redundant wait. groupByCell ensures that resCh gets drained.
	return aggResults, nil
}

// Produce blocks from a raster band, putting them in a channel to be consumed
// downstream. The production here is happening serially, but there would be
// very little speedup from parallelising at this step.
func genBlocks(band *BandContainer, done <-chan struct{}) <-chan godal.Block {
	logrus.Debug("Entered genBlocks")

	blocks := make(chan godal.Block)
	firstBlock := band.Band.Structure().FirstBlock()
	go func() {
		defer close(blocks)
		for block, ok := firstBlock, true; ok; block, ok = block.Next() {
			select {
			// TODO: Re-think the necessity of the done channel pattern. This isn't a streaming pipeline,
			//			 and right now this isn't doing anything, because groupByCell could block and done wouldn't close.
			case blocks <- block:
			case <-done:
				return
			}
		}
	}()
	logrus.Debug("Exited genBlocks")
	return blocks
}

func processBlocks(band *BandContainer, blocks <-chan godal.Block) chan S2CellData {
	logrus.Debug("Entered processBlocks")
	struc := band.Band.Structure()
	resCh := make(chan S2CellData, struc.SizeX*struc.SizeY) // Buffer for worst-case of 1 cell per pixel
	var wg sync.WaitGroup

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go indexBlocks(band, blocks, resCh, &wg)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	logrus.Debug("Exited blockProcessor")
	return resCh
}

// TODO: Tidy this function signature. This is too many params.
func indexBlocks(band *BandContainer, blocks <-chan godal.Block, resCh chan<- S2CellData, wg *sync.WaitGroup) {
	logrus.Debug("Entered indexBlocks")
	defer wg.Done()
	for block := range blocks {
		logrus.Infof("Processing block at [%v, %v]", block.X0, block.Y0)
		err := rasterBlockToS2(band, block, resCh)
		if err != nil {
			logrus.Error(err)
			continue
		}
	}
	logrus.Debug("Exited indexBlocks")
}

// TODO: Record error counts and return them. Need to know how many blocks were
// processed, and how many failed.
func rasterBlockToS2(band *BandContainer, block godal.Block, resCh chan<- S2CellData) error {
	blockOrigin, err := blockOrigin(block, []float64{band.XRes, band.YRes}, band.Origin)
	if err != nil {
		logrus.Error(err)
		return err
	}
	blockBuf := make([]float64, block.H*block.W)

	// Read band into blockBuf
	if err := lockedRead(band, block, blockBuf); err != nil {
		return err
	}

	noData, ok := band.Band.NoData()
	if !ok {
		logrus.Warn("NoData not set")
	}

	for pix := 0; pix < block.W*block.H; pix++ {
		value := blockBuf[pix]
		if value == noData {
			continue
		}
		// GDAL is row-major
		row := pix / block.W
		col := pix % block.W

		lat := blockOrigin.Lat + float64(row)*band.YRes
		lng := blockOrigin.Lng + float64(col)*band.XRes

		latLng := s2.LatLngFromDegrees(lat, lng)
		s2Cell := s2.CellIDFromLatLng(latLng).Parent(s2Lvl)

		cellData := S2CellData{s2Cell, value}
		resCh <- cellData
	}
	return nil
}

// Locking is required to read from compressed rasters.
func lockedRead(band *BandContainer, block godal.Block, blockBuf []float64) error {
	band.mu.Lock()
	defer band.mu.Unlock()
	if err := band.Band.Read(block.X0, block.Y0, blockBuf, block.W, block.H); err != nil {
		return err
	}
	return nil
}

func aggCellResults(resMap map[s2.CellID][]float64, aggFunc AggFunc) []S2CellData {
	logrus.Debug("Entered aggCellResults")
	var aggResults []S2CellData
	for cellID := range resMap {
		aggResults = append(aggResults, aggToS2Cell(cellID, resMap, aggFunc))
	}
	logrus.Debug("Exited aggCellResults")
	return aggResults
}

func aggToS2Cell(cellID s2.CellID, resMap map[s2.CellID][]float64, aggFunc AggFunc) S2CellData {
	values, ok := resMap[cellID]
	if !ok {
		logrus.Debugf("No values found for cell %v", cellID)
		return S2CellData{cellID, 0}
	}

	return S2CellData{cellID, aggFunc(values...)}
}

func groupByCell(resCh <-chan S2CellData) map[s2.CellID][]float64 {
	logrus.Debug("Entered groupByCell")
	var wg sync.WaitGroup
	wg.Add(1)
	outMap := make(map[s2.CellID][]float64)
	go func() {
		defer wg.Done()
		for cellData := range resCh {
			if _, ok := outMap[cellData.cell]; !ok {
				outMap[cellData.cell] = []float64{cellData.data}
			} else {
				outMap[cellData.cell] = append(outMap[cellData.cell], cellData.data)
			}
		}
		return
	}()
	wg.Wait()
	logrus.Debug("Exited groupByCell")
	return outMap
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

func blockOrigin(rasterBlock godal.Block, resolution []float64, origin Point) (Point, error) {
	originLng := float64(rasterBlock.X0)*resolution[0] + origin.Lng
	originLat := float64(rasterBlock.Y0)*resolution[1] + origin.Lat
	return Point{originLat, originLng}, nil
}
