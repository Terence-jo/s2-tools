package celltools

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
	"github.com/sirupsen/logrus"
)

const (
	EarthRadius  float64 = 6371000
	CellWKTSize  int     = 19*5 + 11
	CellDataSize int     = CellWKTSize + 16
	BytesInGB    int     = 1024 * 1024 * 1024
)

type ConfigOpts struct {
	NumWorkers int
	S2Lvl      int
	AggFunc    AggFunc
	MemLimit   int
}

type Point struct {
	Lat float64
	Lng float64
}

type BandContainer struct {
	Band   godal.Band
	Origin Point
	XRes   float64
	YRes   float64
	mu     *sync.Mutex
	wg     *sync.WaitGroup
}

type S2CellData struct {
	Cell       s2.CellID
	Data       float64
	GeomString string
}

type S2CellGeom struct {
	cell s2.CellID
	geom string
}

func (c S2CellData) String() string {
	return fmt.Sprintf("%v;%v;%s", int64(c.Cell), c.Data, c.GeomString)
}

type AggFunc func(...float64) float64

var numWorkers int
var s2Lvl int

func RasterToS2(path string, opts ConfigOpts, sink func(chan S2CellData) error) error {
	godal.RegisterAll()
	// Bad pattern, this is lazy. Figure out something better
	numWorkers = opts.NumWorkers
	s2Lvl = opts.S2Lvl
	var wg sync.WaitGroup
	var mu sync.Mutex

	ds, err := godal.Open(path)
	if err != nil {
		logrus.Error(err)
		return err
	}
	defer func() {
		err = errors.Join(err, ds.Close())
	}()

	origin, xRes, yRes, err := getOriginAndResolution(ds)
	if err != nil {
		return err
	}

	band := ds.Bands()[0]
	bandWithInfo := BandContainer{band, origin, xRes, yRes, &mu, &wg}

	startTime := time.Now()
	wg.Add(numWorkers)
	s2Data, err := indexBand(&bandWithInfo, opts)
	if err != nil {
		logrus.Error(err)
		return err
	}

	sink(s2Data)
	wg.Wait()
	logrus.Infof("Indexing took %v", time.Since(startTime))
	return nil
}

func indexBand(bandWithInfo *BandContainer, opts ConfigOpts) (chan S2CellData, error) {
	// Set up a done channel that can signal time to close for the whole pipeline.
	// Done will close when the pipeline exits, signalling that it is time to abandon
	// upstream stages. Experiment with and without this.
	// var wg sync.WaitGroup // this WaitGroup is redundant. Might still want to figure out a robust way to ensure completion though.

	// Asynchronous generation of blocks to be consumed.
	blocks := genBlocks(bandWithInfo)
	// Parallel processing of each block produced above.
	resCh := processBlocks(bandWithInfo, blocks, opts)

	return resCh, nil
}

// Produce blocks from a raster band, putting them in a channel to be consumed
// downstream. The production here is happening serially, but there would be
// very little speedup from parallelising at this step.
func genBlocks(band *BandContainer) <-chan godal.Block {
	logrus.Debug("Entered genBlocks")

	blocks := make(chan godal.Block)
	firstBlock := band.Band.Structure().FirstBlock()
	go func() {
		defer close(blocks)
		for block, ok := firstBlock, true; ok; block, ok = block.Next() {
			blocks <- block
		}
	}()
	logrus.Debug("Exited genBlocks")
	return blocks
}

func processBlocks(band *BandContainer, blocks <-chan godal.Block, opts ConfigOpts) chan S2CellData {
	logrus.Debug("Entered processBlocks")
	bufferSize := opts.MemLimit * BytesInGB / CellDataSize
	resCh := make(chan S2CellData, bufferSize)

	for i := 0; i < numWorkers; i++ {
		go indexBlocks(band, blocks, opts, resCh)
	}

	go func() {
		band.wg.Wait()
		close(resCh)
	}()

	logrus.Debug("Exited blockProcessor")
	return resCh
}

// TODO: Tidy this function signature. This is too many params.
func indexBlocks(band *BandContainer, blocks <-chan godal.Block, opts ConfigOpts, resCh chan S2CellData) {
	logrus.Debug("Entered indexBlocks")
	defer band.wg.Done()
	for block := range blocks {
		logrus.Infof("Processing block at [%v, %v]", block.X0, block.Y0)
		err := rasterBlockToS2(band, block, opts, resCh)
		if err != nil {
			logrus.Error(err)
			continue
		}
	}
	logrus.Debug("Exited indexBlocks")
}

func rasterBlockToS2(band *BandContainer, block godal.Block, opts ConfigOpts, resCh chan S2CellData) error {
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

	var results []S2CellData
	for pix := 0; pix < block.W*block.H; pix++ {
		value := blockBuf[pix]
		if value == noData {
			continue
		}
		// GDAL is row-major
		row := pix / block.W
		col := pix % block.W

		lat := blockOrigin.Lat + (float64(row)-0.5)*band.YRes
		lng := blockOrigin.Lng + (float64(col)+0.5)*band.XRes

		pixArea := pixelArea(lat, band.XRes)

		latLng := s2.LatLngFromDegrees(lat, lng)
		s2Cell := s2.CellIDFromLatLng(latLng).Parent(s2Lvl)
		geomString := cellToWKT(s2.CellFromCellID(s2Cell))

		// S2 areas are in steradians, so we need to convert to square meters.
		cellArea := s2.CellFromCellID(s2Cell).ApproxArea() * EarthRadius * EarthRadius
		if cellArea < pixArea {
			value = value * cellArea / pixArea
		}

		cellData := S2CellData{s2Cell, value, geomString}
		results = append(results, cellData)
	}
	groupedResults := groupByCell(results)
	aggCellResults(groupedResults, opts.AggFunc, resCh)

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

func aggCellResults(resMap map[S2CellGeom][]float64, aggFunc AggFunc, resCh chan S2CellData) {
	logrus.Debug("Entered aggCellResults")

	for cellGeom := range resMap {
		resCh <- aggToS2Cell(cellGeom, resMap, aggFunc)
	}
	logrus.Debug("Exited aggCellResults")
}

func aggToS2Cell(cellGeom S2CellGeom, resMap map[S2CellGeom][]float64, aggFunc AggFunc) S2CellData {
	values, ok := resMap[cellGeom]
	if !ok {
		logrus.Debugf("No values found for cell %v", cellGeom)
		return S2CellData{cellGeom.cell, 0, cellGeom.geom}
	}

	return S2CellData{cellGeom.cell, aggFunc(values...), cellGeom.geom}
}

func groupByCell(results []S2CellData) map[S2CellGeom][]float64 {
	logrus.Debug("Entered groupByCell")

	outMap := make(map[S2CellGeom][]float64)
	for _, cellData := range results {
		cellGeom := S2CellGeom{cellData.Cell, cellData.GeomString}

		outMap[cellGeom] = append(outMap[cellGeom], cellData.Data)

	}
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

func pixelArea(latitude float64, resolution float64) float64 {
	pixWidth := haversinePixelWidth(latitude, resolution)
	pixHeight := (math.Pi / 180) * resolution * EarthRadius
	return pixWidth * pixHeight
}

func haversinePixelWidth(latitude float64, resolution float64) float64 {
	latRad := latitude * math.Pi / 180
	resRad := resolution * math.Pi / 180
	a := math.Pow(math.Cos(latRad), 2) * math.Pow(math.Sin(resRad/2), 2)
	return 2 * EarthRadius * math.Asin(math.Sqrt(a))
}
