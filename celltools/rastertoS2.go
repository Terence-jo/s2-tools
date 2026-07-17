package celltools

import (
	"errors"
	"fmt"
	"math"
	"reflect"
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
	NumWorkers  int
	S2Lvl       int
	AggFunc     AggFunc
	MemLimit    int
	IsExtensive bool
	Verbose     bool
}

type Point struct {
	Lat float64
	Lng float64
}

type BandContainer struct {
	*sync.Mutex
	godal.Band
	Origin Point
	XRes   float64
	YRes   float64
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

func (f AggFunc) IsExtensive() bool {
	smallVals := []float64{
		f(1, 2, 3),
		f(1, 1, 1),
		f(-1, -1, -1),
		f(0, 0, 0),
		f(-1, -2, -3),
	}
	largeVals := []float64{
		f(1, 1, 2, 2, 3, 3),
		f(1, 1, 1, 1, 1, 1),
		f(-1, -1, -1, -1, -1, -1),
		f(0, 0, 0, 0, 0, 0),
		f(-1, -1, -2, -2, -3, -3),
	}
	return !reflect.DeepEqual(smallVals, largeVals)
}

func RasterToS2(path string, opts ConfigOpts, sink func(chan S2CellData) error) error {
	godal.RegisterAll()

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
	bandWithInfo := BandContainer{&sync.Mutex{}, band, origin, xRes, yRes}

	startTime := time.Now()
	s2Data, err := indexBand(&bandWithInfo, opts)
	if err != nil {
		logrus.Error(err)
		return err
	}

	err = sink(s2Data)
	if err != nil {
		return err
	}
	fmt.Printf("\nIndexing took %v", time.Since(startTime))
	return nil
}

func indexBand(bandWithInfo *BandContainer, opts ConfigOpts) (chan S2CellData, error) {
	// Asynchronous generation of blocks to be consumed.
	blocks := genBlocks(bandWithInfo, opts)
	// Parallel processing of each block produced above.
	resCh := processBlocks(bandWithInfo, blocks, opts)

	return resCh, nil
}

// Produce blocks from a raster band, putting them in a channel to be consumed
// downstream.
func genBlocks(band *BandContainer, opts ConfigOpts) <-chan godal.Block {
	logrus.Debug("Entered genBlocks")

	blocks := make(chan godal.Block)
	struc := band.Structure()
	firstBlock := struc.FirstBlock()
	numBlocks := (struc.SizeX * struc.SizeY) / (struc.BlockSizeX * struc.BlockSizeY)
	var i int
	go func() {
		defer close(blocks)
		for block, ok := firstBlock, true; ok; block, ok = block.Next() {
			blocks <- block
			i++
			if !opts.Verbose {
				// If verbose is set, we'll be getting a flood of output from the workers.
				fmt.Printf("\rProcessing block %d of %d", i, numBlocks)
			}
		}
	}()
	logrus.Debug("Exited genBlocks")
	return blocks
}

func processBlocks(band *BandContainer, blocks <-chan godal.Block, opts ConfigOpts) chan S2CellData {
	logrus.Debug("Entered processBlocks")
	resCh := make(chan S2CellData)
	wg := sync.WaitGroup{}

	for i := 0; i < opts.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			logrus.Debug("Entered indexing goroutine")
			defer wg.Done()
			for block := range blocks {
				logrus.Infof("Processing block at [%v, %v]", block.X0, block.Y0)
				cellsMap, err := rasterBlockToS2(band, block, opts)
				if err != nil {
					logrus.Error(err)
					continue
				}
				aggCellResults(cellsMap, opts.AggFunc, resCh)
			}
			logrus.Debug("Exited indexing goroutine")
		}()
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	logrus.Debug("Exited blockProcessor")
	return resCh
}

func rasterBlockToS2(band *BandContainer, block godal.Block, opts ConfigOpts) (map[S2CellGeom][]float64, error) {
	results, err := readBlockToCells(block, band, opts)
	if err != nil {
		return nil, err
	}
	groupedResults := groupByCell(results)

	return groupedResults, nil
}

func readBlockToCells(block godal.Block, band *BandContainer, opts ConfigOpts) ([]S2CellData, error) {
	blockOrigin, err := blockOrigin(block, []float64{band.XRes, band.YRes}, band.Origin)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	// Read band into blockBuf
	blockBuf := make([]float64, block.H*block.W)

	if err := lockedRead(band, block, blockBuf); err != nil {
		return nil, err
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
		s2Cell := s2.CellIDFromLatLng(latLng).Parent(opts.S2Lvl)

		// S2 areas are in steradians, so we need to convert to square meters.
		cellArea := s2.CellFromCellID(s2Cell).ApproxArea() * EarthRadius * EarthRadius
		if (cellArea < pixArea) && opts.IsExtensive {
			value = value * cellArea / pixArea
		}

		// geom string will be created once cells are aggregated
		cellData := S2CellData{s2Cell, value, ""}
		results = append(results, cellData)
	}
	return results, nil
}

// Locking is required to read from compressed rasters.
func lockedRead(band *BandContainer, block godal.Block, blockBuf []float64) error {
	band.Lock()
	defer band.Unlock()
	if err := band.Band.Read(block.X0, block.Y0, blockBuf, block.W, block.H); err != nil {
		return err
	}
	return nil
}

func aggCellResults(resMap map[S2CellGeom][]float64, aggFunc AggFunc, resCh chan S2CellData) {
	logrus.Debug("Entered aggCellResults")

	for cellGeom := range resMap {
		values := resMap[cellGeom]
		geomString := cellToWKT(s2.CellFromCellID(cellGeom.cell))
		resCh <- S2CellData{cellGeom.cell, aggFunc(values...), geomString}
		// free memory occupied by cell's data as soon as it's not needed
		delete(resMap, cellGeom)
	}
	logrus.Debug("Exited aggCellResults")
}

func groupByCell(results []S2CellData) map[S2CellGeom][]float64 {
	logrus.Debug("Entered groupByCell")

	outMap := make(map[S2CellGeom][]float64)
	for _, cellData := range results {
		cellGeom := S2CellGeom{cellData.Cell, ""}

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