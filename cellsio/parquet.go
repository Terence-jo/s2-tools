package cellsio

import (
	"os"
	"s2-tools/celltools"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

const (
	CellRowSize = 8 + 8 + 19*5 + 11
	BytesInGB   = 1024 * 1024 * 1024
)

type CellRow struct {
	S2id  int64   `parquet:"s2_id, type=INT64"`
	Value float64 `parquet:"value, type=DOUBLE"`
	Geom  string  `parquet:"geom, type=UTF8"`
}

func StreamToParquet(cellData chan celltools.S2CellData, path string, numWorkers int, memLimitGB int) error {
	var mu sync.Mutex
	var wg sync.WaitGroup

	output, err := os.Create(path)
	if err != nil {
		return err
	}

	schema := parquet.SchemaOf(new(CellRow))
	writer := parquet.NewGenericWriter[CellRow](output, schema, parquet.Compression(&parquet.Snappy))
	defer func() {
		if err := writer.Close(); err != nil {
			logrus.Error(err)
		}
		if err := output.Close(); err != nil {
			logrus.Error(err)
		}
	}()

	wg.Add(numWorkers)
	// Attempting to limit memory usage by flushing data to disk every rowBufferSize rows.
	// Some allocation is just accumulating over iterations, I should try to figure out what it is.
	// The 3 is a fudge factor to accomodate this based on observations.
	rowBufferSize := (memLimitGB * BytesInGB / CellRowSize) / ((celltools.DataDuplicationFactor*numWorkers + numWorkers) * 3)
	for i := 0; i < numWorkers; i++ {
		go func() error {
			var j int
			defer wg.Done()
			rowBuf := make([]CellRow, rowBufferSize)
			for cell := range cellData {
				flushData := ((j+1)%rowBufferSize == 0)
				if flushData {
					logrus.Infof("Writing cell %d", j)
					mu.Lock()
					if _, err := writer.Write(rowBuf); err != nil {
						return err
					}
					if err = writer.Flush(); err != nil {
						return err
					}
					mu.Unlock()
					rowBuf = make([]CellRow, rowBufferSize)
				}

				row := CellRow{int64(cell.Cell), cell.Data, cell.GeomString}
				rowBuf[j%rowBufferSize] = row
				j++
			}
			return nil
		}()
	}
	wg.Wait()
	return nil
}
