package cellsio

import (
	"os"
	"s2-tools/celltools"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

const RowBufferSize = 10000

type CellRow struct {
	S2id  int64   `parquet:"s2_id, type=INT64"`
	Value float64 `parquet:"value, type=DOUBLE"`
	Geom  string  `parquet:"geom, type=UTF8"`
}

func StreamToParquet(cellData chan celltools.S2CellData, path string, numWorkers int) error {
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

	var j int
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() error {
			defer wg.Done()
			var rowBuf []CellRow
			for cell := range cellData {
				flushData := (j > 0) && (j%RowBufferSize == 0)
				if flushData {
					logrus.Infof("Writing cell %d", j)
					mu.Lock()
					if _, err := writer.Write(rowBuf); err != nil {
						return err
					}
					mu.Unlock()
					rowBuf = []CellRow{}
				}

				row := CellRow{int64(cell.Cell), cell.Data, cell.GeomString}
				rowBuf = append(rowBuf, row)
				j++
			}
			return nil
		}()
	}
	wg.Wait()
	return nil
}
