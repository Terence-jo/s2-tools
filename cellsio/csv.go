package cellsio

import (
	"os"
	"s2-tools/celltools"
	"sync"

	"github.com/sirupsen/logrus"
)

// TODO: Use batches and the memory limit like in the parquet writer.
func StreamToCSV(cellData chan celltools.S2CellData, path string, numWorkers int, memLimitGB int) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	workerPool := make(chan struct{}, numWorkers)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			logrus.Error(err)
		}
		close(workerPool)
	}()

	if _, err := f.WriteString("s2_id;value;geom\n"); err != nil {
		return err
	}

	var j int
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() error {
			defer wg.Done()
			for cell := range cellData {
				j++
				printProgress := j%100000 == 0
				row := cell.String()
				if printProgress {
					logrus.Infof("Writing cell %d", j)
				}

				mu.Lock()
				if _, err := f.WriteString(row + "\n"); err != nil {
					return err
				}
				mu.Unlock()
			}
			return nil
		}()
	}
	wg.Wait()
	if err = f.Sync(); err != nil {
		return err
	}
	return nil
}
