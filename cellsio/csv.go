package cellsio

import (
	"github.com/sirupsen/logrus"
	"os"
	"s2-tools/celltools"
)

func WriteToCSV(cellData []celltools.S2CellData, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			logrus.Error(err)
		}
	}()

	if _, err := f.WriteString("s2_id,value\n"); err != nil {
		return err
	}

	for i, cell := range cellData {
		if i%10000 == 0 {
			logrus.Infof("Writing cell %d", i)
		}
		if _, err := f.WriteString(cell.String() + "\n"); err != nil {
			return err
		}
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return nil
}
