package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	s2Data, err := RasterToS2("test.tif")
	if err != nil {
		logrus.Error(err)
		return
	}

	f, err := os.Create("test.csv")
	if err != nil {
		logrus.Error(err)
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			logrus.Error(err)
		}
	}()

	if _, err := f.WriteString("cellID,meanValue\n"); err != nil {
		logrus.Error(err)
		return
	}
	for _, cellData := range s2Data {
		if _, err := f.WriteString(fmt.Sprintf("%v,%v\n", int64(cellData.cell), cellData.data)); err != nil {
			logrus.Error(err)
			return
		}
	}
	if err := f.Sync(); err != nil {
		logrus.Error(err)
		return
	}
}

func mean(values []float64) float64 {
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}
