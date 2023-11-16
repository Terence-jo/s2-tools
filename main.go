package main

import (
	"fmt"
	"github.com/golang/geo/s2"
	"github.com/sirupsen/logrus"
	"os"
)

func main() {
	s2Data, err := RasterToS2("test.tif")
	if err != nil {
		logrus.Error(err)
		return
	}
	meanData := make(map[s2.CellID]float64)
	for cell, values := range s2Data {
		meanData[cell] = mean(values)
	}

	f, err := os.Create("test.csv")
	if err != nil {
		logrus.Error(err)
		return
	}
	defer f.Close()

	if _, err := f.WriteString("cellID,mean\n"); err != nil {
		logrus.Error(err)
		return
	}
	for cell, mean := range meanData {
		if _, err := f.WriteString(fmt.Sprintf("%v,%v\n", int64(cell), mean)); err != nil {
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
