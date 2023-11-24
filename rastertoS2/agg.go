package rastertoS2

import "math"

func Mean(inData ...float64) float64 {
	sum := Sum(inData...)
	return sum / float64(len(inData))
}

func Sum(inData ...float64) float64 {
	var sum float64
	for _, val := range inData {
		sum += val
	}
	return sum
}

func SumLn(inData ...float64) float64 {
	var sum float64
	for _, val := range inData {
		sum += math.Log(val)
	}
	return sum
}
