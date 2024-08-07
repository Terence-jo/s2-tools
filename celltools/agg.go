package celltools

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

func Max(inData ...float64) float64 {
	var max float64
	for _, val := range inData {
		if val > max {
			max = val
		}
	}
	return max
}

func Min(inData ...float64) float64 {
	min := inData[0]
	for _, val := range inData[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func Mode(inData ...float64) float64 {
	counts := make(map[float64]int)
	for _, val := range inData {
		counts[val]++
	}
	var mode float64
	var maxCount int
	for val, count := range counts {
		if count > maxCount {
			mode = val
			maxCount = count
		}
	}
	return mode
}
