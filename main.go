/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package main

import "s2-tools/cmd"

func main() {
	cmd.Exiecute()
}
}

func mean(values []float64) float64 {
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
>>>>>>> 1cabbe2 (nailed down correct behaviour for BHI data. needed to apply YRes to the blockOrigin Y coord.)
}
