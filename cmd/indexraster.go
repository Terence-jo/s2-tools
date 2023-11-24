// Package cmd /*
package cmd

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"s2-tools/cellsio"
	"s2-tools/rastertoS2"
)

var numWorkers int
var s2Lvl int

// indexrasterCmd represents the indexraster command
var indexrasterCmd = &cobra.Command{
	Use:   "indexraster",
	Short: "Convert a raster to S2 cells, aggregating over each cell",
	Long: `Convert a GeoTIFF to a CSV (more options to follow) file
	containing S2 cell IDs and aggregated values for the raster cells
	contained.

	Use tiled rasters for best performance. Untiled rasters can exceed
	memory limits when stripes are read. Currently only uncompressed GeoTIFFs
	are supported.

	Options:
		--numWorkers: Number of workers to spawn for parallel processing
		--s2Lvl:			S2 cell level to generate results for. Essentially output resolution
		--aggFunc:		Function to use when aggregating to S2 cell. Default is the mean,
									choose from: mean, sum, max, min, majority, minority (ONLY mean
									IMPLEMENTED AS OF YET)`,
	Run: func(cmd *cobra.Command, args []string) {
		setLogLevels()

		aggFunc := chooseAggFunc(viper.GetString("aggFunc"))
		cellData, err := rastertoS2.RasterToS2(args[0], aggFunc, numWorkers, s2Lvl)
		if err != nil {
			panic(err)
		}
		err = cellsio.WriteToCSV(cellData, args[1])
		if err != nil {
			panic(err)
		}
	},
}

func chooseAggFunc(funcFlag string) rastertoS2.AggFunc {
	switch funcFlag {
	case "mean":
		return rastertoS2.Mean
	case "sum":
		return rastertoS2.Sum
	case "sumln":
		return rastertoS2.SumLn
	default:
		logrus.Warnf("Aggregation function %s not recognized, using mean", funcFlag)
		return rastertoS2.Mean
	}
}
func setLogLevels() {
	if viper.GetBool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	} else if viper.GetBool("verbose") {
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(logrus.WarnLevel)
	}
}

func init() {
	rootCmd.AddCommand(indexrasterCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// indexrasterCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// indexrasterCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	indexrasterCmd.Flags().IntVarP(&numWorkers, "numWorkers", "n", 8, "Number of workers to spawn for parallel processing")
	err := viper.BindPFlag("numWorkers", indexrasterCmd.Flags().Lookup("numWorkers"))
	if err != nil {
		logrus.Exit(1)
	}

	indexrasterCmd.Flags().IntVarP(&s2Lvl, "s2Lvl", "l", 11, "S2 cell level to generate results for. Essentially output resolution")
	err = viper.BindPFlag("s2Lvl", indexrasterCmd.Flags().Lookup("s2Lvl"))
	if err != nil {
		logrus.Exit(1)
	}

	indexrasterCmd.Flags().StringP("aggFunc", "a", "mean", "Function to use when aggregating to S2 cell. Default is the mean, choose from: mean, sum, sumln")
	err = viper.BindPFlag("aggFunc", indexrasterCmd.Flags().Lookup("aggFunc"))
	if err != nil {
		logrus.Exit(1)
	}
}
