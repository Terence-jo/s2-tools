// Package cmd /*
package cmd

import (
	"s2-tools/cellsio"
	"s2-tools/celltools"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var memLimit int
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
	memory limits when stripes are read. Compression is now properly
	supported, but not tested.

	Options:
		--numWorkers: Number of workers to spawn for parallel processing. Not recommended
									to exceed number of CPU cores.
		--s2Lvl:			S2 cell level to generate results for. Essentially output resolution.
		--aggFunc:		Function to use when aggregating to S2 cell. Default is the mean,
									choose from: mean, sum, max, min`,
	Run: func(cmd *cobra.Command, args []string) {
		setLogLevels()

		sink := func(cellData chan celltools.S2CellData) error {
			return cellsio.StreamToParquet(cellData, args[1], numWorkers)
		}

		aggFunc := chooseAggFunc(viper.GetString("aggFunc"))

		opts := celltools.ConfigOpts{
			NumWorkers: numWorkers,
			S2Lvl:      s2Lvl,
			AggFunc:    aggFunc,
			MemLimit:   memLimit,
		}

		if err := celltools.RasterToS2(args[0], opts, sink); err != nil {
			panic(err)
		}
	},
}

func chooseAggFunc(funcFlag string) celltools.AggFunc {
	switch funcFlag {
	case "mean":
		return celltools.Mean
	case "sum":
		return celltools.Sum
	case "max":
		return celltools.Max
	case "min":
		return celltools.Min
	default:
		logrus.Warnf("Aggregation function %s not recognized, using mean", funcFlag)
		return celltools.Mean
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

	indexrasterCmd.Flags().IntVarP(&memLimit, "memLimitGB", "m", 8, "Memory limit in GB for raster processing")
	err = viper.BindPFlag("memLimitGB", indexrasterCmd.Flags().Lookup("memLimitGB"))
	if err != nil {
		logrus.Exit(1)
	}
}
