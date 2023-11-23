// Package cmd /*
package cmd

import (
	"github.com/spf13/cobra"
	"s2-tools/cellsio"
	"s2-tools/rastertoS2"
)

// indexrasterCmd represents the indexraster command
var indexrasterCmd = &cobra.Command{
	Use:   "indexraster",
	Short: "Convert a raster to S2 cells, aggregating over each cell",
	Long: `Convert a GeoTIFF to a CSV (more options to follow) file
	containing S2 cell IDs and aggregated values for the raster cells
	contained.

	Options:
		--numWorkers: Number of workers to spawn for parallel processing
		--s2Lvl:			S2 cell level to generate results for. Essentially output resolution
		--aggFunc:		Function to use when aggregating to S2 cell. Default is the mean,
									choose from: mean, sum, max, min, majority, minority (ONLY mean
									IMPLEMENTED AS OF YET)`,
	Run: func(cmd *cobra.Command, args []string) {
		cellData, err := rastertoS2.RasterToS2(args[0])
		if err != nil {
			panic(err)
		}
		err = cellsio.WriteToCSV(cellData, args[1])
		if err != nil {
			panic(err)
		}
	},
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
}
