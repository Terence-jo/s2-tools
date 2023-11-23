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
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
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
