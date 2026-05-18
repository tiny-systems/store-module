package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tiny-systems/module/cli"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	_ "github.com/tiny-systems/store-module/components/documentstore"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	// Declare PVC requirement so the platform's install flow exposes
	// storage-size / storage-class fields and the helm command
	// includes --set storage.enabled=true. document_store is
	// file-backed via bbolt and requires durable storage to be useful
	// across pod restarts.
	registry.SetRequirements(module.Requirements{
		Storage: module.StorageRequirements{
			Enabled: true,
			Size:    "5Gi",
		},
	})
}

// RootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "Tiny Systems store module — embedded persistent storage (bbolt + PVC)",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func main() {
	// Default level for this example is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	viper.AutomaticEnv()
	if viper.GetBool("debug") {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cli.RegisterCommands(rootCmd)
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Printf("command execute error: %v\n", err)
	}
}
