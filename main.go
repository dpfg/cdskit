package main

import (
	"os"

	"github.com/jessevdk/go-flags"
)

// Opts represent all available commands supported by utility
type Opts struct {
	DeleteAllCmd  DeleteAllCmd  `command:"delete-all" description:"Delete all entities"`
	ExportKindCmd ExportKindCmd `command:"export-kind" description:"Export all entities to a JSON or CSV"`
}

func main() {

	var opts Opts
	p := flags.NewParser(&opts, flags.Default)

	if _, err := p.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}
}
