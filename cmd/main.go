package main

import (
	"fmt"
	"os"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"

	// Import resources package to register all resource handlers via init()
	_ "github.com/LibreDsc/dsc-databricks/internal/resources"
)

func main() {
	cmd := dsc.NewRootCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
