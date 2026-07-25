// Package main is the entry point for the dsc-databricks executable.
package main

import (
	"github.com/LibreDsc/dsc-databricks/internal/resources"
	dsc "github.com/LibreDsc/dsc-go-rdk"
)

func main() {
	m := dsc.NewManager("dsc-databricks",
		dsc.WithDescription("Microsoft DSC v3 resources for Databricks"))
	resources.RegisterAll(m)
	m.Main()
}
