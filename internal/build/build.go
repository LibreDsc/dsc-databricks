package build

// Version is set at build time via ldflags:
//
//	go build -ldflags="-X github.com/LibreDsc/dsc-databricks/internal/build.Version=0.1.0"
var Version = "dev"
