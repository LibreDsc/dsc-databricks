// Package resources contains all Databricks DSC resource handlers.
// Each resource defines a state struct embedding dsc.ExistProperty and a
// handler implementing the dsc-go-rdk capability interfaces (Gettable,
// Settable, Testable, Deletable, Exportable). Resources are registered with
// the manager via RegisterAll.
package resources
