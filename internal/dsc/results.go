package dsc

// GetResult represents the result of a Get operation on a DSC resource.
type GetResult struct {
	// ActualState is the current state of the resource.
	ActualState any `json:"actualState"`
}

// SetResult represents the result of a Set operation on a DSC resource.
type SetResult struct {
	// BeforeState is the state of the resource before the Set operation.
	BeforeState any `json:"beforeState"`

	// AfterState is the state of the resource after the Set operation.
	AfterState any `json:"afterState"`

	// ChangedProperties lists the property names that were changed during the Set operation.
	ChangedProperties []string `json:"changedProperties,omitempty"`
}

// TestResult represents the result of a Test operation on a DSC resource.
type TestResult struct {
	// DesiredState is the desired state that was tested against.
	DesiredState any `json:"desiredState,omitempty"`

	// ActualState is the current actual state of the resource.
	ActualState any `json:"actualState"`

	// InDesiredState indicates whether the resource is in the desired state.
	InDesiredState bool `json:"inDesiredState"`

	// DifferingProperties lists the property names that differ between desired and actual state.
	DifferingProperties []string `json:"differingProperties,omitempty"`
}
