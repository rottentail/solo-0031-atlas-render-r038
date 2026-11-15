package engine

import "atlas-render/internal/raster"

// planStub is a minimal compiled plan for result-commit tests that do not
// render pixels.
func planStub() raster.Plan { return raster.Plan{Width: 1, Height: 1, Scale: 1} }
