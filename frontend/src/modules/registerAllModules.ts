import { isDevMode } from "@lib/utils/devMode";

import "./2DViewer/registerModule";
import "./3DViewer/registerModule";
import "./DistributionPlot/registerModule";
import "./FlowNetwork/registerModule";
import "./InplaceVolumesPlot/registerModule";
import "./InplaceVolumesTable/registerModule";
import "./Intersection/registerModule";
import "./Map/registerModule";
import "./ParameterDistributionMatrix/registerModule";
import "./ParameterResponseCorrelationParallelCoordsPlot/registerModule";
import "./ParameterResponseCorrelationBarPlot/registerModule";
import "./ParameterResponseCorrelationMatrixPlot/registerModule";
import "./ParameterResponseCrossPlot/registerModule";
import "./Pvt/registerModule";
import "./Rft/registerModule";
import "./SimulationTimeSeries/registerModule";
import "./SimulationTimeSeriesSensitivity/registerModule";
import "./SubsurfaceMap/registerModule";
import "./TornadoChart/registerModule";
import "./Vfp/registerModule";
import "./WellCompletions/registerModule";
import "./WellLogViewer/registerModule";

if (isDevMode()) {
    await import("./MyModule/registerModule");
    await import("./MyModule2/registerModule");
    await import("./DbgWorkbenchSpy/registerModule");
}
