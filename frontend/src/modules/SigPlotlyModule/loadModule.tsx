import { ModuleRegistry } from "@framework/ModuleRegistry";

import { SigPlotlySettings } from "./sigPlotlySettings";
import { SigPlotlyState } from "./sigPlotlyState";
import { SigPlotlyView } from "./sigPlotlyView";
import { Frequency } from "@api";

const initialState: SigPlotlyState = {
    ensembleName: null,
    vectorName: null,
    resamplingFrequency: Frequency.MONTHLY,
    showStatistics: true,
    realizationsToInclude: null
};

const module = ModuleRegistry.initModule<SigPlotlyState>("SigPlotlyModule", initialState);

module.viewFC = SigPlotlyView;
module.settingsFC = SigPlotlySettings;
