import { ModuleRegistry } from "@framework/ModuleRegistry";

import { sigExpSettings } from "./sigExpSettings";
import { SigExpSharedState } from "./sigExpSharedState";
import { sigExpView } from "./sigExpView";

const initialState: SigExpSharedState = {
    view_iterationId: null,
    view_baseVectorName: null,
    view_diffVectorName: null,
};

const module = ModuleRegistry.initModule<SigExpSharedState>("SigExpModule", initialState);

module.viewFC = sigExpView;
module.settingsFC = sigExpSettings;
