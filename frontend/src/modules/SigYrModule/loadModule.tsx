import { ModuleRegistry } from "@framework/ModuleRegistry";

import { sigYrSettings } from "./sigYrSettings";
import { SigExpSharedState } from "./sigYrSharedState";
import { sigYrView } from "./sigYrView";

const initialState: SigExpSharedState = {
    view_iterationId: null,
    view_baseVectorName: null,
    view_diffVectorName: null,
};

const module = ModuleRegistry.initModule<SigExpSharedState>("SigYrModule", initialState);

module.viewFC = sigYrView;
module.settingsFC = sigYrSettings;
