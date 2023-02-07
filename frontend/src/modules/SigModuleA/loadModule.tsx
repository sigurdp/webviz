import { ModuleRegistry } from "@framework/ModuleRegistry";

import { settingsSigModuleA } from "./settingsSigModuleA";
import { SigModuleAState } from "./stateSigModuleA";
import { viewSigModuleA } from "./viewSigModuleA";

const initialState: SigModuleAState = {
    text: "Hello Sigurd",
};

const module = ModuleRegistry.initModule<SigModuleAState>("SigModuleA", initialState);

module.viewFC = viewSigModuleA;
module.settingsFC = settingsSigModuleA;
