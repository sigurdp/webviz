import { ModuleRegistry } from "@/core/";
import { settingsSigModuleA } from "./settingsSigModuleA";
import { viewSigModuleA } from "./viewSigModuleA";

const module = ModuleRegistry.registerModule<T>("SigModuleA", {
    
});

module.view = viewSigModuleA;
module.settings = settingsSigModuleA;
