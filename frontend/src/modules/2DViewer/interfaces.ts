import type { InterfaceInitialization } from "@framework/UniDirectionalModuleComponentsInterface";

import type { DataProviderManager } from "../_shared/DataProviderFramework/framework/DataProviderManager/DataProviderManager";

import { dataProviderManagerAtom, preferredViewLayoutAtom } from "./settings/atoms/baseAtoms";
import type { PreferredViewLayout } from "./types";


export type SettingsToViewInterface = {
    layerManager: DataProviderManager | null;
    preferredViewLayout: PreferredViewLayout;
};

export type Interfaces = {
    settingsToView: SettingsToViewInterface;
};

export const settingsToViewInterfaceInitialization: InterfaceInitialization<SettingsToViewInterface> = {
    layerManager: (get) => {
        return get(dataProviderManagerAtom);
    },
    preferredViewLayout: (get) => {
        return get(preferredViewLayoutAtom);
    },
};
