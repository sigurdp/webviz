import type { ColorScaleSerialization } from "@lib/utils/ColorScale";

import type { SettingsKeysFromTuple } from "./utils";

import type { GroupType } from "../groups/groupTypes";
import type { Setting, Settings } from "../settings/settingsDefinitions";

// The following interfaces/types are used to define the structure of the serialized state of the respective items in the data layer framework.

export enum SerializedType {
    DATA_LAYER_MANAGER = "data-layer-manager",
    GROUP = "group",
    DATA_LAYER = "data-layer",
    SETTINGS_GROUP = "settings-group",
    COLOR_SCALE = "color-scale",
    DELTA_SURFACE = "delta-surface",
    SHARED_SETTING = "shared-setting",
}

export interface SerializedItem {
    id: string;
    type: SerializedType;
    name: string;
    expanded: boolean;
    visible: boolean;
}

export type SerializedSettingsState<
    TSettings extends Settings,
    TSettingKey extends SettingsKeysFromTuple<TSettings> = SettingsKeysFromTuple<TSettings>,
> = {
    [K in TSettingKey]: string;
};

export interface SerializedDataLayer<
    TSettings extends Settings,
    TSettingKey extends SettingsKeysFromTuple<TSettings> = SettingsKeysFromTuple<TSettings>,
> extends SerializedItem {
    type: SerializedType.DATA_LAYER;
    layerType: string;
    settings: SerializedSettingsState<TSettings, TSettingKey>;
}

export interface SerializedGroup extends SerializedItem {
    type: SerializedType.GROUP;
    groupType: GroupType;
    color: string;
    children: SerializedItem[];
}

export interface SerializedSettingsGroup extends SerializedItem {
    type: SerializedType.SETTINGS_GROUP;
    children: SerializedItem[];
}

export interface SerializedColorScale extends SerializedItem {
    type: SerializedType.COLOR_SCALE;
    colorScale: ColorScaleSerialization;
    userDefinedBoundaries: boolean;
}

export interface SerializedSharedSetting extends SerializedItem {
    type: SerializedType.SHARED_SETTING;
    wrappedSettingType: Setting;
    value: string;
}

export interface SerializedDataLayerManager extends SerializedItem {
    type: SerializedType.DATA_LAYER_MANAGER;
    children: SerializedItem[];
}

export interface SerializedDeltaSurface extends SerializedItem {
    type: SerializedType.DELTA_SURFACE;
    children: SerializedItem[];
}
