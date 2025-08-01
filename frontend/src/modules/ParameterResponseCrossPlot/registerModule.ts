import { ModuleCategory, ModuleDevState } from "@framework/Module";
import { ModuleDataTagId } from "@framework/ModuleDataTags";
import { ModuleRegistry } from "@framework/ModuleRegistry";
import { SyncSettingKey } from "@framework/SyncSettings";

import type { Interfaces } from "./interfaces";
import { preview } from "./preview";
import { receiverDefs } from "./receiverDefs";

export const MODULE_NAME = "ParameterResponseCrossPlot";

const description = "Cross plot input parameters and the responses from a connected module.";

ModuleRegistry.registerModule<Interfaces>({
    moduleName: MODULE_NAME,
    defaultTitle: "Parameter/Response - Scatter plot",
    category: ModuleCategory.SUB,
    devState: ModuleDevState.PROD,
    syncableSettingKeys: [SyncSettingKey.PARAMETER],
    dataTagIds: [ModuleDataTagId.PARAMETERS, ModuleDataTagId.SUMMARY, ModuleDataTagId.INPLACE_VOLUMES],
    channelReceiverDefinitions: receiverDefs,
    description,
    preview,
});
