import { ModuleFCProps } from "@framework/Module";
import { ModuleContext } from "@framework/ModuleInstance";
import { WorkbenchServices } from "@framework/WorkbenchServices";
import { Input } from "@lib/components/Input";

import { SigModuleAState } from "./stateSigModuleA";

export function settingsSigModuleA({ moduleContext, workbenchServices }: ModuleFCProps<SigModuleAState>) {
    const [text, setText] = moduleContext.useStoreState("text");

    return (
        <div>
            <Input value={text} onChange={(e) => setText(e.target.value)} />
        </div>
    );
}
