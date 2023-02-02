import { ModuleContext } from "@/core/framework/module";
import { WorkbenchContext } from "@/core/framework/workbench";
import { Input } from "@/lib/components/Input";

export function settingsSigModuleA({moduleContext, workbenchContext}: {moduleContext: ModuleContext, workbenchContext: WorkbenchContext}) {
    const [text, setText] = moduleContext.useModuleState<string>(
        "text",
        "Hello"
    );

    return (
        <div>
            <Input value={text} onChange={(e) => setText(e.target.value)} />
        </div>
    );
};
