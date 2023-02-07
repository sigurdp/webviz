import { ModuleFCProps } from "@framework/Module";
import { ModuleContext } from "@framework/ModuleInstance";
import { WorkbenchServices } from "@framework/WorkbenchServices";
import { Button } from "@lib/components/Button";

import { SigModuleAState } from "./stateSigModuleA";

export function viewSigModuleA({ moduleContext, workbenchServices }: ModuleFCProps<SigModuleAState>) {
    const text = moduleContext.useStoreValue("text");

    function handleClickByPublishingMessage(msg: string) {
        console.log(`clicked -- publishing msg=${msg}`);
        workbenchServices.publishGlobalData("global.infoMessage", msg);
    }

    function handleClickByPublishingDepth(depth: number) {
        console.log(`clicked -- publishing depth=${depth}`);
        workbenchServices.publishGlobalData("global.depth", depth);
    }

    return (
        <div>
            <h1>Sigurds Text: {text}</h1>
            <br></br>
            <Button onClick={() => handleClickByPublishingMessage("msg1")}>PubMsg1</Button>
            <Button onClick={() => handleClickByPublishingMessage("msg2")}>PubMsg2</Button>
            <Button onClick={() => handleClickByPublishingMessage("msg3")}>PubMsg3</Button>
            <br></br>
            <br></br>
            <Button onClick={() => handleClickByPublishingDepth(100.5)}>PubDepth 100.5</Button>
            <Button onClick={() => handleClickByPublishingDepth(999.99)}>PubDepth 999.99</Button>
        </div>
    );
}
