import { ModuleContext } from "@/core/framework/module";
import { WorkbenchContext } from "@/core/framework/workbench";
import { Button } from "@/lib/components/Button";
import { SharedTopicMap } from "@/core/framework/WorkbenchServices";

export function viewSigModuleA({moduleContext, workbenchContext}: {moduleContext: ModuleContext, workbenchContext: WorkbenchContext}) {
    const text = moduleContext.useModuleStateValue<string>("text","Hello");

    function handleClickByPublishingMessage(msg: string) {
        console.log(`clicked -- publishing msg=${msg}`);
        workbenchContext.workbenchServices.publishSharedData("InfoMessage", msg);
    }

    function handleClickByPublishingDepth(depth: number) {
        console.log(`clicked -- publishing depth=${depth}`);
        workbenchContext.workbenchServices.publishSharedData("Depth", depth);
    }

    function handleClickByPublishingInternalDataField(fieldName: string) {
        console.log(`clicked -- publishing internal data FieldName=${fieldName}`);
        workbenchContext.workbenchServices.internal_publishWorkbenchData("FieldName", fieldName);
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
            <br></br>
            <br></br>
            <Button onClick={() => handleClickByPublishingInternalDataField("FieldA")}>PublishInternalData_Field=FieldA</Button>
            <Button onClick={() => handleClickByPublishingInternalDataField("FieldB")}>PublishInternalData_Field=FieldB</Button>
        </div>
    );
};

