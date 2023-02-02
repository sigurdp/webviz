import { ModuleRegistry } from "@/core/";
import { useWorkbenchSubscribedValue } from "@/core/framework/WorkbenchServices";
import React from "react";

const module = ModuleRegistry.registerModule("SigModuleB");

module.view = function viewSigModuleB({moduleContext, workbenchContext}) {
    // Use via hook
    const lastMsg = useWorkbenchSubscribedValue("InfoMessage", workbenchContext.workbenchServices);

    const [lastDepth, setLastDepth] = React.useState(-1);

    // Use directly via subscription
    React.useEffect(function subscribeToWorkbenchServicesDepth() {
        const unsub = workbenchContext.workbenchServices.subscribe("Depth", (newDepth:number) => {
            console.log("Got notified about new depth: " + newDepth);
            setLastDepth(newDepth);
        });
        return unsub;
    }, []);

    const fieldName = useWorkbenchSubscribedValue("FieldName", workbenchContext.workbenchServices);


    const timeOfRenderStr = new Date().toISOString();

    return (
        <div>
            timeOfRender: {timeOfRenderStr}
            <br></br>
            Field name: {fieldName}
            <br></br>
            Last message: {lastMsg}
            <br></br>
            Last depth: {lastDepth}
        </div>
    );
};

module.settings = function settingsSigModuleB() {
    return (
        <div>
            "nada"
        </div>
    );
};
