import React from "react";

import { ModuleRegistry } from "@framework/ModuleRegistry";
import { useSubscribedValue } from "@framework/WorkbenchServices";

const module = ModuleRegistry.initModule<{}>("SigModuleB", {});

module.viewFC = function viewSigModuleB({ moduleContext, workbenchServices }) {
    // Use via hook
    const lastMsg = useSubscribedValue("global.infoMessage", workbenchServices);

    const [lastDepth, setLastDepth] = React.useState(-1);

    // Use directly via subscription
    React.useEffect(function subscribeToWorkbenchServicesDepth() {
        const unsub = workbenchServices.subscribe("global.depth", (newDepth: number) => {
            console.log("Got notified about new depth: " + newDepth);
            setLastDepth(newDepth);
        });
        return unsub;
    }, []);

    const fieldName = useSubscribedValue("navigator.fieldName", workbenchServices);

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

module.settingsFC = function settingsSigModuleB() {
    return <div>"nada"</div>;
};
