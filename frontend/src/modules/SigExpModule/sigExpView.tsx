import React from "react";
import { useQuery } from "react-query";

import { ModuleFCProps } from "@framework/Module";
import { useSubscribedValue } from "@framework/WorkbenchServices";

import { getVectorData } from "./sigExpEndpoints";
import { SigExpSharedState } from "./sigExpSharedState";

//-----------------------------------------------------------------------------------------------------------
export function sigExpView({ moduleContext, workbenchServices }: ModuleFCProps<SigExpSharedState>) {
    const iterationId = moduleContext.useStoreValue("view_iterationId");
    const baseVectorName = moduleContext.useStoreValue("view_baseVectorName");
    const diffVectorName = moduleContext.useStoreValue("view_diffVectorName");

    console.log(
        `render sigExpView  iterationId=${iterationId} baseVectorName=${baseVectorName} diffVectorName=${diffVectorName}`
    );

    React.useEffect(function subscribeToWorkbenchServices() {
        const unsub = workbenchServices.subscribe("global.infoMessage", (message: string) => {
            console.log("Got notified, new message is:" + message);
        });
        return unsub;
    }, []);

    const latestMessage = useSubscribedValue("global.infoMessage", workbenchServices);

    // let baseDisplayStr = "nada";
    // let diffDisplayStr = "nada";

    const baseDataQueryRes = useQuery({
        queryKey: ["getVectorData", iterationId, baseVectorName],
        queryFn: () => getVectorData(iterationId || "", baseVectorName || ""),
        enabled: iterationId && baseVectorName ? true : false,
    });

    const diffDataQueryRes = useQuery({
        queryKey: ["getVectorData", iterationId, diffVectorName],
        queryFn: () => getVectorData(iterationId || "", diffVectorName || ""),
        enabled: iterationId && diffVectorName ? true : false,
    });

    let baseDisplayStr = baseDataQueryRes.status.toString();
    if (baseDataQueryRes.isSuccess && baseDataQueryRes.data) {
        baseDisplayStr = `[${baseDataQueryRes.data.toString()}]`;
    }
    let diffDisplayStr = diffDataQueryRes.status.toString();
    if (diffDataQueryRes.isSuccess && diffDataQueryRes.data) {
        diffDisplayStr = `[${diffDataQueryRes.data.toString()}]`;
    }

    return (
        <div>
            View spec:
            <br />
            iteration id: {iterationId ? iterationId : "---"}
            <br />
            base vector: {baseVectorName ? baseVectorName : "---"}
            <br />
            diff vector: {diffVectorName ? diffVectorName : "---"}
            <br />
            <br />
            latest message: {latestMessage || "..."}
            <br />
            <br />
            base data: {baseDisplayStr}
            <br />
            diff data: {diffDisplayStr}
            <br />
        </div>
    );
}
