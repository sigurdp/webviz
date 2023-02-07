import React from "react";
import { useQuery } from "react-query";

import { ModuleFCProps } from "@framework/Module";
import { useSubscribedValue } from "@framework/WorkbenchServices";

import { fetchStations } from "./sigYrEndpoints";
import { SigExpSharedState } from "./sigYrSharedState";


//-----------------------------------------------------------------------------------------------------------
export function sigYrView({ moduleContext, workbenchServices }: ModuleFCProps<SigExpSharedState>) {
    console.log("render sigYrSettings");

    const stationsQueryRes = useQuery({ queryKey: ["fetchStations"], queryFn: fetchStations });

    let str = "NADA";
    // if (stationsQueryRes.isSuccess) {
    //     console.log(stationsQueryRes.data);
    //     for (const station of stationsQueryRes.data) {
    //         str += station.eoi + " --- " + station.name;
    //     }
    // }

    return <div>{str}</div>;
}
