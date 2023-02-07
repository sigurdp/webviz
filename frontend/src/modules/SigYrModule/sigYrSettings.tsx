import React from "react";
import { useQuery } from "react-query";

import { ModuleFCProps } from "@framework/Module";
import { ListBox } from "@lib/components/ListBox";
import { ListBoxItem } from "@lib/components/ListBox/list-box";

import { fetchStations } from "./sigYrEndpoints";
import { SigExpSharedState } from "./sigYrSharedState";

//-----------------------------------------------------------------------------------------------------------
export function sigYrSettings({ moduleContext, workbenchServices }: ModuleFCProps<SigExpSharedState>) {
    console.log("render sigYrSettings");
    const [selectedStation, setSelectedStation] = React.useState<string>("");

    const stationsQueryRes = useQuery({
        queryKey: ["fetchStations"],
        queryFn: fetchStations,
        onSuccess: (data) => setSelectedStation(data[0].eoi),
    });

    let items: ListBoxItem[] = [];
    if (stationsQueryRes.isSuccess) {
        items = stationsQueryRes.data.map((station) => ({ value: station.eoi, label: station.name }));
    }

    function handleStationSelected(eoi: string) {
        const eoiToNameMap = new Map(items.map((item) => [item.value, item.label]));
        console.log(`eoi=${eoi} - name=${eoiToNameMap.get(eoi)}`);
        setSelectedStation(eoi);
    }

    return <ListBox items={items} selectedItem={selectedStation} onSelect={handleStationSelected} />;
}
