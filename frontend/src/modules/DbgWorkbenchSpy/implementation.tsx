import React from "react";

import { omit } from "lodash";

import { ModuleFCProps } from "@framework/Module";
import { SelectItem } from "@framework/SelectionService";
import { AllTopicDefinitions, WorkbenchServices } from "@framework/WorkbenchServices";
import { Button } from "@lib/components/Button";

export type SharedState = {
    triggeredRefreshCounter: number;
};

//-----------------------------------------------------------------------------------------------------------
export function WorkbenchSpySettings(props: ModuleFCProps<SharedState>) {
    const setRefreshCounter = props.moduleContext.useSetStoreValue("triggeredRefreshCounter");
    return (
        <div>
            <Button onClick={() => setRefreshCounter((prev: number) => prev + 1)}>Trigger Refresh</Button>
        </div>
    );
}

//-----------------------------------------------------------------------------------------------------------
export function WorkbenchSpyView(props: ModuleFCProps<SharedState>) {
    const [selectedEnsembles, selectedEnsembles_TS] = useTopicValueWithTS(
        "navigator.ensembles",
        props.workbenchServices
    );
    const [hoverRealization, hoverRealization_TS] = useTopicValueWithTS(
        "global.hoverRealization",
        props.workbenchServices
    );
    const [hoverTimestamp, hoverTimestamp_TS] = useTopicValueWithTS("global.hoverTimestamp", props.workbenchServices);
    const triggeredRefreshCounter = props.moduleContext.useStoreValue("triggeredRefreshCounter");

    const [selectedItems, selectedItems_TS] = useCurrentSelectionWithTS(props.workbenchServices);

    const componentRenderCount = React.useRef(0);
    React.useEffect(function incrementComponentRenderCount() {
        componentRenderCount.current = componentRenderCount.current + 1;
    });

    const componentLastRenderTS = getTimestampString();

    let ensembleSpecAsString: string | undefined;
    if (selectedEnsembles) {
        if (selectedEnsembles.length > 0) {
            ensembleSpecAsString = `${selectedEnsembles[0].ensembleName}  (${selectedEnsembles[0].caseUuid})`;
        } else {
            ensembleSpecAsString = "empty array";
        }
    }

    return (
        <code>
            Navigator topics:
            <table>
                <tbody>{makeTableRow("ensembles", ensembleSpecAsString, selectedEnsembles_TS)}</tbody>
            </table>
            <br />
            Global topics:
            <table>
                <tbody>
                    {makeTableRow("hoverRealization", hoverRealization?.realization, hoverRealization_TS)}
                    {makeTableRow("hoverTimestamp", hoverTimestamp?.timestamp, hoverTimestamp_TS)}
                </tbody>
            </table>
            <br />
            Selection ({selectedItems_TS}):
            {makeSelectedItemsTable(selectedItems)}
            <br />
            <br />
            refreshCounter: {triggeredRefreshCounter}
            <br />
            componentRenderCount: {componentRenderCount.current}
            <br />
            componentLastRenderTS: {componentLastRenderTS}
        </code>
    );
}

function makeTableRow(label: string, value: any, ts: string) {
    return (
        <tr>
            <td>{label}</td>
            <td>
                <b>{value || "N/A"}</b>
            </td>
            <td>({ts})</td>
        </tr>
    );
}

function makeSelectedItemsTable(selectedItems: SelectItem[]) {
     return (
        <table>
            <tbody>
                {selectedItems.map((item, index) => (
                    <tr key={index}>
                        <td>{item.itemType}</td>
                        <td>
                            <b>{JSON.stringify(omit(item, ["itemType"]))}</b>
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}

function getTimestampString() {
    return new Date().toLocaleTimeString("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        fractionalSecondDigits: 2,
    });
}

function useTopicValueWithTS<T extends keyof AllTopicDefinitions>(
    topic: T,
    workbenchServices: WorkbenchServices
): [data: AllTopicDefinitions[T] | null, updatedTS: string] {
    const [latestValue, setLatestValue] = React.useState<AllTopicDefinitions[T] | null>(null);
    const [lastUpdatedTS, setLastUpdatedTS] = React.useState("");

    React.useEffect(
        function subscribeToServiceTopic() {
            function handleNewValue(newValue: AllTopicDefinitions[T]) {
                setLatestValue(newValue);
                setLastUpdatedTS(getTimestampString());
            }
            const unsubscribeFunc = workbenchServices.subscribe(topic, handleNewValue);
            return unsubscribeFunc;
        },
        [topic, workbenchServices]
    );

    return [latestValue, lastUpdatedTS];
}

function useCurrentSelectionWithTS(
    workbenchServices: WorkbenchServices
): [selectedItems: SelectItem[], updatedTS: string] {
    const [latestSelectItems, setLatestSelectItems] = React.useState<SelectItem[]>([]);
    const [lastUpdatedTS, setLastUpdatedTS] = React.useState("");

    React.useEffect(
        function subscribeToSelectionService() {
            function handleWorkbenchSelectionChanged() {
                setLatestSelectItems(workbenchServices.getSelectionService().getSelection());
                setLastUpdatedTS(getTimestampString());
            }
            const unsubscribeFunc = workbenchServices
                .getSelectionService()
                .subscribeToSelectionChanged(handleWorkbenchSelectionChanged);
            return unsubscribeFunc;
        },
        [workbenchServices]
    );

    return [latestSelectItems, lastUpdatedTS];
}
