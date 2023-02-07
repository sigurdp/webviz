import React from "react";
import { UseQueryResult, useQuery } from "react-query";

import { ModuleFCProps } from "@framework/Module";

import { getIterationIds, getVectorNames } from "./sigExpEndpoints";
import { SigExpSharedState } from "./sigExpSharedState";

//-----------------------------------------------------------------------------------------------------------
export function sigExpSettings({ moduleContext, workbenchServices }: ModuleFCProps<SigExpSharedState>) {
    console.log("render sigExpSettings");

    const [iterationId, setIterationId] = React.useState("");
    const [baseVectorName, setBaseVectorName] = React.useState("");
    const [enableDiffCalculation, setEnableDiffCalculation] = React.useState(false);
    const [diffVectorName, setDiffVectorName] = React.useState("");
    const stashedBaseVectorName = React.useRef("");
    const stashedDiffVectorName = React.useRef("");

    const iterationIdsQueryRes = useQuery({
        queryKey: ["getIterationIds"],
        queryFn: getIterationIds,
        onSuccess: function selectDefaultIterationId(iterationIdArr) {
            console.log("selectDefaultIterationId()");
            if (iterationIdArr.length > 0) {
                setIterationId(iterationIdArr[iterationIdArr.length - 1]);
            }
        },
    });

    const vectorNamesQuery = useQuery({
        queryKey: ["getVectorNames", iterationId],
        queryFn: () => getVectorNames(iterationId),
        enabled: iterationId ? true : false,
        onSuccess: function selectDefaultVectorNames(vectorNamesArr) {
            console.log("selectDefaultVectorNames()");
            if (vectorNamesArr.length > 0) {
                if (vectorNamesArr.includes(stashedBaseVectorName.current)) {
                    setBaseVectorName(stashedBaseVectorName.current);
                } else {
                    setBaseVectorName(vectorNamesArr[0]);
                }
                if (vectorNamesArr.includes(stashedDiffVectorName.current)) {
                    setDiffVectorName(stashedDiffVectorName.current);
                } else {
                    setDiffVectorName(vectorNamesArr[vectorNamesArr.length - 1]);
                }
            }
        },
    });

    // Propagate selection to view
    React.useEffect(function updateViewSpec() {
        //console.log(`updateViewSpec() ${iterationId}  ${baseVectorName}  ${enableDiffCalculation}  ${diffVectorName}`);
        const specIterId = iterationId ? iterationId : null;
        const specBaseVecName = specIterId && baseVectorName ? baseVectorName : null;
        const specDiffVecName = specIterId && enableDiffCalculation && diffVectorName ? diffVectorName : null;
        moduleContext.stateStore.setValue("view_iterationId", specIterId);
        moduleContext.stateStore.setValue("view_baseVectorName", specBaseVecName || null);
        moduleContext.stateStore.setValue("view_diffVectorName", specDiffVecName || null);
    });

    function makeSelect(queryRes: UseQueryResult<unknown>, selected: string, onChangeHandler: any) {
        if (queryRes.isLoading) {
            return <div>Loading...</div>;
        }
        if (queryRes.isError) {
            return <div>Error</div>;
        }
        if (queryRes.isSuccess) {
            return (
                <select value={selected} onChange={onChangeHandler}>
                    {(queryRes.data as string[]).map((option) => (
                        <option key={option} value={option}>
                            {option}
                        </option>
                    ))}
                </select>
            );
        }
        return <div>No options</div>;
    }

    function handleIterationSelectionChange(event: React.ChangeEvent<HTMLSelectElement>) {
        console.log("handleIterationSelectionChange() " + event.target.value);
        setIterationId(event.target.value);
        stashedBaseVectorName.current = baseVectorName;
        stashedDiffVectorName.current = diffVectorName;
        setBaseVectorName("");
        setDiffVectorName("");
    }

    function handleBaseVectorSelectionChange(event: React.ChangeEvent<HTMLSelectElement>) {
        console.log("handleBaseVectorSelectionChange() " + event.target.value);
        setBaseVectorName(event.target.value);
    }

    function handleEnableDiffCheckboxChange(event: React.ChangeEvent<HTMLInputElement>) {
        console.log("handleEnableDiffCheckboxChange() " + event.target.checked);
        setEnableDiffCalculation(event.target.checked);
    }

    function handleDiffVectorSelectionChange(event: React.ChangeEvent<HTMLSelectElement>) {
        console.log("handleSelectedDiffVectorChange() " + event.target.value);
        setDiffVectorName(event.target.value);
    }

    return (
        <>
            <label>Iteration</label>
            <br />
            {makeSelect(iterationIdsQueryRes, iterationId, handleIterationSelectionChange)}

            <br />
            <br />
            <label>Select base vector</label>
            <br />
            {makeSelect(vectorNamesQuery, baseVectorName, handleBaseVectorSelectionChange)}

            <br />
            <br />
            <label>
                <input type="checkbox" checked={enableDiffCalculation} onChange={handleEnableDiffCheckboxChange} />
                Calculate diff
            </label>

            {enableDiffCalculation && (
                <>
                    <br />
                    <label>Select diff vector</label>
                    <br />
                    {makeSelect(vectorNamesQuery, diffVectorName || "", handleDiffVectorSelectionChange)}
                </>
            )}
        </>
    );
}
