import React from "react";
import Plot from "react-plotly.js";

import { ModuleFCProps } from "@framework/Module";
import { useSubscribedValue } from "@framework/WorkbenchServices";
import { useElementSize } from "@lib/hooks/useElementSize";

import Plotly from "plotly.js";
import { PlotData } from "plotly.js";

import { useStatisticalVectorDataQuery, useVectorDataQuery } from "./sigPlotlyQueryHooks";
import { SigPlotlyState } from "./sigPlotlyState";

interface MyPlotData extends Partial<PlotData> {
    realizationNumber?: number | null;

    // Did they forget to expose this one
    legendrank?: number;
}

//-----------------------------------------------------------------------------------------------------------
export function SigPlotlyView({ moduleContext, workbenchServices }: ModuleFCProps<SigPlotlyState>) {
    const wrapperDivRef = React.useRef<HTMLDivElement>(null);
    const wrapperDivSize = useElementSize(wrapperDivRef);
    const caseUuid = useSubscribedValue("navigator.caseId", workbenchServices);
    const ensembleName = moduleContext.useStoreValue("ensembleName");
    const vectorName = moduleContext.useStoreValue("vectorName");
    const resampleFrequency = moduleContext.useStoreValue("resamplingFrequency");
    const showStatistics = moduleContext.useStoreValue("showStatistics");
    const realizationsToInclude = moduleContext.useStoreValue("realizationsToInclude");
    const [highlightRealization, setHighlightRealization] = React.useState(-1);

    console.log(`render SigPlotlyView  ensembleName=${ensembleName}  vectorName=${vectorName}  caseUuid=${caseUuid}`);

    const vectorQuery = useVectorDataQuery(
        caseUuid,
        ensembleName,
        vectorName,
        resampleFrequency,
        realizationsToInclude
    );

    const statisticsQuery = useStatisticalVectorDataQuery(
        caseUuid,
        ensembleName,
        vectorName,
        resampleFrequency,
        realizationsToInclude,
        showStatistics
    );

    React.useEffect(
        function subscribeToHoverRealizationTopic() {
            const unsubscribeFunc = workbenchServices.subscribe("global.hoverRealization", ({ realization }) => {
                setHighlightRealization(realization);
            });
            return unsubscribeFunc;
        },
        [workbenchServices]
    );

    const tracesDataArr: MyPlotData[] = [];

    if (vectorQuery.data && vectorQuery.data.length > 0) {
        let highlightedTrace: MyPlotData | null = null;
        for (let i = 0; i < Math.min(vectorQuery.data.length, 10); i++) {
            const vec = vectorQuery.data[i];
            const isHighlighted = vec.realization === highlightRealization ? true : false;
            const curveColor = vec.realization === highlightRealization ? "red" : "green";
            const lineWidth = vec.realization === highlightRealization ? 3 : 1;
            const trace: MyPlotData = {
                x: vec.timestamps,
                y: vec.values,
                name: `real-${vec.realization}`,
                realizationNumber: vec.realization,
                legendrank: vec.realization,
                type: "scatter",
                mode: "lines",
                line: { color: curveColor, width: lineWidth },
            };

            if (isHighlighted) {
                highlightedTrace = trace;
            } else {
                tracesDataArr.push(trace);
            }
        }

        if (highlightedTrace) {
            tracesDataArr.push(highlightedTrace);
        }
    }

    if (showStatistics && statisticsQuery.data) {
        for (const statValueObj of statisticsQuery.data.value_objects) {
            const trace: MyPlotData = {
                x: statisticsQuery.data.timestamps,
                y: statValueObj.values,
                name: statValueObj.statistic_function,
                legendrank: -1,
                type: "scatter",
                mode: "lines",
                line: { color: "lightblue", width: 2, dash: "dot" },
            };
            tracesDataArr.push(trace);
        }
    }

    function handleHover(e: Plotly.PlotHoverEvent) {
        const curveData = e.points[0].data as MyPlotData;
        console.log(`handleHover() ${curveData.realizationNumber}   ${e.xvals}   `);
        if (typeof curveData.realizationNumber === "number") {
            setHighlightRealization(curveData.realizationNumber);
            workbenchServices.publishGlobalData("global.hoverRealization", {
                realization: curveData.realizationNumber,
            });
        }
    }

    function handleUnHover(e: Plotly.PlotMouseEvent) {
        console.log(`handleUnHover()`);
        setHighlightRealization(-1);
    }

    const layout: Partial<Plotly.Layout> = {
        width: wrapperDivSize.width, 
        height: wrapperDivSize.height,
        title: vectorName?.toUpperCase(),
        // shapes: [
        //     {
        //         type: "line",
        //         x0: "2020-01-11",
        //         y0: 0,
        //         x1: "2020-01-11",
        //         yref: "paper",
        //         y1: 1,
        //         line: {
        //             color: "grey",
        //             width: 1.5,
        //             dash: "dot",
        //         },
        //     },
        // ],
    };

    return (
        <div className="w-full h-full flex flex-col">
            <div>
            ensembleName: {ensembleName ?? "---"}
            <br />
            vectorName: {vectorName ?? "---"}
            <br />
            <br />
            vector status: {vectorQuery.status}
            <br />
            statistics status: {statisticsQuery.status}
            </div>
            <div className="flex-grow h-0" ref={wrapperDivRef}>
            <Plot
                data={tracesDataArr}
                layout={layout}
                onHover={handleHover}
                onUnhover={handleUnHover}
            />
            </div>
        </div>
    );
}
