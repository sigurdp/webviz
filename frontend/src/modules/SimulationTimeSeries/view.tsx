import React from "react";
import Plot from "react-plotly.js";

import { ModuleFCProps } from "@framework/Module";
import { SelectItem } from "@framework/SelectionService";
import { useSubscribedValue } from "@framework/WorkbenchServices";
import { useElementSize } from "@lib/hooks/useElementSize";
import { isoStringToTimestampUtcMs } from "@shared-utils/timeUtils";

import { ButtonClickEvent, Layout, PlotData, PlotHoverEvent, PlotMouseEvent } from "plotly.js";

import { useStatisticalVectorDataQuery, useVectorDataQuery } from "./queryHooks";
import { State } from "./state";
import ContinuousLegendWrapper from "@webviz/subsurface-components/dist/components/ColorLegends/WebVizContinuousLegend";

interface MyPlotData extends Partial<PlotData> {
    realizationNumber?: number | null;

    // Did they forget to expose this one
    legendrank?: number;
}

export const view = ({ moduleContext, workbenchServices }: ModuleFCProps<State>) => {
    const wrapperDivRef = React.useRef<HTMLDivElement>(null);
    const wrapperDivSize = useElementSize(wrapperDivRef);
    const vectorSpec = moduleContext.useStoreValue("vectorSpec");
    const resampleFrequency = moduleContext.useStoreValue("resamplingFrequency");
    const showStatistics = moduleContext.useStoreValue("showStatistics");
    const realizationsToInclude = moduleContext.useStoreValue("realizationsToInclude");
    const [highlightRealization, setHighlightRealization] = React.useState(-1);

    const vectorQuery = useVectorDataQuery(
        vectorSpec?.caseUuid,
        vectorSpec?.ensembleName,
        vectorSpec?.vectorName,
        resampleFrequency,
        realizationsToInclude
    );

    const statisticsQuery = useStatisticalVectorDataQuery(
        vectorSpec?.caseUuid,
        vectorSpec?.ensembleName,
        vectorSpec?.vectorName,
        resampleFrequency,
        realizationsToInclude,
        showStatistics
    );

    // React.useEffect(
    //     function subscribeToHoverRealizationTopic() {
    //         const unsubscribeFunc = workbenchServices.subscribe("global.hoverRealization", ({ realization }) => {
    //             setHighlightRealization(realization);
    //         });
    //         return unsubscribeFunc;
    //     },
    //     [workbenchServices]
    // );

    const subscribedPlotlyTimeStamp = useSubscribedValue("global.hoverTimestamp", workbenchServices);
    const subscribedPlotlyRealization = useSubscribedValue("global.hoverRealization", workbenchServices);
    // const highlightedTrace
    const handleHover = (e: PlotHoverEvent) => {
        if (e.xvals.length > 0 && typeof e.xvals[0]) {
            console.log("Value is: ", e.xvals[0]);
            console.log("index is: ", e.points[0].pointIndex);
            console.log("Indexed data is is: ", e.points[0].data.x[e.points[0].pointIndex]);
            workbenchServices.publishGlobalData("global.hoverTimestamp", { timestamp: e.xvals[0] as number });
        }
        const curveData = e.points[0].data as MyPlotData;
        if (typeof curveData.realizationNumber === "number") {
            // setHighlightRealization(curveData.realizationNumber);

            workbenchServices.publishGlobalData("global.hoverRealization", {
                realization: curveData.realizationNumber,
            });
        }
    };

    function handleUnHover() {
        workbenchServices.publishGlobalData("global.hoverRealization", { realization: -1 });
    }

    const handleSelection = (e: Readonly<Plotly.PlotMouseEvent>) => {
        console.log("handleSelection", e);

        const selectedItems: SelectItem[] = [];

        const curveData = e.points[0].data as MyPlotData;
        if (typeof curveData.realizationNumber === "number" && vectorSpec) {
            selectedItems.push({
                itemType: "realization",
                caseUuid: vectorSpec.caseUuid,
                ensembleName: vectorSpec.ensembleName,
                realization: curveData.realizationNumber,
            });
        }

        const xValFromPlot = e.points[0].x;
        if (typeof xValFromPlot === "string") {
            const timestampMs = isoStringToTimestampUtcMs(xValFromPlot);
            if (timestampMs > 0) {
                selectedItems.push({ itemType: "timestamp", timestampUtcMs: timestampMs });
            }
        }

        workbenchServices.getSelectionService().setSelection(selectedItems);
    };

    const tracesDataArr: MyPlotData[] = [];
    let unitString = "";

    if (vectorQuery.data && vectorQuery.data.length > 0) {
        let highlightedTrace: MyPlotData | null = null;
        unitString = vectorQuery.data[0].unit;
        for (let i = 0; i < vectorQuery.data.length; i++) {
            const vec = vectorQuery.data[i];
            const isHighlighted = vec.realization === subscribedPlotlyRealization?.realization ? true : false;
            const curveColor = vec.realization === subscribedPlotlyRealization?.realization ? "red" : "green";
            const lineWidth = vec.realization === subscribedPlotlyRealization?.realization ? 3 : 1;
            const lineShape = vec.is_rate ? "vh" : "linear";

            // const tsarr = new Float32Array(vec.timestamps.length);
            // for (let i = 0; i < vec.timestamps.length; i++) {
            //     tsarr[i] = isoStringToTimestampUtcMs(vec.timestamps[i]);
            // }

            const trace: MyPlotData = {
                x: vec.timestamps,
                y: vec.values,
                name: `real-${vec.realization}`,
                realizationNumber: vec.realization,
                legendrank: vec.realization,
                type: "scatter",
                mode: "lines",
                line: { color: curveColor, width: lineWidth, shape: lineShape },
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
        const lineShape = statisticsQuery.data.is_rate ? "vh" : "linear";
        for (const statValueObj of statisticsQuery.data.value_objects) {
            const trace: MyPlotData = {
                x: statisticsQuery.data.timestamps,
                y: statValueObj.values,
                name: statValueObj.statistic_function,
                legendrank: -1,
                type: "scatter",
                mode: "lines",
                line: { color: "lightblue", width: 2, dash: "dot", shape: lineShape },

            };
            tracesDataArr.push(trace);
        }
    }

    let title = "N/A";
    const hasGotAnyRequestedData = vectorQuery.data || (showStatistics && statisticsQuery.data);
    if (vectorSpec && hasGotAnyRequestedData) {
        title = `${vectorSpec.vectorName} [${unitString}] - ${vectorSpec.ensembleName}, ${vectorSpec.caseName}`;
    }

    const layout: Partial<Layout> = {
        width: wrapperDivSize.width,
        height: wrapperDivSize.height,
        title: title,
        xaxis: { type: 'date' }
    };

    if (subscribedPlotlyTimeStamp) {
        layout["shapes"] = [
            {
                type: "line",
                xref: "x",
                yref: "paper",
                x0: new Date(subscribedPlotlyTimeStamp.timestamp),
                y0: 0,
                x1: new Date(subscribedPlotlyTimeStamp.timestamp),
                y1: 1,
                line: {
                    color: "#ccc",
                    width: 1,
                },
            },
        ];
    }

    return (
        <div className="w-full h-full" ref={wrapperDivRef}>
            <Plot
                data={tracesDataArr}
                layout={layout}
                config={{ scrollZoom: true }}
                onHover={handleHover}
                onUnhover={handleUnHover}
                onClick={handleSelection}
            />
            <div className="absolute bottom-5 right-5 italic text-pink-400">{moduleContext.getInstanceIdString()}</div>
        </div>
    );
};
