import React from "react";
import Plot from "react-plotly.js";

import { BroadcastChannelMeta } from "@framework/Broadcaster";
import { ModuleFCProps } from "@framework/Module";
import { useSubscribedValue } from "@framework/WorkbenchServices";
import { useElementSize } from "@lib/hooks/useElementSize";

import { Layout, PlotData, PlotHoverEvent, PlotDatum } from "plotly.js";

import { BroadcastChannelNames } from "./channelDefs";
import { useStatisticalVectorDataQuery, useVectorDataQuery } from "./queryHooks";
import { State } from "./state";
import { timestampUtcMsToIsoString, isoStringToTimestampUtcMs, isoStringArrayToTimestampUtcMs } from "@framework/utils/TimestampUtils";

interface MyPlotData extends Partial<PlotData> {
    realizationNumber?: number | null;

    // Did they forget to expose this one
    legendrank?: number;
}

export const view = ({ moduleContext, workbenchSession, workbenchServices }: ModuleFCProps<State>) => {
    const renderCount = React.useRef(0);
    React.useEffect(function incrementRenderCount() {
        renderCount.current = renderCount.current + 1;
    });

    const wrapperDivRef = React.useRef<HTMLDivElement>(null);
    const wrapperDivSize = useElementSize(wrapperDivRef);
    const vectorSpec = moduleContext.useStoreValue("vectorSpec");
    const resampleFrequency = moduleContext.useStoreValue("resamplingFrequency");
    //const showStatistics = moduleContext.useStoreValue("showStatistics");
    const showStatistics = false;
    const realizationsToInclude = moduleContext.useStoreValue("realizationsToInclude");


    const myInstanceIdStr = moduleContext.getInstanceIdString();
    console.debug(`${myInstanceIdStr} -- render SimulationTimeSeries view, count=${renderCount.current}`);


    const vectorQuery = useVectorDataQuery(
        vectorSpec?.ensembleIdent.getCaseUuid(),
        vectorSpec?.ensembleIdent.getEnsembleName(),
        vectorSpec?.vectorName,
        resampleFrequency,
        realizationsToInclude
    );

    const statisticsQuery = useStatisticalVectorDataQuery(
        vectorSpec?.ensembleIdent.getCaseUuid(),
        vectorSpec?.ensembleIdent.getEnsembleName(),
        vectorSpec?.vectorName,
        resampleFrequency,
        realizationsToInclude,
        showStatistics
    );

    const ensembleSet = workbenchSession.getEnsembleSet();
    const ensemble = vectorSpec ? ensembleSet.findEnsemble(vectorSpec.ensembleIdent) : null;

    /*
    React.useEffect(
        function broadcast() {
            if (!ensemble) {
                return;
            }

            const dataGenerator = (): { key: number; value: number }[] => {
                const data: { key: number; value: number }[] = [];
                if (vectorQuery.data) {
                    vectorQuery.data.forEach((vec) => {
                        data.push({
                            key: vec.realization,
                            value: vec.values[0],
                        });
                    });
                }
                return data;
            };

            const channelMeta: BroadcastChannelMeta = {
                ensembleIdent: ensemble.getIdent(),
                description: `${ensemble.getDisplayName()} ${vectorSpec?.vectorName}`,
                unit: vectorQuery.data?.at(0)?.unit || "",
            };

            moduleContext.getChannel(BroadcastChannelNames.Realization_Value).broadcast(channelMeta, dataGenerator);
        },
        [vectorQuery.data, ensemble, vectorSpec, moduleContext]
    );
    */

    // React.useEffect(
    //     function subscribeToHoverRealizationTopic() {
    //         const unsubscribeFunc = workbenchServices.subscribe("global.hoverRealization", ({ realization }) => {
    //             setHighlightRealization(realization);
    //         });
    //         return unsubscribeFunc;
    //     },
    //     [workbenchServices]
    // );

    const subscribedHoverTimestamp = useSubscribedValue("global.hoverTimestamp", workbenchServices);
    const subscribedHoverRealization = useSubscribedValue("global.hoverRealization", workbenchServices);
    
    function handleHover(e: PlotHoverEvent) {
        const plotDatum: PlotDatum = e.points[0];

        console.debug(`${myInstanceIdStr} -- hover plotDatum.pointIndex=${plotDatum.pointIndex}`);

        if (plotDatum.data.x instanceof Float64Array && plotDatum.pointIndex < plotDatum.data.x.length) {
            // const xValue = plotDatum.data.x[plotDatum.pointIndex];
            // const timestampUtcMs = isoStringToTimestampUtcMs(xValue as string);
            const timestampUtcMs = plotDatum.data.x[plotDatum.pointIndex];
            workbenchServices.publishGlobalData("global.hoverTimestamp", { timestamp: timestampUtcMs });
        }

        const curveData = plotDatum.data as MyPlotData;
        if (typeof curveData.realizationNumber === "number") {
            // setHighlightRealization(curveData.realizationNumber);

            workbenchServices.publishGlobalData("global.hoverRealization", {
                realization: curveData.realizationNumber,
            });
        }
    };

    function handleUnHover() {
        console.debug(`${myInstanceIdStr} -- UNhover`);
        workbenchServices.publishGlobalData("global.hoverRealization", { realization: -1 });
    }


    
    const memoizedTracesDataArr: MyPlotData[] = React.useMemo(() => {
        const tracesArr: MyPlotData[] = [];
        if (vectorQuery.data && vectorQuery.data.length > 0) {
            for (let i = 0; i < vectorQuery.data.length; i++) {
                const vec = vectorQuery.data[i];
                const curveColor = "rgba(0, 155, 0, 0.7)";//"green";
                const lineWidth = 0.5;
                const lineShape = vec.is_rate ? "vh" : "linear";
                const trace: MyPlotData = {
                    x: vec.timestampMsArr,
                    y: vec.valuesArr,
                    name: `real-${vec.realization}`,
                    realizationNumber: vec.realization,
                    legendrank: vec.realization,
                    type: "scatter",
                    mode: "lines",
                    line: { color: curveColor, width: lineWidth, shape: lineShape },
                    //hoverinfo: "none",
                };

                tracesArr.push(trace);
            };
        }

        return tracesArr;
    }, [vectorQuery.data]);


    //const tracesDataArr: MyPlotData[] = memoizedTracesDataArr;

    const tracesDataArr: MyPlotData[] = React.useMemo(() => {
        if (!subscribedHoverRealization || subscribedHoverRealization.realization < 0) {
            return memoizedTracesDataArr;
        }

        const retTracesDataArr: MyPlotData[] = [];
        let highlightedTrace: MyPlotData | null = null;
        for (const trace of memoizedTracesDataArr) {
            if (trace.realizationNumber !== subscribedHoverRealization.realization) {
                retTracesDataArr.push(trace);
            }
            else {
                //highlightedTrace = {...trace, line: {...trace.line, color: "blue", width: 3}};
                highlightedTrace = {...trace, line: {...trace.line, color: "rgba(155, 0, 0, 0.99)", width: 1.5}};
                retTracesDataArr.push(highlightedTrace);
            }
        }

        // if (highlightedTrace) {
        //     retTracesDataArr.push(highlightedTrace);
        // }

        return retTracesDataArr;
    }, [memoizedTracesDataArr, subscribedHoverRealization?.realization]);


    /*
    const tracesDataArr: MyPlotData[] = [];

    if (vectorQuery.data && vectorQuery.data.length > 0) {
        let highlightedTrace: MyPlotData | null = null;
        for (let i = 0; i < vectorQuery.data.length; i++) {
            const vec = vectorQuery.data[i];
            // const isHighlighted = vec.realization === subscribedHoverRealization?.realization ? true : false;
            //const curveColor = vec.realization === subscribedHoverRealization?.realization ? "red" : "green";
            //const lineWidth = vec.realization === subscribedHoverRealization?.realization ? 3 : 1;
            const isHighlighted = false;
            const curveColor = "green";
            const lineWidth = 1;
            const lineShape = vec.is_rate ? "vh" : "linear";
            const trace: MyPlotData = {
                //x: isoStringArrayToTimestampUtcMs(vec.timestamps),
                x: vec.timestampMsArr,
                //y: vec.values,
                y: vec.valuesArr,
                name: `real-${vec.realization}`,
                realizationNumber: vec.realization,
                legendrank: vec.realization,
                type: "scatter",
                mode: "lines",
                line: { color: curveColor, width: lineWidth, shape: lineShape },
                //hoverinfo: "none",
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
    */

    if (showStatistics && statisticsQuery.data) {
        const lineShape = statisticsQuery.data.is_rate ? "vh" : "linear";
        for (const statValueObj of statisticsQuery.data.value_objects) {
            const trace: MyPlotData = {
                x: isoStringArrayToTimestampUtcMs(statisticsQuery.data.timestamps),
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

    /*
    React.useEffect(
        function updateInstanceTitle() {
            const hasGotAnyRequestedData = vectorQuery.data || (showStatistics && statisticsQuery.data);
            if (ensemble && vectorSpec && hasGotAnyRequestedData) {
                const ensembleDisplayName = ensemble.getDisplayName();
                moduleContext.setInstanceTitle(`${ensembleDisplayName} - ${vectorSpec.vectorName}`);
            }
        },
        [ensemble, vectorSpec, vectorQuery.data, showStatistics, statisticsQuery.data, moduleContext]
    );
    */

    console.debug(`${myInstanceIdStr} -- subscribedHoverTimestamp=${subscribedHoverTimestamp?.timestamp ?? "null"}}`);
    console.debug(`${myInstanceIdStr} -- subscribedHoverRealization=${subscribedHoverRealization?.realization ?? "null"}}`);

    const layout: Partial<Layout> = React.useMemo(() => {
        const retLayout: Partial<Layout> = {
            width: wrapperDivSize.width,
            height: wrapperDivSize.height,
            margin: { t: 0, r: 0, l: 40, b: 40 },
            xaxis: { type: "date" },
            // xaxis: { range:["2018-01-01T00:00:00", "2020-07-01T00:00:00"], type: "date" },
            // yaxis: { range: [5*1e9, 8*1e9], type: "linear" },
            //showlegend: false
        };

        if (subscribedHoverTimestamp) {
            retLayout.shapes = [
                {
                    type: "line",
                    xref: "x",
                    yref: "paper",
                    x0: subscribedHoverTimestamp.timestamp,
                    y0: 0,
                    x1: subscribedHoverTimestamp.timestamp,
                    y1: 1,
                    line: {
                        color: "#ccc",
                        width: 1,
                    },
                },
            ];
        }

        return retLayout

    }, [wrapperDivSize.width, wrapperDivSize.height, subscribedHoverTimestamp?.timestamp]);


    /*
    const layout: Partial<Layout> = {
        width: wrapperDivSize.width,
        height: wrapperDivSize.height,
        margin: { t: 0, r: 0, l: 40, b: 40 },
        xaxis: { type: "date" },
    };

    if (subscribedHoverTimestamp) {
        layout.shapes = [
            {
                type: "line",
                xref: "x",
                yref: "paper",
                x0: subscribedHoverTimestamp.timestamp,
                y0: 0,
                x1: subscribedHoverTimestamp.timestamp,
                y1: 1,
                line: {
                    color: "#ccc",
                    width: 1,
                },
            },
        ];
    }
    */

    return (
        <>
        <div>({renderCount.current})</div>
        <div className="w-full h-full" ref={wrapperDivRef}>
            <Plot
                data={tracesDataArr}
                layout={layout}
                //config={{ scrollZoom: true }}
                onHover={handleHover}
                onUnhover={handleUnHover}
            />
        </div>
        </>
    );
};
