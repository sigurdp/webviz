import React from "react";

import type { GuiMessageBroker } from "@framework/GuiMessageBroker";
import { GuiState, useGuiValue } from "@framework/GuiMessageBroker";
import type { ModuleInstanceStatusControllerInternal } from "@framework/internal/ModuleInstanceStatusControllerInternal";
import { useStatusControllerStateValue } from "@framework/internal/ModuleInstanceStatusControllerInternal";
import { StatusSource } from "@framework/ModuleInstanceStatusController";
import { isDevMode } from "@lib/utils/devMode";

type DebugProfilerRenderInfoProps = {
    children: React.ReactNode | React.ReactNode[];
    title: string;
};

const DebugProfilerRenderInfo: React.FC<DebugProfilerRenderInfoProps> = (props) => {
    return (
        <span className="text-pink-300" title={props.title}>
            {props.children}
        </span>
    );
};

DebugProfilerRenderInfo.displayName = "DebugProfilerRenderInfo";

type DebugProfilerWrapperProps = {
    id: string;
    children: React.ReactNode;
    onRender: React.ProfilerOnRenderCallback;
};

const DebugProfilerWrapper = React.memo((props: DebugProfilerWrapperProps) => {
    return (
        <React.Profiler id={props.id} onRender={props.onRender}>
            {props.children}
        </React.Profiler>
    );
});

DebugProfilerWrapper.displayName = "DebugProfilerWrapper";

export type DebugProfilerProps = {
    id: string;
    children: React.ReactNode;
    statusController: ModuleInstanceStatusControllerInternal;
    source: StatusSource;
    guiMessageBroker: GuiMessageBroker;
};

/* 
* This helper type could be used to convert the React.ProfilerOnRenderCallback parameters tuple to an object with named properties.
* This would make it easier to automatically adjust to type changes in the React.ProfilerOnRenderCallback parameters tuple.
* However, the downside of this approach is that the TypeScript syntax is hard to read and understand.
* Therefore, we are not using this type for now and are instead manually specifying the types of the React.ProfilerOnRenderCallback parameters.
*
type TupleToObject<T extends readonly any[], M extends Record<Exclude<keyof T, keyof any[]>, PropertyKey>> = {
    [K in Exclude<keyof T, keyof any[]> as M[K]]: T[K];
};

type RenderInfo = Omit<
    TupleToObject<
        Parameters<React.ProfilerOnRenderCallback>,
        ["id", "phase", "actualDuration", "baseDuration", "startTime", "commitTime", "interactions"]
    >,
    "id"
> & {
    renderCount: number;
    minTime: number;
    maxTime: number;
    totalTime: number;
    avgTime: number;
};
*/

type RenderInfo = {
    phase: "mount" | "update" | "nested-update";
    actualDuration: number;
    baseDuration: number;
    startTime: number;
    commitTime: number;
    renderCount: number;
    minTime: number;
    maxTime: number;
    totalTime: number;
    avgTime: number;
};

export const DebugProfiler: React.FC<DebugProfilerProps> = (props) => {
    const [renderInfo, setRenderInfo] = React.useState<RenderInfo | null>(null);
    const reportedRenderCount = useStatusControllerStateValue(
        props.statusController,
        props.source === StatusSource.View ? "viewRenderCount" : "settingsRenderCount",
    );
    const customDebugMessage = useStatusControllerStateValue(
        props.statusController,
        props.source === StatusSource.View ? "viewDebugMessage" : "settingsDebugMessage",
    );
    const debugInfoVisible = useGuiValue(props.guiMessageBroker, GuiState.DevToolsVisible);

    const handleRender = React.useCallback(
        (
            ...[
                ,
                phase,
                actualDuration,
                baseDuration,
                startTime,
                commitTime,
            ]: Parameters<React.ProfilerOnRenderCallback>
        ) => {
            setRenderInfo((prev) => ({
                phase,
                actualDuration,
                baseDuration,
                startTime,
                commitTime,
                renderCount: (prev?.renderCount ?? 0) + 1,
                minTime: Math.min(prev?.minTime ?? actualDuration, actualDuration),
                maxTime: Math.max(prev?.maxTime ?? actualDuration, actualDuration),
                totalTime: (prev?.totalTime ?? 0) + actualDuration,
                avgTime: ((prev?.totalTime ?? 0) + actualDuration) / ((prev?.renderCount ?? 0) + 1),
            }));
        },
        [],
    );

    if (isDevMode()) {
        return (
            <>
                <DebugProfilerWrapper id={props.id} onRender={handleRender}>
                    {props.children}
                </DebugProfilerWrapper>
                {debugInfoVisible && (
                    <div className="absolute bottom-1 w-full flex gap-2 flex-wrap pointer-events-none">
                        {renderInfo && (
                            <>
                                {reportedRenderCount !== null && (
                                    <DebugProfilerRenderInfo title="Reported component render count">
                                        Component RC: {reportedRenderCount}
                                    </DebugProfilerRenderInfo>
                                )}
                                {customDebugMessage && (
                                    <DebugProfilerRenderInfo title="Custom debug message">
                                        Message: {customDebugMessage}
                                    </DebugProfilerRenderInfo>
                                )}
                                <DebugProfilerRenderInfo title="Tree render count">
                                    Tree RC: {renderInfo.renderCount}
                                </DebugProfilerRenderInfo>
                                <DebugProfilerRenderInfo title="Phase">P: {renderInfo.phase}</DebugProfilerRenderInfo>
                                <DebugProfilerRenderInfo
                                    title={
                                        "Actual duration: The number of milliseconds spent rendering the module and its descendants for the current update. " +
                                        "This indicates how well the subtree makes use of memoization (e.g. memo and useMemo). " +
                                        "Ideally this value should decrease significantly after the initial mount as many of the descendants will only " +
                                        "need to re-render if their specific props change."
                                    }
                                >
                                    AD: {renderInfo.actualDuration.toFixed(2)}ms
                                </DebugProfilerRenderInfo>
                                <DebugProfilerRenderInfo
                                    title={
                                        "Base Duration: The number of milliseconds estimating how much time it would take to re-render the entire module subtree without any optimizations. " +
                                        "It is calculated by summing up the most recent render durations of each component in the tree. " +
                                        "This value estimates a worst-case cost of rendering (e.g. the initial mount or a tree with no memoization). " +
                                        "Compare actualDuration against it to see if memoization is working."
                                    }
                                >
                                    BD: {renderInfo.baseDuration.toFixed(2)}ms
                                </DebugProfilerRenderInfo>
                                <DebugProfilerRenderInfo title="The number of milliseconds of the fastest render duration.">
                                    MIN: {renderInfo.minTime.toFixed(2)}ms
                                </DebugProfilerRenderInfo>
                                <DebugProfilerRenderInfo title="The number of milliseconds of the slowest render duration.">
                                    MAX: {renderInfo.maxTime.toFixed(2)}ms
                                </DebugProfilerRenderInfo>
                                <DebugProfilerRenderInfo title="The number of milliseconds of the average render duration.">
                                    AVG: {renderInfo.avgTime.toFixed(2)}ms
                                </DebugProfilerRenderInfo>
                            </>
                        )}
                    </div>
                )}
            </>
        );
    }

    return <>{props.children}</>;
};

DebugProfiler.displayName = "DebugProfiler";
