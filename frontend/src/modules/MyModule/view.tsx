import React from "react";

import type { Options } from "@hey-api/client-axios";
import { useQuery, useQueryClient } from "@tanstack/react-query";

//import { postConcatenateOptions, postConcatenate, postConcatenateQueryKey, type PostConcatenateData_api, type ProgressInfo_api } from "@api";
import { postGetTbSampleSurfInPoints, PostGetTbSampleSurfInPointsData_api, postGetTbSampleSurfInPointsQueryKey, type ProgressInfo_api } from "@api";
import { postTbSampleSurfInPointsSubmit, PostTbSampleSurfInPointsSubmitData_api } from "@api";
import { postTbSampleSurfInPointsSubmitOptions, postTbSampleSurfInPointsSubmitQueryKey } from "@api";

import { wrapLongRunningQuery } from "@framework/utils/longRunningApiCalls";
import { Button } from "@lib/components/Button";
import { CircularProgress } from "@lib/components/CircularProgress";
import { Input } from "@lib/components/Input";
import { Switch } from "@lib/components/Switch";

const SAMPLE_POINTS = {
    x_points:[461980,461990,462000,462010,462020,462030,462040,462050,462060,462070,462080,462090,462100,462110,462120,462130,462140,462150,462160,462170,462180,462190,462200,462210,462220,462230,462240,462250,462260,462270,462280,462290,462300,462310,462320,462330,462340,462350,462360,462370,462380,462390,462400,462410,462420,462430,462440,462450,462460,462470,462480,462490,462500,462510,462520,462530,462540,462550,462560,462570,462580,462590,462600,462610,462620,462630,462640,462650,462660,462670,462680,462690,462700,462710,462720,462730,462740,462750,462760,462770,462780,462790,462800,462810,462820,462830,462840,462850,462860,462870,462880,462890,462900,462910,462920,462930,462940,462950,462960,462970,462980],
    y_points:[5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232,5934232]
}

// const SAMPLE_POINTS = {
//     x_points: [461980, 461990, 462000, 462010, 462020],
//     y_points: [5934232, 5934232, 5934232, 5934232, 5934232],
// };

const CASE_UUID = "aea92953-b5a3-49c6-9119-5ab34dd10bc4";
const ENSEMBLE_NAME = "iter-0";
const SURFACE_NAME = "Therys Fm. Top";
const SURFACE_ATTRIBUTE = "DS_extract_geogrid";
const REALIZATION_NUMS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20];

export function View(): React.ReactNode {
    const [progress, setProgress] = React.useState<ProgressInfo_api | undefined>(undefined);
    const [numPoints, setNumPoints] = React.useState<number>(3);
    const [delay, setDelay] = React.useState<number>(10);
    const [fail, setFail] = React.useState<boolean>(false);
    const [options, setOptions] = React.useState<Options<PostTbSampleSurfInPointsSubmitData_api | PostGetTbSampleSurfInPointsData_api, false>>({
        query: {
            case_uuid: CASE_UUID,
            ensemble_name: ENSEMBLE_NAME,
            surface_name: SURFACE_NAME,
            surface_attribute: SURFACE_ATTRIBUTE,
            realization_nums: REALIZATION_NUMS,
        },
        body: {
            sample_points: {
                x_points: SAMPLE_POINTS.x_points.slice(0, numPoints),
                y_points: SAMPLE_POINTS.y_points.slice(0, numPoints),
            },
        },
    });
    const [queryKey, setQueryKey] = React.useState<unknown[]>(postTbSampleSurfInPointsSubmitQueryKey(options));
    //const [queryKey, setQueryKey] = React.useState<unknown[]>(postGetTbSampleSurfInPointsQueryKey(options));

    const wrapped = wrapLongRunningQuery({
        queryFn: postTbSampleSurfInPointsSubmit,
        //queryFn: postGetTbSampleSurfInPoints,
        queryFnArgs: options,
        queryKey,
        pollIntervalMs: 200,
        maxRetries: 10,
        onProgress: (progress) => {
            setProgress(progress);
        },
    });

    const queryClient = useQueryClient();
    const result = useQuery({ ...wrapped });

    function handleInputChange(event: React.ChangeEvent<HTMLInputElement>) {
        const value = event.target.value;
        const newDelay = value ? parseInt(value, 10) : 10;
        setDelay(newDelay);
    }

    function handleButtonClick() {
        queryClient.invalidateQueries({
            queryKey,
            refetchType: "all",
        });
        const newOptions: Options<PostTbSampleSurfInPointsSubmitData_api | PostGetTbSampleSurfInPointsData_api, false> = {
            query: {
                case_uuid: CASE_UUID,
                ensemble_name: ENSEMBLE_NAME,
                surface_name: SURFACE_NAME,
                surface_attribute: SURFACE_ATTRIBUTE,
                realization_nums: REALIZATION_NUMS,
                cache_busting: new Date().toISOString(),
            },
            body: {
                sample_points: {
                    x_points: SAMPLE_POINTS.x_points.slice(0, numPoints),
                    y_points: SAMPLE_POINTS.y_points.slice(0, numPoints),
                },
            },
        };
        setQueryKey(postTbSampleSurfInPointsSubmitQueryKey(newOptions));
        //setQueryKey(postGetTbSampleSurfInPointsQueryKey(newOptions));
        setOptions(newOptions);
    }

    return (
        <div className="w-full h-full flex flex-col gap-2">
            <h2 className="font-bold">Request</h2>
            <table>
                <tbody>
                    <tr>
                        <td>numPoints:</td>
                        <td>
                            <Input type="number" defaultValue={numPoints} onChange={(e) => setNumPoints(Number(e.target.value))} />
                        </td>
                    </tr>
                    <tr>
                        <td>Delay:</td>
                        <td>
                            <Input type="number" defaultValue={delay} onChange={handleInputChange} endAdornment="s" />
                        </td>
                    </tr>
                    <tr>
                        <td>Let task fail:</td>
                        <td>
                            <Switch checked={fail} onChange={(e) => setFail(e.target.checked)} />
                        </td>
                    </tr>
                </tbody>
            </table>
            <Button onClick={handleButtonClick} variant="contained">
                Start
            </Button>
            <h2 className="mt-4 font-bold">Response</h2>
            <table>
                <tbody>
                    <tr>
                        <td>Loading:</td>
                        <td>{result.isFetching ? <CircularProgress /> : null}</td>
                    </tr>
                    <tr>
                        <td>Progress:</td>
                        <td>{!result.isFetching ? "" : progress?.progress_message}</td>
                    </tr>
                    <tr>
                        <td>Error:</td>
                        <td className="text-red-600">{result.error ? result.error.message : null}</td>
                    </tr>
                    <tr>
                        <td>Data:</td>
                        <td>{result.data && !result.isFetching ? JSON.stringify(result.data) : "No data"}</td>
                    </tr>
                </tbody>
            </table>
        </div>
    );
}
