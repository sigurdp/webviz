import React from "react";

import type { UseQueryResult, DefaultError } from "@tanstack/react-query";
import { useQuery } from "@tanstack/react-query";

import type { TaskProgress_api, SurfaceDataPng_api, SurfaceDef_api, SurfaceMetaSet_api } from "@api";
import { getCeleryPollingSurfaceDataOptions, getObservedSurfacesMetadataOptions, getRealizationSurfacesMetadataOptions, getSurfaceDataOptions } from "@api";
import { encodePropertiesAsKeyValStr } from "@lib/utils/queryStringUtils";

import type { SurfaceDataFloat_trans } from "./queryDataTransforms";
import { transformSurfaceData } from "./queryDataTransforms";
import { type FullSurfaceAddress, encodeSurfAddrStr, peekSurfaceAddressType } from "./surfaceAddress";
import type { SurfaceDataFloat_api } from "@api";

import { GetSigurdExperimentResponse_api, getSigurdExperimentOptions } from "@api";


export function useRealizationSurfacesMetadataQuery(
    caseUuid: string | undefined,
    ensembleName: string | undefined,
): UseQueryResult<SurfaceMetaSet_api> {
    return useQuery({
        ...getRealizationSurfacesMetadataOptions({
            query: {
                case_uuid: caseUuid ?? "",
                ensemble_name: ensembleName ?? "",
            },
        }),
        enabled: Boolean(caseUuid && ensembleName),
    });
}

export function useObservedSurfacesMetadataQuery(caseUuid: string | undefined): UseQueryResult<SurfaceMetaSet_api> {
    return useQuery({
        ...getObservedSurfacesMetadataOptions({
            query: {
                case_uuid: caseUuid ?? "",
            },
        }),
        enabled: Boolean(caseUuid),
    });
}

export function useSurfaceDataQuery(surfAddrStr: string | null, format: "float", resampleTo: SurfaceDef_api | null, allowEnable: boolean): UseQueryResult<SurfaceDataFloat_trans>; // prettier-ignore
export function useSurfaceDataQuery(surfAddrStr: string | null, format: "png", resampleTo: SurfaceDef_api | null, allowEnable: boolean): UseQueryResult<SurfaceDataPng_api>; // prettier-ignore
export function useSurfaceDataQuery(surfAddrStr: string | null, format: "float" | "png", resampleTo: SurfaceDef_api | null, allowEnable: boolean): UseQueryResult<SurfaceDataFloat_trans | SurfaceDataPng_api>; // prettier-ignore
export function useSurfaceDataQuery(
    surfAddrStr: string | null,
    format: "float" | "png",
    resampleTo: SurfaceDef_api | null,
    allowEnable: boolean,
): UseQueryResult<SurfaceDataFloat_trans | SurfaceDataPng_api> {
    if (surfAddrStr) {
        const surfAddrType = peekSurfaceAddressType(surfAddrStr);
        if (surfAddrType !== "OBS" && surfAddrType !== "REAL" && surfAddrType !== "STAT") {
            throw new Error("Invalid surface address type for surface data query");
        }
    }

    let resampleToKeyValStr: string | null = null;
    if (resampleTo) {
        resampleToKeyValStr = encodePropertiesAsKeyValStr(resampleTo);
    }

    return useQuery({
        ...getSurfaceDataOptions({
            query: {
                surf_addr_str: surfAddrStr ?? "",
                data_format: format,
                resample_to_def_str: resampleToKeyValStr,
            },
        }),
        select: transformSurfaceData,
        enabled: Boolean(allowEnable && surfAddrStr),
    });
}

export function useSurfaceDataQueryByAddress(surfAddr: FullSurfaceAddress | null, format: "float", resampleTo: SurfaceDef_api | null, allowEnable: boolean): UseQueryResult<SurfaceDataFloat_trans>; // prettier-ignore
export function useSurfaceDataQueryByAddress(surfAddr: FullSurfaceAddress | null, format: "png", resampleTo: SurfaceDef_api | null, allowEnable: boolean): UseQueryResult<SurfaceDataPng_api>; // prettier-ignore
export function useSurfaceDataQueryByAddress(
    surfAddr: FullSurfaceAddress | null,
    format: "float" | "png",
    resampleTo: SurfaceDef_api | null,
    allowEnable: boolean,
): UseQueryResult<SurfaceDataFloat_trans | SurfaceDataPng_api> {
    const surfAddrStr = surfAddr ? encodeSurfAddrStr(surfAddr) : null;
    return useSurfaceDataQuery(surfAddrStr, format, resampleTo, allowEnable);
}




export type UseQueryResultWithProgress<TData = unknown, TError = DefaultError> = UseQueryResult<TData, TError> & {
  progressMsg: string | null;
};


// export function useCelerySurfaceDataQueryByAddress(
//     surfAddr: FullSurfaceAddress | null,
//     allowEnable: boolean,
// ): UseQueryResult<SurfaceDataFloat_trans | SurfaceDataPng_api> {
//     const surfAddrStr = surfAddr ? encodeSurfAddrStr(surfAddr) : null;
//     if (surfAddrStr) {
//         const surfAddrType = peekSurfaceAddressType(surfAddrStr);
//         if (surfAddrType !== "OBS" && surfAddrType !== "REAL" && surfAddrType !== "STAT") {
//             throw new Error("Invalid surface address type for surface data query");
//         }
//     }

//     return useQuery({
//         ...getCelerySurfaceDataOptions({
//             query: {
//                 surf_addr_str: surfAddrStr ?? ""
//             },
//         }),
//         select: transformSurfaceData,
//         enabled: Boolean(allowEnable && surfAddrStr),
//     });
// }


export function useCeleryPollingSurfaceDataQueryByAddress(
    surfAddr: FullSurfaceAddress | null,
    allowEnable: boolean,
): UseQueryResultWithProgress<SurfaceDataFloat_trans | TaskProgress_api> {
    //const [progressMsg, setProgressMsg] = React.useState<string | null>(null);

    const surfAddrStr = surfAddr ? encodeSurfAddrStr(surfAddr) : null;
    if (surfAddrStr) {
        const surfAddrType = peekSurfaceAddressType(surfAddrStr);
        if (surfAddrType !== "OBS" && surfAddrType !== "REAL" && surfAddrType !== "STAT") {
            throw new Error("Invalid surface address type for surface data query");
        }
    }

    const result = useQuery({

        ...getCeleryPollingSurfaceDataOptions({
            query: {
                surf_addr_str: surfAddrStr ?? ""
            },
        }),

        refetchInterval: (query) => { if (query.state?.data && "progress" in query.state?.data) return 200; return false; },

        select: (data) => {
            if ("values_b64arr" in data) {
                return transformSurfaceData(data) as SurfaceDataFloat_trans; 
            }
            else {
                return data;
            }
        },

        enabled: Boolean(allowEnable && surfAddrStr),
    });

    const progressMsg = (result.data && "progress" in result.data) ? result.data.progress : "NOPE";

    return {
        ...result,
        progressMsg: progressMsg,
    };
}



export function useSigurdExperiment(
    allowEnable: boolean,
): UseQueryResult<GetSigurdExperimentResponse_api> {

    const result = useQuery({

        ...getSigurdExperimentOptions(),

        refetchInterval: (query) => { if (query.state?.data?.status === "inProgress") return 200; return false; },

        // select: (data) => {
        //     if ("values_b64arr" in data) {
        //         return transformSurfaceData(data) as SurfaceDataFloat_trans; 
        //     }
        //     else {
        //         return data;
        //     }
        // },

        // enabled: Boolean(allowEnable && surfAddrStr),
    });

    const progressMsg = (result.data && "progress" in result.data) ? result.data.progress : "NOPE";

    return {
        ...result,
    };
}

