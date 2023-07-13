import { Frequency_api, VectorDescription_api } from "@api";
import { VectorRealizationData_api, VectorStatisticData_api } from "@api";
import { apiService } from "@framework/ApiService";
import { UseQueryResult, useQuery } from "@tanstack/react-query";

import { timestampUtcMsToIsoString, isoStringToTimestampUtcMs, isoStringArrayToTimestampUtcMs } from "@framework/utils/TimestampUtils";


const STALE_TIME = 60 * 1000;
const CACHE_TIME = 60 * 1000;

export function useVectorsQuery(
    caseUuid: string | undefined,
    ensembleName: string | undefined
): UseQueryResult<Array<VectorDescription_api>> {
    return useQuery({
        queryKey: ["getVectorNamesAndDescriptions", caseUuid, ensembleName],
        queryFn: () => apiService.timeseries.getVectorNamesAndDescriptions(caseUuid ?? "", ensembleName ?? ""),
        staleTime: STALE_TIME,
        cacheTime: CACHE_TIME,
        enabled: caseUuid && ensembleName ? true : false,
    });
}

interface VectorRealizationDataSig extends VectorRealizationData_api {
    timestampMsArr: Float64Array;
    valuesArr: Float32Array;
}

function transformTimeStampsAndValues(data: VectorRealizationData_api[]): VectorRealizationDataSig[] {
    console.log("transformTimeStamps");
    return data.map((vec) => {
        return {
            ...vec,
            timestampMsArr: isoStringArrayToTimestampUtcMs(vec.timestamps),
            valuesArr: new Float32Array(vec.values),
        };
    });
}

export function useVectorDataQuery(
    caseUuid: string | undefined,
    ensembleName: string | undefined,
    vectorName: string | undefined,
    resampleFrequency: Frequency_api | null,
    realizationsToInclude: number[] | null
): UseQueryResult<Array<VectorRealizationDataSig>> {
    const allOrNonEmptyRealArr = realizationsToInclude === null || realizationsToInclude.length > 0 ? true : false;
    return useQuery({
        queryKey: [
            "getRealizationsVectorData",
            caseUuid,
            ensembleName,
            vectorName,
            resampleFrequency,
            realizationsToInclude,
        ],
        queryFn: () =>
            apiService.timeseries.getRealizationsVectorData(
                caseUuid ?? "",
                ensembleName ?? "",
                vectorName ?? "",
                resampleFrequency ?? undefined,
                realizationsToInclude ?? undefined
            ),
        select: transformTimeStampsAndValues,
        staleTime: STALE_TIME,
        cacheTime: CACHE_TIME,
        enabled: caseUuid && ensembleName && vectorName && allOrNonEmptyRealArr ? true : false,
    });
}

export function useStatisticalVectorDataQuery(
    caseUuid: string | undefined,
    ensembleName: string | undefined,
    vectorName: string | undefined,
    resampleFrequency: Frequency_api | null,
    realizationsToInclude: number[] | null,
    allowEnable: boolean
): UseQueryResult<VectorStatisticData_api> {
    const allOrNonEmptyRealArr = realizationsToInclude === null || realizationsToInclude.length > 0 ? true : false;
    return useQuery({
        queryKey: [
            "getStatisticalVectorData",
            caseUuid,
            ensembleName,
            vectorName,
            resampleFrequency,
            realizationsToInclude,
        ],
        queryFn: () =>
            apiService.timeseries.getStatisticalVectorData(
                caseUuid ?? "",
                ensembleName ?? "",
                vectorName ?? "",
                resampleFrequency ?? Frequency_api.MONTHLY,
                undefined,
                realizationsToInclude ?? undefined
            ),
        staleTime: STALE_TIME,
        cacheTime: CACHE_TIME,
        enabled:
            allowEnable && caseUuid && ensembleName && vectorName && resampleFrequency && allOrNonEmptyRealArr
                ? true
                : false,
    });
}
