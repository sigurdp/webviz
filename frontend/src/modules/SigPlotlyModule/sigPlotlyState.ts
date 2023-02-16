import { Frequency } from "@api";

export interface SigPlotlyState {
    ensembleName: string | null;
    vectorName: string | null;
    resamplingFrequency: Frequency | null;
    showStatistics: boolean;
    realizationsToInclude: number[] | null;
}
