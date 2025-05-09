import { atom } from "jotai";

import { Frequency_api } from "@api";
import type { VectorSpec } from "@modules/SimulationTimeSeriesSensitivity/typesAndEnums";


export const vectorSpecificationAtom = atom<VectorSpec | null>(null);
export const resamplingFrequencyAtom = atom<Frequency_api | null>(Frequency_api.MONTHLY);
export const selectedSensitivityNamesAtom = atom<string[]>([]);
export const showStatisticsAtom = atom<boolean>(true);
export const showRealizationsAtom = atom<boolean>(false);
export const showHistoricalAtom = atom<boolean>(true);

export const userSelectedActiveTimestampUtcMsAtom = atom<number | null>(null);
