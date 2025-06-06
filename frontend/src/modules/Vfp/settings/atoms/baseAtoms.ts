import { atom } from "jotai";

import type { RegularEnsembleIdent } from "@framework/RegularEnsembleIdent";
import { atomWithCompare } from "@framework/utils/atomUtils";
import { areEnsembleIdentsEqual } from "@framework/utils/ensembleIdentUtils";
import type { PressureOption, VfpParam } from "@modules/Vfp/types";


export const userSelectedRealizationNumberAtom = atom<number | null>(null);

export const validRealizationNumbersAtom = atom<number[] | null>(null);

export const userSelectedEnsembleIdentAtom = atomWithCompare<RegularEnsembleIdent | null>(null, areEnsembleIdentsEqual);

export const userSelectedVfpTableNameAtom = atom<string | null>(null);

export const userSelectedThpIndicesAtom = atom<number[] | null>(null);

export const userSelectedWfrIndicesAtom = atom<number[] | null>(null);

export const userSelectedGfrIndicesAtom = atom<number[] | null>(null);

export const userSelectedAlqIndicesAtom = atom<number[] | null>(null);

export const userSelectedPressureOptionAtom = atom<PressureOption | null>(null);

export const userSelectedColorByAtom = atom<VfpParam | null>(null);
