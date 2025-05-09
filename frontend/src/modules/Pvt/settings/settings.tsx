import React from "react";

import { useAtom, useAtomValue, useSetAtom } from "jotai";

import { EnsembleSelect } from "@framework/components/EnsembleSelect";
import type { ModuleSettingsProps } from "@framework/Module";
import type { RegularEnsembleIdent } from "@framework/RegularEnsembleIdent";
import { useEnsembleRealizationFilterFunc, useEnsembleSet } from "@framework/WorkbenchSession";
import { CollapsibleGroup } from "@lib/components/CollapsibleGroup";
import { Dropdown } from "@lib/components/Dropdown";
import { PendingWrapper } from "@lib/components/PendingWrapper";
import { RadioGroup } from "@lib/components/RadioGroup";
import type { SelectOption } from "@lib/components/Select";
import { Select } from "@lib/components/Select";


import type { Interfaces } from "../interfaces";
import {
    ColorBy,
    PHASE_TO_DISPLAY_NAME,
    PRESSURE_DEPENDENT_VARIABLE_TO_DISPLAY_NAME,
    PhaseType,
    PressureDependentVariable,
} from "../typesAndEnums";
import { computeRealizationsIntersection } from "../utils/realizationsIntersection";

import {
    selectedColorByAtom,
    selectedDependentVariablesAtom,
    selectedPhaseAtom,
    userSelectedEnsembleIdentsAtom,
    userSelectedPvtNumsAtom,
    userSelectedRealizationsAtom,
} from "./atoms/baseAtoms";
import {
    pvtDataAccessorAtom,
    selectedEnsembleIdentsAtom,
    selectedPvtNumsAtom,
    selectedRealizationsAtom,
} from "./atoms/derivedAtoms";
import { pvtDataQueriesAtom } from "./atoms/queryAtoms";
import { DependentVariableSelector } from "./components/DependentVariableSelector/dependentVariableSelector";


export function Settings({ workbenchSession }: ModuleSettingsProps<Interfaces>) {
    const ensembleSet = useEnsembleSet(workbenchSession);
    const filterEnsembleRealizationsFunc = useEnsembleRealizationFilterFunc(workbenchSession);

    const selectedEnsembleIdents = useAtomValue(selectedEnsembleIdentsAtom);
    const selectedPvtNums = useAtomValue(selectedPvtNumsAtom);
    const pvtDataQueries = useAtomValue(pvtDataQueriesAtom);
    const pvtDataAccessor = useAtomValue(pvtDataAccessorAtom);
    const selectedRealizations = useAtomValue(selectedRealizationsAtom);
    const setSelectedEnsembleIdents = useSetAtom(userSelectedEnsembleIdentsAtom);
    const setSelectedRealizations = useSetAtom(userSelectedRealizationsAtom);
    const setSelectedPvtNums = useSetAtom(userSelectedPvtNumsAtom);

    const [selectedPhase, setSelectedPhase] = useAtom(selectedPhaseAtom);
    const [selectedColorBy, setSelectedColorBy] = useAtom(selectedColorByAtom);
    const [selectedDependentVariables, setSelectedPlots] = useAtom(selectedDependentVariablesAtom);

    const [selectedMultiEnsembleIdents, setSelectedMultiEnsembleIdents] =
        React.useState<RegularEnsembleIdent[]>(selectedEnsembleIdents);
    const [selectedMultiRealizations, setSelectedMultiRealizations] = React.useState<number[]>(selectedRealizations);
    const [selectedMultiPvtNums, setSelectedMultiPvtNums] = React.useState<number[]>(selectedPvtNums);

    function handleEnsembleSelectionChange(ensembleIdents: RegularEnsembleIdent[]) {
        setSelectedEnsembleIdents(ensembleIdents);
        setSelectedMultiEnsembleIdents(ensembleIdents);
    }

    function handleRealizationSelectionChange(values: string[]) {
        const newRealizations = values.map((value) => parseInt(value) as number);
        setSelectedRealizations(newRealizations);
        setSelectedMultiRealizations(newRealizations);
    }

    function handlePvtNumChange(values: string[]) {
        const newPvtNums = values.map((value) => parseInt(value) as number);
        setSelectedPvtNums(newPvtNums);
        setSelectedMultiPvtNums(newPvtNums);
    }

    function handleColorByChange(_: React.ChangeEvent<HTMLInputElement>, colorBy: ColorBy) {
        setSelectedColorBy(colorBy);
        if (colorBy === ColorBy.PVT_NUM) {
            setSelectedEnsembleIdents([selectedMultiEnsembleIdents[0]]);
            setSelectedRealizations([selectedMultiRealizations[0]]);
            setSelectedPvtNums(selectedMultiPvtNums);
        } else {
            setSelectedEnsembleIdents(selectedMultiEnsembleIdents);
            setSelectedRealizations(selectedMultiRealizations);
            setSelectedPvtNums([selectedMultiPvtNums[0]]);
        }
    }

    function handlePhasesChange(value: string) {
        setSelectedPhase(value as PhaseType);
    }

    function handleVisualizePlotsChange(plots: string[]) {
        const orderedPlots = [
            PressureDependentVariable.FORMATION_VOLUME_FACTOR,
            PressureDependentVariable.DENSITY,
            PressureDependentVariable.VISCOSITY,
            PressureDependentVariable.FLUID_RATIO,
        ];
        setSelectedPlots(orderedPlots.filter((plot) => plots.includes(plot)));
    }

    let errorMessage = "";
    if (pvtDataQueries.allQueriesFailed) {
        errorMessage = "Failed to fetch PVT data. Make sure the selected ensemble has PVT data.";
    }

    const realizations = computeRealizationsIntersection(selectedEnsembleIdents, filterEnsembleRealizationsFunc);

    return (
        <div className="flex flex-col gap-2">
            <CollapsibleGroup title="Color by" expanded>
                <RadioGroup
                    options={[
                        { label: "Ensemble", value: ColorBy.ENSEMBLE },
                        { label: "PVTNum", value: ColorBy.PVT_NUM },
                    ]}
                    value={selectedColorBy}
                    onChange={handleColorByChange}
                />
            </CollapsibleGroup>
            <CollapsibleGroup title="Ensembles" expanded>
                <EnsembleSelect
                    ensembles={ensembleSet.getRegularEnsembleArray()}
                    onChange={handleEnsembleSelectionChange}
                    value={selectedEnsembleIdents}
                    size={5}
                    multiple={selectedColorBy === ColorBy.ENSEMBLE}
                />
            </CollapsibleGroup>
            <CollapsibleGroup title="Realizations" expanded>
                <Select
                    options={makeRealizationOptions(realizations)}
                    value={selectedRealizations.map((el) => el.toString())}
                    onChange={handleRealizationSelectionChange}
                    size={5}
                    multiple={selectedColorBy === ColorBy.ENSEMBLE}
                />
            </CollapsibleGroup>
            <PendingWrapper isPending={pvtDataQueries.isFetching} errorMessage={errorMessage}>
                <CollapsibleGroup title="PVT Num" expanded>
                    <Select
                        options={makePvtNumOptions(pvtDataAccessor.getUniquePvtNums())}
                        value={selectedPvtNums.map((el) => el.toString())}
                        onChange={handlePvtNumChange}
                        size={5}
                        multiple={selectedColorBy === ColorBy.PVT_NUM}
                    />
                </CollapsibleGroup>
                <CollapsibleGroup title="Phase" expanded>
                    <Dropdown options={makePhaseOptions()} value={selectedPhase} onChange={handlePhasesChange} />
                </CollapsibleGroup>
                <CollapsibleGroup title="Show plot for" expanded>
                    <DependentVariableSelector
                        dependentVariables={makeDependentVariableOptions(selectedPhase)}
                        value={selectedDependentVariables}
                        onChange={handleVisualizePlotsChange}
                    />
                </CollapsibleGroup>
            </PendingWrapper>
        </div>
    );
}

function makePvtNumOptions(pvtNums: number[]): SelectOption[] {
    return pvtNums.map((pvtNum) => ({ label: pvtNum.toString(), value: pvtNum.toString() }));
}

function makePhaseOptions(): SelectOption[] {
    return Object.values(PhaseType).map((phase: PhaseType) => {
        return { value: phase, label: PHASE_TO_DISPLAY_NAME[phase] };
    });
}

function makeDependentVariableOptions(phaseType: PhaseType): PressureDependentVariable[] {
    const plots: PressureDependentVariable[] = [];

    for (const variable of Object.keys(PRESSURE_DEPENDENT_VARIABLE_TO_DISPLAY_NAME)) {
        if (variable === PressureDependentVariable.FLUID_RATIO && phaseType === PhaseType.WATER) {
            continue;
        }
        plots.push(variable as PressureDependentVariable);
    }

    return plots;
}

function makeRealizationOptions(realizations: number[]): SelectOption[] {
    return realizations.map((realization) => ({ label: realization.toString(), value: realization.toString() }));
}
