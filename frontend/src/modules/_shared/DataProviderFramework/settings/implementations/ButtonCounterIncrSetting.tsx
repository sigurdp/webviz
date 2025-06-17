import type React from "react";
import type {
    CustomSettingImplementation,
    SettingComponentProps,
} from "../../interfacesAndTypes/customSettingImplementation";
import type { SettingCategory } from "../settingsDefinitions";
import { Button } from "@lib/components/Button";

type ValueType = number;
export class ButtonCounterIncrSetting implements CustomSettingImplementation<ValueType, SettingCategory.NUMBER> {
    private label: string;
    constructor(label?: string) {
        this.label = label ?? "Increment";
    }

    isValueValid(value: ValueType): boolean {
        return typeof value === "number";
    }

    getIsStatic(): boolean {
        return true;
    }

    makeComponent(): (props: SettingComponentProps<ValueType, SettingCategory.NUMBER>) => React.ReactNode {
        const label = this.label;
        return function ButtonCounter(props: SettingComponentProps<ValueType, SettingCategory.NUMBER>) {
            function handleChange() {
                const currentValue = props.value ?? 0;
                props.onValueChange(currentValue + 1);
            }

            return (
                <Button variant="contained" onClick={handleChange}>
                    {label}
                </Button>
            );
        };
    }
}
