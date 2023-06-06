import { WorkbenchServices } from "./WorkbenchServices";


export type EnsembleItem = {
    itemType: "ensemble";
    caseUuid: string;
    ensembleName: string;
};

export type RealizationItem = {
    itemType: "realization";
    caseUuid: string;
    ensembleName: string;
    realization: number;
};

export type TimestampItem = {
    itemType: "timestamp";
    timestampUtcMs: number;
};

export type SelectItem = EnsembleItem | RealizationItem | TimestampItem;


export class SelectionService {
    private _workbenchServices: WorkbenchServices;
    private _itemArr: SelectItem[];
    private _subscribersSet: Set<() => void>;
 
    constructor(workbenchServices: WorkbenchServices) {
        this._workbenchServices = workbenchServices;
        this._itemArr = [];
        this._subscribersSet = new Set();
    }

    clearSelection() {
        this._itemArr = [];
        this.notifySubscribersThatSelectionChanged();
    }

    setSelection(itemArr: SelectItem[]) {
        this._itemArr = itemArr;
        this.notifySubscribersThatSelectionChanged();
    }

    hasItemOfType(itemType: SelectItem["itemType"]): boolean {
        return this._itemArr.some((item) => item.itemType === itemType);
    }

    getItemOfType(itemType: SelectItem["itemType"]): SelectItem | undefined {
        return this._itemArr.find((item) => item.itemType === itemType);
    }

    getSelection(): SelectItem[] {
        return this._itemArr;
    }

    subscribeToSelectionChanged(callbackFn: () => void): () => void {
        this._subscribersSet.add(callbackFn);
        return () => {
            this._subscribersSet.delete(callbackFn);
        };
    }

    private notifySubscribersThatSelectionChanged(): void {
        for (const callbackFn of this._subscribersSet) {
            callbackFn();
        }
    }

}

