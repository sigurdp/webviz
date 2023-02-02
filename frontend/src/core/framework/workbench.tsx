import { ModuleInstance } from "./module";
import { StateStore } from "./state-store";
import { ModuleRegistry } from "./ModuleRegistry";
import { WorkbenchServices } from "./WorkbenchServices";

let workbenchInstance: Workbench | undefined;

export enum WorkbenchEvent {
    ModuleAdded = "workbench:module-added",
    ModuleRemoved = "workbench:module-removed",
    ModuleSelected = "workbench:module-selected",
    ModulesChanged = "workbench:modules-changed",
}

export type WorkbenchContext = {
    useWorkbenchStateValue: <T>(key: string) => T;
    workbenchServices: WorkbenchServices;
};

class Workbench {
    private moduleInstances: ModuleInstance[];
    private _activeModuleId: string;
    private _importedModules: string[];
    private layout: string[];
    private stateStore: StateStore;
    private _workbenchServices: WorkbenchServices;

    constructor() {
        this.moduleInstances = [];
        this._importedModules = [];
        this._activeModuleId = "";
        this.layout = [];
        this.stateStore = new StateStore();
        this._workbenchServices = new WorkbenchServices();

        if (workbenchInstance) {
            throw new Error("Workbench already exists");
        }

        workbenchInstance = this;
    }

    public getStateStore(): StateStore {
        return this.stateStore;
    }

    public getWorkbenchServices(): WorkbenchServices {
        return this._workbenchServices;
    }

    public get activeModuleId(): string {
        return this._activeModuleId;
    }

    public get activeModuleName(): string {
        return (
            this.moduleInstances.find(
                (moduleInstance) => moduleInstance.id === this._activeModuleId
            )?.name || ""
        );
    }

    public set activeModuleId(id: string) {
        this._activeModuleId = id;
        this.dispatchEvents(WorkbenchEvent.ModuleSelected);
    }

    public getModuleInstances(): ModuleInstance[] {
        return this.moduleInstances;
    }

    public makeLayout(layout: string[]): void {
        this.moduleInstances = [];
        layout.forEach((moduleName) => {
            this.addModuleToLayout(moduleName);
        });
    }

    private dispatchEvents(...eventNames: string[]): void {
        eventNames.forEach((eventName) => {
            const event = new CustomEvent(eventName);
            window.dispatchEvent(event);
        });
    }

    private addModuleToLayout(moduleName: string): void {
        if (this._importedModules.includes(moduleName)) {
            this.addModuleInstance(moduleName);
            return;
        }

        this.loadModule(moduleName);
    }

    private loadModule(moduleName: string): void {
        this.moduleInstances.push({
            id: moduleName,
            name: moduleName,
            loading: true,
        });
        this.dispatchEvents(
            WorkbenchEvent.ModuleAdded,
            WorkbenchEvent.ModulesChanged
        );

        import(`/src/modules/${moduleName}/module.tsx`)
            .then(() => {
                this._importedModules.push(moduleName);
                this.replaceLoadingModuleInstances(moduleName);
            })
            .catch((err) => {
                throw new Error(`Module ${moduleName} not found`);
            });
    }

    private maybeMakeFirstModuleInstanceActive(): void {
        if (
            !this.moduleInstances.some((el) => el.id === this._activeModuleId)
        ) {
            this.activeModuleId = this.moduleInstances[0].id;
        }
    }

    private addModuleInstance(moduleName: string): void {
        const module = ModuleRegistry.getModule(moduleName);
        if (!module) {
            throw new Error(`Module ${moduleName} not found`);
        }

        this.moduleInstances.push(module.makeInstance());
        this.maybeMakeFirstModuleInstanceActive();
        this.dispatchEvents(
            WorkbenchEvent.ModuleAdded,
            WorkbenchEvent.ModulesChanged
        );
    }

    private replaceLoadingModuleInstances(moduleName: string): void {
        const module = ModuleRegistry.getModule(moduleName);
        if (!module) {
            throw new Error(`Module ${moduleName} not found`);
        }

        this.moduleInstances = this.moduleInstances.map((instance) => {
            if (instance.loading && instance.name === moduleName) {
                return module.makeInstance();
            }
            return instance;
        });
        this.maybeMakeFirstModuleInstanceActive();
        this.dispatchEvents(WorkbenchEvent.ModulesChanged);
    }
}

// !!!!!!!!!!!!!!!!!!!!!!!!!!!
const workbench = new Workbench();
export { workbench };
