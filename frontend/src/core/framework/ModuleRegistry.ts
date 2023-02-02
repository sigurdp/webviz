import { Module } from "./module";


export class ModuleRegistry {
    private static _registeredModules: Module[] = [];

    private constructor() {
    }

    public static registerModule(moduleName: string): Module {
        const module = new Module(moduleName);
        this._registeredModules.push(module);
        return module;
    }

    public static getModule(moduleName: string) : Module | null {
        const module = this._registeredModules.find((m) => m.name === moduleName);
        if (module) {
            return module;
        }
        return null;
    }
}

