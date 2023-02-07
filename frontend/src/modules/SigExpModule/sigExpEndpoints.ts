const iterationsList: string[] = ["Iter1", "Iter2"];

function vectorNamesForIteration(iterationId: string): string[] {
    return [
        "Vector_A",
        "Vector_B",
        "Vector_C",
        "Vector_X--" + iterationId,
        "Vector_Y--" + iterationId,
        "Vector_Z--" + iterationId,
    ];
}

export function getIterationIds(): Promise<string[]> {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(iterationsList);
        }, 1000);
    });
}

export function getVectorNames(iterationId: string): Promise<string[]> {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(vectorNamesForIteration(iterationId));
        }, 2000);
    });
}

export function getVectorData(iterationId: string, vectorName: string): Promise<number[]> {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            const iterationIdx = iterationsList.indexOf(iterationId);
            const vectorIdx = vectorNamesForIteration(iterationId).indexOf(vectorName);
            if (iterationIdx < 0 || vectorIdx < 0) {
                return reject();
            }

            const value: number = iterationIdx * 100 + vectorIdx;
            const valueArr = Array<number>(5).fill(value);
            resolve(valueArr);
        }, 1000);
    });
}
