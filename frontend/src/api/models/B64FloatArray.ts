/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

export type B64FloatArray = {
    element_type: B64FloatArray.element_type;
    data_b64str: string;
    offset: (number | null);
    intToFloatScale: (number | null);
};

export namespace B64FloatArray {

    export enum element_type {
        FLOAT32 = 'float32',
        FLOAT64 = 'float64',
        FLOAT32_AS_UINT16 = 'float32_as_uint16',
    }


}

