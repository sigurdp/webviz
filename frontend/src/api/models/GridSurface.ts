/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { B64FloatArray } from './B64FloatArray';
import type { B64UintArray } from './B64UintArray';

export type GridSurface = {
    polys_b64arr: B64UintArray;
    points_b64arr: B64FloatArray;
    xmin: number;
    xmax: number;
    ymin: number;
    ymax: number;
    zmin: number;
    zmax: number;
};

