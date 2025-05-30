
import type { IntersectedItem, IntersectionCalculator } from "../types/types";
import { IntersectionItemShape } from "../types/types";

import { BoundingSphere2D } from "./BoundingSphere2D";

export interface PointIntersectedItem extends IntersectedItem {
    shape: IntersectionItemShape.POINT;
}

export class PointIntersectionCalculator implements IntersectionCalculator {
    private _boundingSphere: BoundingSphere2D;

    constructor(point: number[], margin: number = 10) {
        this._boundingSphere = new BoundingSphere2D(point, margin);
    }

    calcIntersection(point: number[]): PointIntersectedItem | null {
        if (!this._boundingSphere.contains(point)) {
            return null;
        }

        return {
            shape: IntersectionItemShape.POINT,
            point: this._boundingSphere.getCenter(),
        };
    }
}
