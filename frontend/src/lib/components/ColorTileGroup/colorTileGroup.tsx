import type React from "react";

import { ColorTile } from "@lib/components/ColorTile";
import type { ColorPalette } from "@lib/utils/ColorPalette";
import { resolveClassNames } from "@lib/utils/resolveClassNames";

export type ColorPaletteProps = {
    colorPalette: ColorPalette;
    gap?: boolean;
};

export const ColorTileGroup: React.FC<ColorPaletteProps> = (props) => {
    return (
        <div className="flex">
            <div
                className={resolveClassNames("flex w-full", {
                    "gap-1": props.gap,
                    "rounded-sm border border-slate-600": !props.gap,
                })}
            >
                {props.colorPalette.getColors().map((color) => (
                    <ColorTile key={color} color={color} grouped />
                ))}
            </div>
            <div className="grow" />
        </div>
    );
};
