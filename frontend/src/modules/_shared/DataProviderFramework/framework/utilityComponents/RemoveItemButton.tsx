import { Delete } from "@mui/icons-material";

import { DenseIconButton } from "@lib/components/DenseIconButton";
import { DenseIconButtonColorScheme } from "@lib/components/DenseIconButton/denseIconButton";

import type { Item } from "../../interfacesAndTypes/entities";
import { DataProvider } from "../DataProvider/DataProvider";

export type RemoveItemButtonProps = {
    item: Item;
};

export function RemoveItemButton(props: RemoveItemButtonProps): React.ReactNode {
    function handleRemove() {
        const parentGroup = props.item.getItemDelegate().getParentGroup();
        if (parentGroup) {
            parentGroup.removeChild(props.item);
        }

        if (props.item instanceof DataProvider) {
            props.item.beforeDestroy();
        }
    }

    return (
        <>
            <DenseIconButton onClick={handleRemove} title="Remove item" colorScheme={DenseIconButtonColorScheme.DANGER}>
                <Delete fontSize="inherit" />
            </DenseIconButton>
        </>
    );
}
