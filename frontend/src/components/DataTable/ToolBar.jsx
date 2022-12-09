import {
    GridToolbarColumnsButton,
    GridToolbarContainer,
    GridToolbarDensitySelector, GridToolbarExport,
    GridToolbarFilterButton
} from "@mui/x-data-grid-pro";
import React from "react";

const CustomToolbar = ({
    addTool
}) => (
    <GridToolbarContainer>
        <GridToolbarFilterButton />
        <GridToolbarColumnsButton />
        <GridToolbarDensitySelector />
        <GridToolbarExport />
        {/*<style.toolbarText>alpha : </style.toolbarText>*/}
        {/*<style.alphaInput label="alpha" variant="outlined" name="alpha" id="alpha" placeholder="default 0.7" ref={alphaInput}/>*/}
        {/*<style.alphaBtn variant="contained" onClick={changeAlpha}>Apply</style.alphaBtn>*/}
        {addTool && addTool}
    </GridToolbarContainer>
);
export default CustomToolbar