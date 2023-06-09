import React, {useState} from "react";
import {DataGridPro, LicenseInfo} from '@mui/x-data-grid-pro';
import styled from "styled-components";
import ColumnData from "./ColumnData";

LicenseInfo.setLicenseKey(
    'a2de1b3a7dbfa31c88ed686c8184b394T1JERVI6MzYzOTAsRVhQSVJZPTE2NzQzNjA3NDAwMDAsS0VZVkVSU0lPTj0x',
);

const DataTable = ({
    vendor,
    rowData,
    toolBar,
    setSelectedData
}) => {
    const [pageSize, setPageSize] = useState(1000);
    const {columns,GCPcolumns,AZUREcolumns} = ColumnData();
    const toolBarComponent = () =>(
        toolBar
    )
    return(
        <DataGridTable
            rows={rowData}
            columns={vendor==='AWS' ? columns : vendor === 'GCP' ? GCPcolumns : AZUREcolumns}
            experimentalFeatures={{ columnGrouping: true }}
            checkboxSelection
            onSelectionModelChange = {(newSelectionModel) => {
                let selected = [];
                const Data = rowData
                newSelectionModel.map((e) => {
                    selected.push(Data[e-1])
                })
                setSelectedData(selected);
            }}
            components={{ Toolbar: toolBarComponent }}
            pageSize={pageSize}
            onPageSizeChange={(newPageSize) => setPageSize(newPageSize)}
            rowsPerPageOptions={[500, 1000, 2000]}
            pagination
            sx={{
                '& .MuiDataGrid-main .Mui-checked, .MuiButton-textPrimary':{
                    color : vendor==='AWS' ? '#f68d11 !important' : vendor === 'GCP' ? 'rgb(234, 67, 53) !important' : '#0067b8 !important'
                },
                '& .Mui-selected': {
                    backgroundColor : vendor==='AWS' ? 'rgba(246, 141, 17, 0.08) !important' : vendor === 'GCP' ? 'rgba(234, 67, 53, 0.08) !important' : 'rgba(0, 103, 184, 0.08) !important'
                },
                '& .MuiDataGrid-cellCheckbox:focus-within' :{
                    outline : vendor==='AWS' ? 'solid rgba(246, 141, 17, 0.5) 1px !important' : vendor === 'GCP' ?'solid rgba(234, 67, 53, 0.5) 1px !important' : 'solid rgba(0, 103, 184, 0.5) 1px !important'
                },
                "& .MuiDataGrid-cell" : {
                    justifyContent : "center !important",
                }
            }}
        />
    )
}
const DataGridTable = styled(DataGridPro)`
height: calc(100% - 41px);
width: 100%;
`;
export default DataTable;
