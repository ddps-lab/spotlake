import styled from 'styled-components';
import {LicenseInfo} from "@mui/x-data-grid-pro";
import { DataGridPro } from '@mui/x-data-grid-pro';
import { LineChart } from 'recharts';
import Button from '@mui/material/Button';
import {InputLabel, MenuItem, Select} from "@mui/material";
import { LoadingButton } from '@mui/lab';
LicenseInfo.setLicenseKey(
  'a2de1b3a7dbfa31c88ed686c8184b394T1JERVI6MzYzOTAsRVhQSVJZPTE2NzQzNjA3NDAwMDAsS0VZVkVSU0lPTj0x',
);

export const demo = styled.div`
  width: 80%;
  min-width: 800px;
  margin: 0 auto;
  padding-top: 10px;
  color: #fff;
`;
export const vendor = styled.div` 
  color: #000;
  margin: 50px auto;
  display: flex;
  flex-direction: row;
  justify-content: center;
  gap: 30px;
`;
export const vendorBtn = styled(Button)`
  width: 180px;
  min-width: max-content !important;
  border: none;
  box-shadow: 0px 0px 4px rgba(0,0,0,0.26);
  background: none;
  flex-direction: column;
  justify-content: space-between !important;
  ${(props) => ((props.clicked===true)?'box-shadow: inset 2px 2px 4px rgba(0,0,0,0.16); background:rgb(61,86,178, 0.1) !important':null)}
`;
export const vendorIcon = styled.img`
  width: 60px;
`;
export const vendorTitle = styled.p`
  color: #555;
  font-size: 1em;
  text-transform: none;
  white-space: nowrap;
  margin: 0 !important;
`;
export const notice = styled.p`
  color: #000;
  text-align: left;
  margin-bottom: 15px;
  font-size: 1.1em;
`;
export const imgDiv = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  width: 100%;
  align-items: center;
  margin-bottom: 40px;
`;
export const calimg = styled.img`
  height: 60px;
  margin: 0 auto;
`;
export const table = styled.div`
  width: 100%;
  height: 100vh;
  overflow-x: scroll;
  position: relative;
  margin-bottom: 50px;
`;
export const tablefilter = styled.div`
  color: #000;
  width: calc(100% - 6px);
  display: flex;
  justify-content: space-around;
  height: 45px;
  align-items: center;
  border: 3px solid ${(props) => (props.vendor==='AWS' ? '#f68d11' : props.vendor ==='GCP' ? 'rgb(234, 67, 53)': '#0067b8')};
  border-radius: 5px;
  padding-top: 10px;
  padding-bottom: 10px;
  margin: 40px 0;
  input {
    border: none;
    border-bottom: 1px solid #a0a0a0;
    outline: none;
    border-radius: 0;
    width: 90%;
    background: none;
  }
`;
export const filterTitle = styled.p`
  line-height: 30px;
  color: #000;
  text-align: left;
  margin: 0 0 0 10px;
`;
export const filterLabel = styled(InputLabel)`
  color : ${(props) => (props.vendor==='AWS' ? '#f68d11 !important' : props.vendor ==='GCP' ? 'rgb(234, 67, 53) !important': '#0067b8 !important')};
`;
export const dataLabel = styled.label`
  position: absolute;
  top: -3px;
  font-size: 0.8125rem;
  color: #666;
`;
export const filterSelect = styled(Select)`
  &::after {
    border-bottom: 2px solid ${(props) => (props.vendor==='AWS' ? '#f68d11 !important' : props.vendor ==='GCP' ? 'rgb(234, 67, 53) !important': '#0067b8 !important')};
  }
`;
export const selectItem = styled(MenuItem)`
 &.Mui-selected {
   background: ${(props) => (props.vendor==='AWS' ? 'rgba(246, 141, 17, 0.08) !important;' :props.vendor==='GCP'? 'rgba(234, 67, 53, 0.08) !important;' : 'rgba(0, 103, 184, 0.08) !important;')};;
 }
`;
export const toolbarText = styled.p`
  margin: 0 0 3px 5px;
`;
export const alphaInput = styled.input`
  background: none;
  border: 2px solid #1876d2;
  height: 21px;
  margin: 0 5px;
  border-radius: 5px;
  outline: none;
  width: 100px;
  padding: 0 5px;
`;
export const alphaBtn = styled(LoadingButton)`
  height: 25px;
  margin: 0 5px !important;
  &.MuiButton-text {
    color: #fff;
  }
  background-color: ${(props) => (props.vendor==='AWS' ? '#f68d11 !important;' :props.vendor==='GCP'? 'rgb(234, 67, 53) !important;' : '#0067b8 !important;')};
  &:hover {
    background: ${(props) => (props.vendor==='AWS' ? 'rgba(246, 141, 17, 0.8) !important;' :props.vendor==='GCP'? 'rgba(234, 67, 53, 0.8) !important;' : 'rgba(0, 103, 184, 0.8) !important;')};
  }
`;
export const dataTable = styled(DataGridPro)`
  height: calc(100% - 41px);
  width: 100%;
`;
export const chartBtn = styled(LoadingButton)`
  width: 10%;
  height: 30px;
  background: ${(props) => (props.vendor==='AWS' ? '#f68d11 !important;' :props.vendor==='GCP'? 'rgb(234, 67, 53) !important;' : '#0067b8 !important;')};;
  &.MuiButton-text {
    color: #fff;
  }
  &:hover {
    background: ${(props) => (props.vendor==='AWS' ? 'rgba(246, 141, 17, 0.8) !important;' :props.vendor==='GCP'? 'rgba(234, 67, 53, 0.8) !important;' : 'rgba(0, 103, 184, 0.8) !important;')};;
  }
`;
export const chartModal = styled.div`
  width: 100%;
  height: calc(100% + 30px);
  background: rgba(255,255,255,0.5);
  position: absolute;
  left: 0;
  z-index: 10;
`;
export const chartSection = styled.div`
  width: 80%;
  background: #fff;
  margin: 0 auto;
  padding: 30px 30px 0 30px;
  position: relative;
  border: 1px solid #c4c4c4;
  border-radius: 3px;
  top: 20px;
`;
export const chartClose = styled.button`
  color: #4d4d4d;
  font-size: 25px;
  padding: 4.5px 0;
  background: none;
  width: 40px;
  height: 40px;
  position: absolute;
  top: 10px;
  right: 10px;
  border: 1px solid #c4c4c4;
  border-radius: 3px;
  &:hover {
    background: #e7e7e7;
  }
`;
export const chartTitle = styled.p`
  color: #000;
  font-size: 20px;
  font-weight: bold;
  text-align: center;
  padding-left: 35px;
`;
export const chartSubTitle = styled.p`
  color: #000;
  font-size: 15px;
  margin-left: 20%;
  text-align: left;
`;
export const chart = styled(LineChart)`
  margin: 0 auto 30px auto;
  border-bottom: 1px solid #e3e3e3;
`;
export const progressBar = styled.div`
  z-index: 1;
  position: absolute;
  top: 28%;
  right: 10%;
  width: 80%;
  & .MuiLinearProgress-root {
    height: 20px !important;
    border-radius: 10px;
    background-color: ${(props) => (props.vendor==='AWS' ? 'rgba(246, 141, 17, 0.3) !important;' :props.vendor==='GCP'? 'rgba(234, 67, 53, 0.3) !important;' : 'rgba(0, 103, 184, 0.3) !important;')};
  }
  & .MuiLinearProgress-bar {
   background-color: ${(props) => (props.vendor==='AWS' ? '#f68d11' : props.vendor ==='GCP' ? 'rgb(234, 67, 53)': '#0067b8')};
  }
`;
export const noticeMsg = styled.p`
  color: #000;
  font-size: 14px;
  margin: 10px 0;
`;