import styled from "styled-components";
import {LineChart} from "recharts";

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