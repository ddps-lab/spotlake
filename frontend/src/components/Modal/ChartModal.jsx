import {CartesianGrid, Legend, Line, Tooltip, XAxis, YAxis} from "recharts";
import React, {useRef} from "react";
import * as style from "./ChartModalStyle"

const ChartModal = ({
    width,
    setChartModal,
    selectedData,
    IFGraph,
    setIFGraph,
    SPSGraph,
    setSPSGraph,
    SPGraph,
    setSPGraph
}) => {
    const canvasSection = useRef();
    const chartColor = ['#339f5e','#2a81ea', '#b23eff', '#ffa53e', '#ff6565'];
    return(
        <style.chartModal>
            <style.chartSection ref={canvasSection}>
                {/*모달 닫기 & 데이터 초기화*/}
                <style.chartClose onClick={() => {setChartModal(false); setSPGraph([]); setIFGraph([]);setSPSGraph([]);}}>X</style.chartClose>
                <style.chartTitle>History Visualizing</style.chartTitle>
                <style.chartSubTitle>Legend (InstanceType / Region / AZ)</style.chartSubTitle>
                <style.chart
                    className="firstChart"
                    width={width}
                    height={300}
                    data={SPSGraph}
                    margin={{
                        top: 5, right: 30, left: 20, bottom: 30,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3"/>
                    <XAxis dataKey="name" label={{ value: "Times", position: "bottom", dy: 0}} />
                    <YAxis label={{ value: "Spot Placement Score", angle: -90, position: "left", dy: -70}} />
                    <Tooltip/>
                    <Legend verticalAlign="top"/>
                    {selectedData.map((d) => (
                        <Line type="monotone" dataKey={d.InstanceType+'/'+d.Region+'/'+d.AZ} stroke={chartColor[selectedData.indexOf(d)]} activeDot={{r: 8}}/>
                    ))}
                </style.chart>
                <style.chart
                    width={width}
                    height={300}
                    data={IFGraph}
                    margin={{
                        top: 5, right: 30, left: 20, bottom: 30,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3"/>
                    <XAxis dataKey="name" label={{ value: "Times", position: "bottom", dy: 0}} />
                    <YAxis label={{ value: "Interrupt Frequency", angle: -90, position: "left", dy: -70}} />
                    <Tooltip/>
                    {selectedData.map((d) => (
                        <Line type="monotone" dataKey={d.InstanceType+'/'+d.Region+'/'+d.AZ} stroke={chartColor[selectedData.indexOf(d)]} activeDot={{r: 8}}/>
                    ))}
                </style.chart>
                <style.chart
                    width={width}
                    height={300}
                    data={SPGraph}
                    margin={{
                        top: 5, right: 30, left: 20, bottom: 30,
                    }}
                    style={{border:'none'}}
                >
                    <CartesianGrid strokeDasharray="3 3"/>
                    <XAxis dataKey="name" label={{ value: "Times", position: "bottom", dy: 0}} />
                    <YAxis label={{ value: "Spot Price", angle: -90, position: "left", dy: -50}} />
                    <Tooltip/>
                    {selectedData.map((d) => (
                        <Line type="monotone" dataKey={d.InstanceType+'/'+d.Region+'/'+d.AZ} stroke={chartColor[selectedData.indexOf(d)]} activeDot={{r: 8}}/>
                    ))}
                </style.chart>
            </style.chartSection>
        </style.chartModal>
    )
}
export default ChartModal;