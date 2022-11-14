import * as style from "../../pages/demo/styles";
import {FormControl} from "@mui/material";
import React, {useEffect, useState} from "react";
import axios from "axios";
import aws_association from "../../pages/demo/aws_association.json";
import gcp_association from "../../pages/demo/gcp_association.json";
import azure_association from "../../pages/demo/azure_association.json";

const Query = ({
  vendor,
  selectedData,
  setSelectedData,
  setGetdata,
  setGCPData,
  setAZUREData,
}) =>{
    const url = "https://ohu7b2tglrqpl7qlogbu7pduq40flbgg.lambda-url.us-west-2.on.aws/";
    const [load, setLoad] = useState(false);
    const [region, setRegion] = useState();
    const [az, setAZ] = useState();
    const [instance, setInstance] = useState();
    const [assoRegion, setAssoRegion] = useState();
    const [assoInstance, setAssoInstance] = useState();
    const [assoAZ, setAssoAZ] = useState();
    const [assoTier, setAssoTier] = useState(['ALL', 'Standard', 'Basic']);
    const [searchFilter, setSearchFilter] = useState({instance: '', region: '', start_date: '', end_date: ''});
    const [dateRange, setDateRange] = useState({
        min: null,
        max: new Date().toISOString().split('T')[0]
    })

    const setDate = (name, value) => {
        const tmpMax = new Date(value);
        const today = new Date();
        tmpMax.setMonth(tmpMax.getMonth() + 1);
        if (tmpMax < today) {
            setDateRange({ ...dateRange, max: tmpMax.toISOString().split('T')[0] });
        } else setDateRange({ ...dateRange, max: today.toISOString().split('T')[0] });
    }
    const filterSort = (V) => {
        // V : vendor
        let list = []
        let inst = []
        if (V==='AWS') {
            aws_association.map((e) => {
                let key = Object.keys(e)
                list = list.concat(key)
                inst = inst.length < e[key]['InstanceType'].length ? e[key]['InstanceType'] : inst
            })
            setRegion(['ALL'].concat(list))
            setInstance(inst)
            setAZ(['ALL'])
        }else {
            const assoFile = V==='GCP' ? gcp_association : azure_association
            assoFile.map((e) => {
                let key = Object.keys(e)
                list = list.concat(key)
                inst = inst.length < e[key].length ? e[key] : inst
            })
            setRegion(['ALL'].concat(list))
            setInstance(inst)
            setAZ(['ALL'])
        }
    }
    const setFilter = ({target}) => { //filter value 저장
        const {name, value} = target;
        // 날짜가 입력 될 경우
        if (name.includes('start_date')) setDate(name, value);
        setSearchFilter({...searchFilter, [name] : value});
        const assoFile = vendor==='AWS' ? aws_association : vendor === 'GCP' ? gcp_association : azure_association;
        if (value!=="ALL"){
            if (name==='region'&&region.includes(value)){
                if (vendor==='AWS') {
                    setAssoInstance(assoFile[region.indexOf(value) - 1][value]['InstanceType'])
                    setAssoAZ(['ALL'].concat(assoFile[region.indexOf(value) - 1][value]['AZ']))
                }else { //gcp, azure
                    setAssoInstance(assoFile[region.indexOf(value) - 1][value])
                }
            }else if (name==='instance'){
                let includeRegion = []
                region.map((r) => {
                    try {
                        if (vendor==='AWS') {
                            if (r !== 'ALL' && assoFile[region.indexOf(r) - 1][r]['InstanceType'].includes(value)) {
                                includeRegion.push(r)
                            }
                        }else { //gcp, azure
                            if (r !== 'ALL' && assoFile[region.indexOf(r) - 1][r].includes(value)) {
                                includeRegion.push(r)
                            }
                        }
                    }catch (e) {console.log(e)}
                })
                setAssoRegion(['ALL'].concat(includeRegion))
            }
        }else {
            if (name==='region'){
                setAssoInstance(instance)
                setAssoAZ(['ALL'])
            }
        }
    }
    const querySubmit = async () => {
        // 쿼리를 날리기 전에 searchFilter에 있는 값들이 비어있지 않은지 확인.
        const invalidQuery = Object.keys(searchFilter).map((data) => { if (!searchFilter[data]) return false }).includes(false)
        if (invalidQuery) {
            alert("The query is invalid. \nPlease check your search option.");
            return;
        }
        //start_date , end_date 비교 후 start_date가 end_date보다 이전일 경우에만 데이터 요청
        if (searchFilter["start_date"] <= searchFilter["end_date"]){
            // button load True로 설정
            setLoad(true);
            //guery 요청시 들어가는 Params, params의 값은 searchFilter에 저장되어 있음
            const params  = {
                TableName: vendor.toLowerCase(),
                ...(vendor === 'AWS' && { AZ: searchFilter["az"] === 'ALL' ? "*" : searchFilter["az"] }),
                Region: searchFilter["region"] === 'ALL' ? "*" : searchFilter["region"],
                InstanceType: searchFilter["instance"] === 'ALL' ? "*" : searchFilter["instance"],
                ...(vendor === 'AZURE' && { InstanceTier: searchFilter["tier"] === 'ALL' ? '*' : searchFilter["tier"] }),
                Start: searchFilter["start_date"] === '' ? "*" : searchFilter["start_date"],
                End: searchFilter["end_date"] === '' ? "*" : searchFilter["end_date"]
            };
            //현재 url = "https://puhs0z1q3l.execute-api.us-west-2.amazonaws.com/default/sungjae-timestream-query";
            await axios.get(url,{params})
                .then((res) => {
                    //get 요청을 통해 받는 리턴값
                    let parseData = JSON.parse(res.data);
                    const setQueryData = vendor === 'AWS' ? setGetdata : (vendor === 'GCP' ? setGCPData : setAZUREData);
                    setQueryData(parseData);
                    let dataCnt = parseData.length;
                    if (dataCnt<20000){
                        alert("Total "+dataCnt+" data points have been returned")
                    }else if (dataCnt === 20000){
                        alert("The maximum number of data points has been returned (20,000)")
                    }
                    // button load false로 설정
                    setLoad(false);
                }).catch((e) => {
                    console.log(e)
                    setLoad(false);
                    if (e.message === "Network Error"){
                        alert("A network error occurred. Try it again. ")
                    }else {
                        alert(e.message)
                    }
                })}
        else {
            alert("The date range for the query is invalid. Please set the date correctly.");
        };
    };
    const ResetSelected = () => {
        if (selectedData.length!==0) {
            document.querySelector('.PrivateSwitchBase-input').click();
            setSelectedData([]);
        }
    }
    useEffect(() => {
        setSearchFilter({instance: '', region: '', start_date: '', end_date: ''})
        setAssoRegion();
        setAssoInstance();
        setAssoAZ();
        filterSort(vendor);
        ResetSelected();
    },[vendor])

    useEffect(() => { // end_date가 max를 초과할 경우
        if (searchFilter.end_date && new Date(searchFilter.end_date) > new Date(dateRange.max)) {
            setSearchFilter({ ...searchFilter, end_date: dateRange.max });
        }
    }, [searchFilter.start_date]);

    return(
        <style.tablefilter vendor={vendor}>
            <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }}>
                <style.filterLabel id="instance-input-label" vendor={vendor}>Instance</style.filterLabel>
                <style.filterSelect
                    labelId="instance-input-label"
                    id="instance-input"
                    value={searchFilter['instance']}
                    onChange={setFilter}
                    label="Instance"
                    name="instance"
                    vendor={vendor}
                >
                    {assoInstance ? assoInstance.map((e) => (
                        <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                    )):instance ? instance.map((e) => (
                        <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                    )) : null}
                </style.filterSelect>
            </FormControl>
            <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }}>
                <style.filterLabel id="region-input-label" vendor={vendor}>Region</style.filterLabel>
                <style.filterSelect
                    labelId="region-input-label"
                    id="region-input"
                    value={searchFilter['region']}
                    onChange={setFilter}
                    label="Region"
                    name="region"
                    vendor={vendor}
                >
                    {assoRegion ? assoRegion.map((e) => (
                        <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                    )):region ? region.map((e) => (
                        <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                    )) : null}
                </style.filterSelect>
            </FormControl>
            {vendor === 'AWS' ?
                <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }}>
                    <style.filterLabel id="az-input-label" vendor={vendor}>AZ</style.filterLabel>
                    <style.filterSelect
                        labelId="az-input-label"
                        id="az-input"
                        value={searchFilter['az']}
                        onChange={setFilter}
                        label="AZ"
                        name="az"
                        vendor={vendor}
                    >
                        {assoAZ ? assoAZ.map((e) => (
                            <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                        )):az ? az.map((e) => (
                            <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                        )) : null}
                    </style.filterSelect>
                </FormControl>
                :null}
            {vendor === 'AZURE' &&
            <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }}>
                <style.filterLabel id="instance-tier-input-label" vendor={vendor}>Tier</style.filterLabel>
                <style.filterSelect
                    labelId="instance-tier-input-label"
                    id="instance-tier-input"
                    value={searchFilter['tier']}
                    onChange={setFilter}
                    label="tier"
                    name="tier"
                    vendor={vendor}
                >
                    {assoTier ? assoTier.map((e) => (
                        <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                    )):assoTier ? assoTier.map((e) => (
                        <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                    )) : null}
                </style.filterSelect>
            </FormControl>
            }
            <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }} className="date-input">
                <style.dataLabel htmlFor="start_date-input">Start date : </style.dataLabel>
                <input type="date" id="start_date-input" name="start_date" onChange={setFilter} value={searchFilter.start_date} max={new Date().toISOString().split('T')[0]}/>
            </FormControl>
            <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }} className="date-input">
                <style.dataLabel htmlFor="end_date-input">End date : </style.dataLabel>
                <input type="date" id="end_date-input" name="end_date" onChange={setFilter} value={searchFilter.end_date} max={dateRange.max}/>
            </FormControl>
            <style.chartBtn onClick={querySubmit} vendor={vendor} loading={load}>Query</style.chartBtn>
            {/*{load?<ReactLoading type='spin' height='30px' width='30px' color='#1876d2' /> : null}*/}
        </style.tablefilter>
    )
}
export default Query;