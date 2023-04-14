import * as style from "../../pages/demo/styles";
import { FormControl } from "@mui/material";
import React, { useEffect, useState } from "react";
import axios from "axios";
import gcp_association from "../../pages/demo/gcp_association.json";

const AWS_INSTANCE = {};
const AWS_REGION = {};
const AWS_AZ = {};

const AZURE_INSTANCE = {};
const AZURE_REGION = {};
const AZURE_TIER = {};

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
    const [assoTier, setAssoTier] = useState();
    const [searchFilter, setSearchFilter] = useState({ instance: '', region: '', start_date: '', end_date: '' });
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
        if (V === 'AWS') {
           setInstance(Object.keys(AWS_INSTANCE));
           setRegion(['ALL', ...Object.keys(AWS_REGION)]);
           setAZ(['ALL']);
        } else if (V === 'AZURE') {
            setInstance(Object.keys(AZURE_INSTANCE));
            setRegion(['ALL', ...Object.keys(AZURE_REGION)]);
            setAssoTier(['ALL', ...Object.keys(AZURE_TIER)]);
        } else {
            let list = []
            let inst = []
            gcp_association.map((e) => {
                let key = Object.keys(e)
                list = list.concat(key)
                inst = inst.length < e[key].length ? e[key] : inst
            })
            setRegion(['ALL'].concat(list))
            setInstance(inst)
            setAZ(['ALL'])
        }
    }
    const setFilter = ({ target }) => { //filter value 저장
        const { name, value } = target;
        // 날짜가 입력 될 경우
        if (name.includes('start_date')) setDate(name, value);
        setSearchFilter({ ...searchFilter, [name]: value });
        if (value !== "ALL") {
            if (name === 'region' && region.includes(value)) {
                if (vendor === 'AWS') {
                    setAssoInstance([...AWS_REGION[value]]);
                    try {
                        let newAssoAZ = new Set();
                        [...AWS_REGION[value]].map((instance) => {
                            newAssoAZ = new Set([...newAssoAZ, ...AWS_INSTANCE[instance]['AZ']]);
                        });
                        setAssoAZ(['ALL', ...newAssoAZ]);
                    } catch (e) { console.log(e); }
                } else if (vendor === 'AZURE') {
                    setAssoInstance([...AZURE_REGION[value]]);
                    try {
                        let newAssoTier = new Set();
                        [...AZURE_REGION[value]].map((instance) => {
                            newAssoTier = new Set([...newAssoTier, ...AZURE_INSTANCE[instance]['InstanceTier']]);
                        })
                        setAssoTier(['ALL', ...newAssoTier]);
                    } catch (e) { console.log(e) }
                } else { //gcp
                    setAssoInstance(gcp_association[region.indexOf(value) - 1][value])
                }
            } else if (name === 'instance') {
                let includeRegion = []
                if (vendor === 'AWS') {
                    includeRegion = [...AWS_INSTANCE[value]['Region']];
                    setAssoAZ(['ALL', ...AWS_INSTANCE[value]['AZ']]);
                } else if (vendor === 'AZURE') {
                    includeRegion = [...AZURE_INSTANCE[value]['Region']];
                    setAssoTier(['ALL', ...AZURE_INSTANCE[value]['InstanceTier']]);
                } else { // gcp
                    region.map((r) => {
                        try {
                            if (r !== 'ALL' && gcp_association[region.indexOf(r) - 1][r].includes(value)) {
                                includeRegion.push(r)
                            }
                        } catch (e) { console.log(e) }
                    });
                }
                setAssoRegion(['ALL'].concat(includeRegion));
            }
        } else {
            if (name === 'region') {
                setAssoAZ(['ALL']);
                setAssoTier(['ALL']);
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
        if (searchFilter["start_date"] <= searchFilter["end_date"]) {
            // button load True로 설정
            setLoad(true);
            //guery 요청시 들어가는 Params, params의 값은 searchFilter에 저장되어 있음
            const params = {
                TableName: vendor.toLowerCase(),
                ...(vendor === 'AWS' && { AZ: searchFilter["az"] === 'ALL' ? "*" : searchFilter["az"] }),
                Region: searchFilter["region"] === 'ALL' ? "*" : searchFilter["region"],
                InstanceType: searchFilter["instance"] === 'ALL' ? "*" : searchFilter["instance"],
                ...(vendor === 'AZURE' && { InstanceTier: searchFilter["tier"] === 'ALL' ? '*' : searchFilter["tier"] }),
                Start: searchFilter["start_date"] === '' ? "*" : searchFilter["start_date"],
                End: searchFilter["end_date"] === '' ? "*" : searchFilter["end_date"]
            };
            //현재 url = "https://puhs0z1q3l.execute-api.us-west-2.amazonaws.com/default/sungjae-timestream-query";
            await axios.get(url, { params })
              .then((res) => {
                  if (res.data.Status === 403) {
                      alert("Invalid Access");
                  } else if (res.data.Status === 500) {
                      alert("Internal Server Error");
                  } else { // Status 성공 시,
                      let parseData = JSON.parse(JSON.parse(res.data.Data));
                      // let parseData = JSON.parse(res.data);
                      const setQueryData = vendor === 'AWS' ? setGetdata : (vendor === 'GCP' ? setGCPData : setAZUREData);
                      setQueryData(parseData);
                      let dataCnt = parseData.length;
                      if (dataCnt < 20000) {
                          alert("Total " + dataCnt + " data points have been returned")
                      } else if (dataCnt === 20000) {
                          alert("The maximum number of data points has been returned (20,000)")
                      }
                  }
                  // button load false로 설정
                  setLoad(false);
              }).catch((e) => {
                  setLoad(false);
                  console.log(e);
                  if (e.message === "Network Error") {
                      alert("A network error occurred. Try it again. ")
                  }
              });
        }
        else {
            alert("The date range for the query is invalid. Please set the date correctly.");
        };
    };
    const ResetSelected = () => {
        if (selectedData.length !== 0) {
            document.querySelector('.PrivateSwitchBase-input').click();
            setSelectedData([]);
        }
    }

    const setFilterData = async () => { // fecth Query Association JSON (now only AWS, AZURE)
        let assoAWS = await axios.get('https://spotlake.s3.us-west-2.amazonaws.com/query-selector/associated/association_aws.json');
        let assoAzure = await axios.get('https://spotlake.s3.us-west-2.amazonaws.com/query-selector/associated/association_azure.json');
        if (assoAWS && assoAWS.data) {
            assoAWS = assoAWS.data[0];
            Object.keys(assoAWS).map((instance) => {
                AWS_INSTANCE[instance] = {
                    ...assoAWS[instance],
                    Region: assoAWS[instance]["Region"].filter((region) => region !== 'nan'),
                    AZ: assoAWS[instance]["AZ"].filter((AZ) => AZ !== 'nan'),
                };
                assoAWS[instance]["Region"].map((region) => {
                    if (region === 'nan') return;
                    if (!AWS_REGION[region]) AWS_REGION[region] = new Set();
                    AWS_REGION[region].add(instance);
                });
                assoAWS[instance]["AZ"].map((az) => {
                    if (az === 'nan') return;
                    if (!AWS_AZ[az]) AWS_AZ[az] = new Set();
                    AWS_AZ[az].add(instance);
                });
            });
        }
        if (assoAzure && assoAzure.data) {
            assoAzure = assoAzure.data[0];
            Object.keys(assoAzure).map((instance) => {
                AZURE_INSTANCE[instance] = {
                    ...assoAzure[instance],
                    Region: assoAzure[instance]["Region"].filter((region) => region !== 'nan'),
                    InstanceTier: assoAzure[instance]["InstanceTier"].filter((tier) => tier !== 'nan'),
                };
                assoAzure[instance]["Region"].map((region) => {
                    if (region === 'nan') return;
                    if (!AZURE_REGION[region]) AZURE_REGION[region] = new Set();
                    AZURE_REGION[region].add(instance);
                });
                assoAzure[instance]["InstanceTier"].map((InstanceTier) => {
                    if (InstanceTier === 'nan') return;
                    if (!AZURE_TIER[InstanceTier]) AZURE_TIER[InstanceTier] = new Set();
                    AZURE_TIER[InstanceTier].add(instance);
                });
            });
        }
        filterSort(vendor);
    }

    useEffect(() => {
        setFilterData();
    }, [])
    useEffect(() => {
        setSearchFilter({ instance: '', region: '', start_date: '', end_date: '' })
        setAssoRegion();
        setAssoInstance();
        setAssoAZ(['ALL']);
        filterSort(vendor);
        ResetSelected();
    }, [vendor])

    useEffect(() => { // end_date가 max를 초과할 경우
        if (searchFilter.end_date && new Date(searchFilter.end_date) > new Date(dateRange.max)) {
            setSearchFilter({ ...searchFilter, end_date: dateRange.max });
        }
    }, [searchFilter.start_date]);

    return (
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
                  )) : instance ? instance.map((e) => (
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
                  )) : region ? region.map((e) => (
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
                    )) : az ? az.map((e) => (
                      <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                    )) : null}
                </style.filterSelect>
            </FormControl>
            : null}
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
                    )) : assoTier ? assoTier.map((e) => (
                      <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                    )) : null}
                </style.filterSelect>
            </FormControl>
          }
          <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }} className="date-input">
              <style.dataLabel htmlFor="start_date-input">Start date : </style.dataLabel>
              <input type="date" id="start_date-input" name="start_date" onChange={setFilter} value={searchFilter.start_date} max={new Date().toISOString().split('T')[0]} />
          </FormControl>
          <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }} className="date-input">
              <style.dataLabel htmlFor="end_date-input">End date : </style.dataLabel>
              <input type="date" id="end_date-input" name="end_date" onChange={setFilter} value={searchFilter.end_date} max={dateRange.max} />
          </FormControl>
          <style.chartBtn onClick={querySubmit} vendor={vendor} loading={load}>Query</style.chartBtn>
          {/*{load?<ReactLoading type='spin' height='30px' width='30px' color='#1876d2' /> : null}*/}
      </style.tablefilter>
    )
}
export default Query;
