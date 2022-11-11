import React, {useEffect, useRef, useState} from "react";
import axios from 'axios';
import * as style from './styles';
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts';
import { LicenseInfo } from '@mui/x-data-grid-pro';
import { GridToolbarContainer, GridToolbarFilterButton, GridToolbarColumnsButton, GridToolbarExport, GridToolbarDensitySelector, } from '@mui/x-data-grid-pro';
import aws_association from './aws_association.json';
import gcp_association from './gcp_association.json';
import azure_association from './azure_association.json';
import {FormControl} from "@mui/material";
import LinearProgress from '@mui/material/LinearProgress';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';

LicenseInfo.setLicenseKey(
    'a2de1b3a7dbfa31c88ed686c8184b394T1JERVI6MzYzOTAsRVhQSVJZPTE2NzQzNjA3NDAwMDAsS0VZVkVSU0lPTj0x',
);

function Demo () {
  const url = "https://ohu7b2tglrqpl7qlogbu7pduq40flbgg.lambda-url.us-west-2.on.aws/";
  const [w, setWidth] = useState(window.innerWidth*0.6);
  const canvasSection = useRef();
  const [chartColor, setChartColor] = useState(['#339f5e','#2a81ea', '#b23eff', '#ffa53e', '#ff6565']);
  const [chartModal, setChartModal] = useState(false);
  const [getData, setGetdata] = useState([]);
  const [IFGraph, setIFGraph] = useState([]);
  const [SPSGraph, setSPSGraph] = useState([]);
  const [SPGraph, setSPGraph] = useState([]);
  const [searchFilter, setSearchFilter] = useState({instance: '', region: '', start_date: '', end_date: ''});
  const [alpha, setAlpha] = useState(0.7);
  const alphaInput = useRef();
  const [pageSize, setPageSize] = useState(1000);
  const [load, setLoad] = useState(false);
  const [region, setRegion] = useState();
  const [az, setAZ] = useState();
  const [instance, setInstance] = useState();
  const [assoRegion, setAssoRegion] = useState();
  const [assoInstance, setAssoInstance] = useState();
  const [assoAZ, setAssoAZ] = useState();
  const [selectedData, setSelectedData] = useState([]);
  const [graphData, setGraphData] = useState([]);
  const [graphLoad, setGraphLoad] = useState(false);
  const [visualDate, setVisualDate] = useState(0);
  const [vendor, setVendor] = useState('AWS');
  const [GCPData, setGCPData] = useState([]);
  const [AZUREData, setAZUREData] = useState([]);
  const [progress, setProgress]= useState({
    AWS: {
      loading: false,
      percent: 0,
    },
    GCP: {
      loading: false,
      percent: 0,
    },
    AZURE: {
      loading: false,
      percent: 0,
    },
  });
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
  useEffect(() => {
    setWidth(window.innerWidth*0.6);
  },[window.innerWidth]);

  useEffect(() => {
    setSearchFilter({instance: '', region: '', start_date: '', end_date: ''})
    setAssoRegion();
    setAssoInstance();
    setAssoAZ();
    filterSort(vendor);
    ResetSelected();

    if (vendor && !progress[vendor].loading) {
      if (vendor === 'AWS' && Object.keys(getData) === []) {
        getLatestData(vendor, "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_aws.json", setGetdata);
      } else if (vendor === 'GCP' && GCPData.length === 0) {
        getLatestData(vendor, "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_gcp.json", setGCPData);
      } else if (vendor === 'AZURE' && AZUREData.length === 0) {
        getLatestData(vendor, "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_azure.json", setAZUREData);
      }
    }
  },[vendor])

  useEffect(() => {
    getLatestData('AWS', "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_aws.json", setGetdata);
  },[])

  useEffect(() => { //데이터 가져오기 한번 끝날때마다 한곳에 모으기
    if (SPSGraph.length>visualDate){
      for(let i=0; i<(SPGraph.length-selectedData.length);i++) {
        Object.assign(IFGraph[i], IFGraph[i+visualDate]);
        setIFGraph(IFGraph.slice(0,visualDate))
        Object.assign(SPSGraph[i], SPSGraph[i+visualDate]);
        setSPSGraph(SPSGraph.slice(0,visualDate))
        Object.assign(SPGraph[i], SPGraph[i+visualDate]);
        setSPGraph(SPGraph.slice(0,visualDate))
      }
    }
  },[graphData])

  useEffect(() => { // end_date가 max를 초과할 경우
    if (searchFilter.end_date && new Date(searchFilter.end_date) > new Date(dateRange.max)) {
      setSearchFilter({ ...searchFilter, end_date: dateRange.max });
    }
  }, [searchFilter.start_date]);

  const ResetSelected = () => {
    if (selectedData.length!==0) {
      document.querySelector('.PrivateSwitchBase-input').click();
      setSelectedData([]);
    }
  }
  //latest data 가져오기
  const getLatestData = async (curVendor, DataUrl, setLatestData) => {
    await axios({
      url: DataUrl,
      method: "GET",
      responseType: "json", // important
      onDownloadProgress: (progressEvent) => {
        let percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total); // you can use this to show user percentage of file downloaded
        setProgress({ ...progress, [curVendor]: { loading: true, percent: percentCompleted } });
      }
    }).then((response) => {
      setProgress({ ...progress, [curVendor]: { ...progress[curVendor], loading: false } });
      let responData = response.data;
      // responData.map((obj) => {
      //   Object.keys(obj).map((key) => {
      //     if (obj[key] === -1) obj[key] = "N/A";
      //   });
      // });
      setLatestData(responData);
    }).catch((err) => {
      console.log(err);
      setProgress({ ...progress, [curVendor]: { percent: 0, loading: false } });
    })
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
  //AWS columns
  const columns = [
    { field: 'id', headerName: 'ID', flex: 1, filterable: false, hide: true },
    { field: 'InstanceType', headerName: 'InstanceType', flex: 1,
      valueGetter: (params) => {
        return params.row.InstanceType == -1 ? "N/A" : params.row.InstanceType;
      }
    },
    { field: 'Region', headerName: 'Region', flex: 1, headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.Region == -1 ? "N/A" : params.row.Region;
      }
    },
    { field: 'AZ', headerName: 'AZ', flex: 0.5, type: 'number', description: 'Availability Zone ID. For details, please refer to https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.AZ == -1 ? "N/A" : params.row.AZ;
      }
    },
    { field: 'SPS', headerName: 'Availability', flex: 1.3, description: 'In AWS, it is Spot Placement Score. For details, please refer to https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-placement-score.html', type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.SPS == -1 ? "N/A" : params.row.SPS;
      }
    },
    { field: 'IF', headerName: 'Interruption Ratio', flex: 1.3, description: 'In AWS, it is Interruption-free score. For details, please refer to “Frequency of interruption” in https://aws.amazon.com/ec2/spot/instance-advisor/', type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.IF == -1 ? "N/A" : params.row.IF;
      }
    },
    { field: 'SpotPrice', headerName: 'SpotPrice ($)', type: 'number', flex: 1.3,
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.SpotPrice == -1 ? "N/A" : params.row.SpotPrice;
      }
    },
    { field: 'Savings', headerName: 'Savings (%)', flex: 1.3, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        if (!params.row.OndemandPrice || !params.row.SpotPrice) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
        else if (params.row.OndemandPrice == -1 || params.row.SpotPrice == -1) return "N/A"; // 값이 -1일 경우 (string, num...)
        let savings = Math.round((params.row.OndemandPrice - params.row.SpotPrice) / params.row.OndemandPrice * 100)
        return isNaN(savings) ? "N/A" : savings;
      }
    },
    // { field: 'SpotRank', headerName: 'SpotRank', flex: 1, type: 'number',
    //   valueGetter: (params) =>{
    //     let rank = (params.row.Savings / 100.0) * (alpha * params.row.SPS + (1-alpha) * params.row.IF)
    //     return rank.toFixed(2);
    //   }
    // },
    {field: 'Date', headerName: 'Date', type: 'date', flex: 2,
      headerAlign: 'center',
      valueGetter: (params) =>{
      if (params.row.TimeStamp) {
        const date = new Date(params.row.TimeStamp);
        let year = date.getFullYear();
        let month = "0" + (date.getMonth() + 1);
        let day = "0" + date.getDate();
        let hour = date.getHours();
        let min = date.getMinutes();
        return year + '-' + month.substr(-2) + '-' + day.substr(-2) + ' ' + "0" + hour + ':' + "0" + min;
      } else return params.row.time;
    }},
  ];
  //GCP columns
  const GCPcolumns = [
    { field: 'id', headerName: 'ID', flex: 1, filterable: false, hide: true},
    { field: 'InstanceType', headerName: 'InstanceType', flex: 1.3,
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.InstanceType == -1 ? "N/A" : params.row.InstanceType;
      }
    },
    { field: 'Region', headerName: 'Region', flex: 1,
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.Region == -1 ? "N/A" : params.row.Region;
      }
    },
    { field: 'Calculator OnDemand Price', headerName: 'OnDemand Price', flex: 1, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row['Calculator OnDemand Price'] == -1 ? "N/A" : params.row['Calculator OnDemand Price'];
      }
    },
    { field: 'Calculator Preemptible Price', headerName: 'Preemptible Price', flex: 1, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row['Calculator Preemptible Price'] == -1 ? "N/A" : params.row['Calculator Preemptible Price'];
      }
    },
    { field: 'Calculator Savings', headerName: 'Savings', flex: 1, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        if (!params.row['Calculator OnDemand Price'] || !params.row['Calculator Preemptible Price']) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
        else if (params.row['Calculator OnDemand Price'] == -1 || params.row['Calculator Preemptible Price'] == -1) return "N/A";  // 값이 -1일 경우 (string, num...)
        let savings = Math.round((params.row['Calculator OnDemand Price'] - params.row['Calculator Preemptible Price']) / params.row['Calculator OnDemand Price'] * 100)
        return isNaN(savings) ? "N/A" : savings;
      }
    },
    { field: 'VM Instance OnDemand Price', headerName: 'OnDemand Price', flex: 1, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row['VM Instance OnDemand Price'] == -1 ? "N/A" : params.row['VM Instance OnDemand Price'];
      }
    },
    { field: 'VM Instance Preemptible Price', headerName: 'Preemptible Price', flex: 1, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row['VM Instance Preemptible Price'] == -1 ? "N/A" : params.row['VM Instance Preemptible Price'];
      }
    },
    { field: 'VM Instance Savings', headerName: 'Savings', flex: 1, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        if (!params.row['VM Instance Preemptible Price'] || !params.row['VM Instance OnDemand Price']) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
        else if (params.row['VM Instance Preemptible Price'] == -1 || params.row['VM Instance OnDemand Price'] == -1) return "N/A"; // 값이 -1일 경우 (string, num...)
        let savings = Math.round((params.row['VM Instance OnDemand Price'] - params.row['VM Instance Preemptible Price']) / params.row['VM Instance OnDemand Price'] * 100)
        return isNaN(savings) ? "N/A" : savings;
      }
    },
    {field: 'time', headerName: 'Date', type: 'date', flex: 2, headerAlign: 'center', }
  ];
  const GCPcolumnGroup = [
    {
      groupId: 'Caculator',
      description: '',
      children: [{ field: 'Calculator OnDemand Price' }, {field: 'Calculator Preemptible Price'},{field: 'Calculator Savings'}],
    },
    {
      groupId: 'VM Instance',
      description: '',
      children: [{ field: 'VM Instance OnDemand Price' }, {field: 'VM Instance Preemptible Price'},{field: 'VM Instance Savings'}],
    }
  ];
  //AZURE columns
  const AZUREcolumns = [
    { field: 'id', headerName: 'ID', flex: 1, filterable: false, hide: true},
    { field: 'instanceTier', headerName: 'InstanceTier', flex: 1 ,
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.instanceTier == -1 ? "N/A" : params.row.instanceTier;
      }
    },
    { field: 'instanceType', headerName: 'InstanceType', flex: 1,
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.instanceType == -1 ? "N/A" : params.row.instanceType;
      }
    },
    { field: 'region', headerName: 'Region', flex: 1,
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.region == -1 ? "N/A" : params.row.region;
      }
    },
    { field: 'ondemandPrice', headerName: 'OndemandPrice', flex: 1, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.ondemandPrice == -1 ? "N/A" : params.row.ondemandPrice;
      }
    },
    { field: 'spotPrice', headerName: 'SpotPrice', flex: 1.3, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        return params.row.spotPrice == -1 ? "N/A" : params.row.spotPrice;
      }
    },
    { field: 'savings', headerName: 'Savings (%)', flex: 1.3, type: 'number',
      headerAlign: 'center',
      valueGetter: (params) => {
        if (!params.row.ondemandPrice || !params.row.spotPrice) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
        else if (params.row.ondemandPrice == -1 || params.row.spotPrice == -1) return "N/A"; // 값이 -1일 경우 (string, num...)
        let savings = Math.round((params.row.ondemandPrice - params.row.spotPrice) / params.row.ondemandPrice * 100)
        return isNaN(savings) ? "N/A" : savings;
      }
    },
    { field: 'time', headerName: 'Date', type: 'date', flex: 2, headerAlign: 'center' }
  ];

  const CustomToolbar = () => (
      <GridToolbarContainer>
        <GridToolbarFilterButton />
        <GridToolbarColumnsButton />
        <GridToolbarDensitySelector />
        <GridToolbarExport />
        {/*<style.toolbarText>alpha : </style.toolbarText>*/}
        {/*<style.alphaInput label="alpha" variant="outlined" name="alpha" id="alpha" placeholder="default 0.7" ref={alphaInput}/>*/}
        {/*<style.alphaBtn variant="contained" onClick={changeAlpha}>Apply</style.alphaBtn>*/}
        {vendor==='AWS' ?
          <style.alphaBtn variant="contained" onClick={showChart} vendor={vendor} loading={graphLoad}>Visualize</style.alphaBtn>
          :null }
      </GridToolbarContainer>
  );

  const changeAlpha = () => {
    const ainput = alphaInput.current.value;
    if (ainput <=0.5 || ainput>1) {
      alert('Please enter a value greater than 0.5 and less than or equal to 1.0')
    }else {
      ainput === '' ? setAlpha(0.7) : setAlpha(ainput);
    }
  };

  const showChart = () => {
    if (selectedData.length === 0){
      alert("Please select at least 1 data point")
    }
    else if (selectedData.length<=5) {
      setGraphLoad(true);
      selectedData.map(async (d, order) => {
        const params = {
          AZ: d['AZ'],
          Region: d['Region'],
          InstanceType: d['InstanceType']
        }
        await axios.get('https://9rby9fcc95.execute-api.us-west-2.amazonaws.com/default/spotrank-calc-avg', {params})
            .then((response) => {
            // console.log(response.data);
            const list = Object.keys(response.data);
              setVisualDate(list.length)
              list.map((da, idx) => {
                let name = d['InstanceType']+'/'+d['Region']+'/'+d['AZ'];
                let newIF = {name: da, [name] : Number(response.data[da]['IF'].toFixed(4))}
                setIFGraph(IFGraph => [...IFGraph, newIF])
                let newSPS = {name: da, [name] : Number(response.data[da]['SPS'].toFixed(4))}
                setSPSGraph(SPSGraph => [...SPSGraph, newSPS])
                let newSP = {name: da, [name] : Number(response.data[da]['SpotPrice'].toFixed(4))}
                setSPGraph(SPGraph => [...SPGraph, newSP])
              })
            setGraphData([...graphData, response.data]);
          }
        ).catch((e) => {
          console.log(e);
          setGraphLoad(false);
        })
        setChartModal(!chartModal);
        setGraphLoad(false);
      })
    }else {
      alert("Only 5 or less data can be visualized.\nCurrently, there are "+selectedData.length+" selected data");
    }
  };

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

  const LinearProgressWithLabel = (props) => {
    return (
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <Box sx={{ width: '100%', mr: 1 }}>
            <LinearProgress variant="determinate" {...props} />
          </Box>
          <Box sx={{ minWidth: 35 }}>
            <Typography variant="body2" color="text.secondary">{`${Math.round(props.value)}%`}</Typography>
          </Box>
        </Box>
    );
  }

  return (
    <div style={{width: '100%', height: '100%'}}>
      <style.demo>
        {/*설명 텍스트 제거*/}
        {/*<style.notice>*/}
        {/*  The current version of this website contains limited information for the HPDC 2022 submission review purpose.*/}
        {/*  <br/>*/}
        {/*  Upon acceptance of the paper, the entire datasets, features, and source code will be available.*/}
        {/*  <br/>*/}
        {/*  For the demonstration purpose, we currently provide limited historical spot instance dataset from 2022. January 15 to 2022. January 21.*/}
        {/*  <br/>*/}
        {/*  A single dataset value per each instance type and availability zone is provided daily.*/}
        {/*  <br/>*/}
        {/*  Please note that the original datasets are collected every 10 minutes.*/}
        {/*</style.notice>*/}
        {/*<style.imgDiv>*/}
        {/*  <style.calimg src={process.env.PUBLIC_URL + '/spotrank-equation.png'} />*/}
        {/*</style.imgDiv>*/}
        {/*<style.filterTitle>Fill the Boxes for take a specific value</style.filterTitle>*/}
        <style.vendor>
          <style.vendorBtn
            onClick={() => {setVendor('AWS')}}
            clicked={vendor==='AWS'}
            disabled={progress[vendor].loading}
          >
            <style.vendorIcon src={process.env.PUBLIC_URL + '/icon/awsIcon.png'} alt="awsIcon"/>
            <style.vendorTitle>Amazon Web Services</style.vendorTitle>
          </style.vendorBtn>
          <style.vendorBtn
            onClick={() => {setVendor('GCP')}}
            clicked={vendor==='GCP'}
            disabled={progress[vendor].loading}
          >
            <style.vendorIcon src={process.env.PUBLIC_URL + '/icon/gcpIcon.png'} alt="awsIcon"/>
            <style.vendorTitle>Google Cloud Platform</style.vendorTitle>
          </style.vendorBtn>
          <style.vendorBtn
            onClick={() => {setVendor('AZURE')}}
            clicked={vendor==='AZURE'}
            disabled={progress[vendor].loading}
          >
            <style.vendorIcon src={process.env.PUBLIC_URL + '/icon/azureIcon.png'} alt="awsIcon"/>
            <style.vendorTitle>Microsoft Azure</style.vendorTitle>
          </style.vendorBtn>
        </style.vendor>
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
                label="instance-tier"
                name="instance-tier"
                vendor={vendor}
              >
                {assoAZ ? assoAZ.map((e) => (
                  <style.selectItem value={e} vendor={vendor}>{e}</style.selectItem>
                )):az ? az.map((e) => (
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
        {chartModal ?
            <style.chartModal>
              <style.chartSection ref={canvasSection}>
                {/*모달 닫기 & 데이터 초기화*/}
                <style.chartClose onClick={() => {setChartModal(!chartModal); setSPGraph([]); setIFGraph([]);setSPSGraph([]);}}>X</style.chartClose>
                <style.chartTitle>History Visualizing</style.chartTitle>
                <style.chartSubTitle>Legend (InstanceType / Region / AZ)</style.chartSubTitle>
                <style.chart
                    className="firstChart"
                    width={w}
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
                    width={w}
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
                    width={w}
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
            : null}
        <style.table>
          {vendor && progress[vendor].loading &&
            <style.progressBar vendor={vendor}>
              <LinearProgressWithLabel value={progress[vendor].percent} />
              <style.noticeMsg>After the data is loaded, you can change to other vendors.</style.noticeMsg>
            </style.progressBar>}
            <style.dataTable
                rows={vendor==='AWS' ? getData : vendor === 'GCP' ? GCPData : AZUREData}
                columns={vendor==='AWS' ? columns : vendor === 'GCP' ? GCPcolumns : AZUREcolumns}
                columnGroupingModel={GCPcolumnGroup}
                experimentalFeatures={{ columnGrouping: true }}
                checkboxSelection
                onSelectionModelChange = {(newSelectionModel) => {
                    let selected = [];
                    const Data = getData
                    newSelectionModel.map((e) => {
                        selected.push(Data[e-1])
                    })
                    setSelectedData(selected);
                }}
                components={{ Toolbar: CustomToolbar }}
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
        </style.table>
      </style.demo>
    </div>
  )
}
export default Demo;