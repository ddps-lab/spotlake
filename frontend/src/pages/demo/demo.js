import React, {useEffect, useRef, useState} from "react";
import axios from 'axios';
import * as style from './styles';
import LinearProgress from '@mui/material/LinearProgress';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import DataTable from "../../components/DataTable/DataTable";
import ChartModal from "../../components/Modal/ChartModal";
import CustomToolbar from "../../components/DataTable/ToolBar";
import Query from "../../components/QuerySection/Query";

function Demo () {
  const [w, setWidth] = useState(window.innerWidth*0.6);
  const [chartModal, setChartModal] = useState(false);
  const [getData, setGetdata] = useState([]);
  const [IFGraph, setIFGraph] = useState([]);
  const [SPSGraph, setSPSGraph] = useState([]);
  const [SPGraph, setSPGraph] = useState([]);
  const [alpha, setAlpha] = useState(0.7);
  const alphaInput = useRef();
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
  useEffect(() => {
    setWidth(window.innerWidth*0.6);
  },[window.innerWidth]);


  useEffect(() => {
    getLatestData('AWS', "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_aws.json", setGetdata);
  },[])

  useEffect(() => {
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
      setLatestData(responData);
    }).catch((err) => {
      console.log(err);
      setProgress({ ...progress, [curVendor]: { percent: 0, loading: false } });
    })
  }

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
        <Query
            vendor={vendor}
            selectedData={selectedData}
            setSelectedData={setSelectedData}
            setGetdata={setGetdata}
            setGCPData={setGCPData}
            setAZUREData={setAZUREData}
        />
        {chartModal &&
          <ChartModal
            width={w}
            setChartModal={setChartModal}
            selectedData={selectedData}
            IFGraph={IFGraph}
            setIFGraph={setIFGraph}
            SPSGraph={SPSGraph}
            setSPSGraph={setSPSGraph}
            SPGraph={SPGraph}
            setSPGraph={setSPSGraph}
          />}
        <style.table>
          {vendor && progress[vendor].loading &&
            <style.progressBar vendor={vendor}>
              <LinearProgressWithLabel value={progress[vendor].percent} />
              <style.noticeMsg>After the data is loaded, you can change to other vendors.</style.noticeMsg>
            </style.progressBar>}
            <DataTable
                rowData={vendor==='AWS' ? getData : vendor === 'GCP' ? GCPData : AZUREData}
                vendor={vendor}
                toolBar={<CustomToolbar
                    addTool={vendor==='AWS' && <style.alphaBtn variant="contained"
                                                               onClick={showChart}
                                                               vendor={vendor}
                                                               loading={graphLoad}
                                                >Visualize</style.alphaBtn>
                    } />}
                setSelectedData={setSelectedData}
            />
        </style.table>
      </style.demo>
    </div>
  )
}
export default Demo;