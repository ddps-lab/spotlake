import React, { useEffect, useRef, useState } from "react";
import axios from "axios";
import * as style from "./styles";
import LinearProgress from "@mui/material/LinearProgress";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import DataTable from "../../components/DataTable/DataTable";
import ChartModal from "../../components/Modal/ChartModal";
import CustomToolbar from "../../components/DataTable/ToolBar";
import Query from "../../components/QuerySection/Query";
import { Snackbar, Alert } from "@mui/material";

function Home() {
  const [w, setWidth] = useState(window.innerWidth * 0.6);
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
  const [vendor, setVendor] = useState("AWS");
  const [GCPData, setGCPData] = useState([]);
  const [AZUREData, setAZUREData] = useState([]);
  const [progress, setProgress] = useState({
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
  const [snackbar, setSnackbar] = useState({
    open: false,
    message: "",
    severity: "error",
  });

  useEffect(() => {
    setWidth(window.innerWidth * 0.6);
  }, [window.innerWidth]);

  useEffect(() => {
    getLatestData(
      "AWS",
      "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_aws.json",
      setGetdata
    );
  }, []);

  useEffect(() => {
    if (vendor && !progress[vendor].loading) {
      if (vendor === "AWS" && Object.keys(getData).length === 0) {
        getLatestData(
          vendor,
          "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_aws.json",
          setGetdata
        );
      } else if (vendor === "GCP" && GCPData.length === 0) {
        getLatestData(
          vendor,
          "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_gcp.json",
          setGCPData
        );
      } else if (vendor === "AZURE" && AZUREData.length === 0) {
        getLatestData(
          vendor,
          "https://spotlake.s3.us-west-2.amazonaws.com/latest_data/latest_azure.json",
          setAZUREData
        );
      }
    }
  }, [vendor]);
  useEffect(() => {
    //데이터 가져오기 한번 끝날때마다 한곳에 모으기
    if (SPSGraph.length > visualDate) {
      for (let i = 0; i < SPGraph.length - selectedData.length; i++) {
        Object.assign(IFGraph[i], IFGraph[i + visualDate]);
        setIFGraph(IFGraph.slice(0, visualDate));
        Object.assign(SPSGraph[i], SPSGraph[i + visualDate]);
        setSPSGraph(SPSGraph.slice(0, visualDate));
        Object.assign(SPGraph[i], SPGraph[i + visualDate]);
        setSPGraph(SPGraph.slice(0, visualDate));
      }
    }
  }, [graphData]);

  //latest data 가져오기
  const getLatestData = async (curVendor, DataUrl, setLatestData) => {
    await axios({
      url: DataUrl,
      method: "GET",
      responseType: "json", // important
      onDownloadProgress: (progressEvent) => {
        let percentCompleted = Math.round(
          (progressEvent.loaded * 100) / progressEvent.total
        ); // you can use this to show user percentage of file downloaded
        setProgress({
          ...progress,
          [curVendor]: { loading: true, percent: percentCompleted },
        });
      },
    })
      .then((response) => {
        setProgress({
          ...progress,
          [curVendor]: { ...progress[curVendor], loading: false },
        });

        // 응답 값 검증
        if (response.status !== 200) {
          throw new Error(`Network Error ${response.status}`);
        }

        // responData 대한 검증
        let responData = response.data;
        if (responData && Array.isArray(responData)) {
          setLatestData(responData);
        } else {
          throw new Error("Invalid response data");
        }
      })
      .catch((err) => {
        console.error(err);
        setProgress({
          ...progress,
          [curVendor]: { percent: 0, loading: false },
        });
        setSnackbar({
          open: true,
          message: err.message,
          severity: "error",
        });
      });
  };

  const changeAlpha = () => {
    const ainput = alphaInput.current.value;
    if (ainput <= 0.5 || ainput > 1) {
      alert(
        "Please enter a value greater than 0.5 and less than or equal to 1.0"
      );
    } else {
      ainput === "" ? setAlpha(0.7) : setAlpha(ainput);
    }
  };

  const LinearProgressWithLabel = (props) => {
    return (
      <Box sx={{ display: "flex", alignItems: "center" }}>
        <Box sx={{ width: "100%", mr: 1 }}>
          <LinearProgress variant="determinate" {...props} />
        </Box>
        <Box sx={{ minWidth: 35 }}>
          <Typography variant="body2" color="text.secondary">{`${Math.round(
            props.value
          )}%`}</Typography>
        </Box>
      </Box>
    );
  };

  return (
    <div style={{ width: "100%", height: "100%" }}>
      <style.demo>
        <style.vendor>
          <style.vendorBtn
            onClick={() => {
              setVendor("AWS");
            }}
            clicked={vendor === "AWS"}
            disabled={progress[vendor].loading}
          >
            <style.vendorIcon
              src={process.env.PUBLIC_URL + "/icon/awsIcon.png"}
              alt="awsIcon"
            />
            <style.vendorTitle>Amazon Web Services</style.vendorTitle>
          </style.vendorBtn>
          <style.vendorBtn
            onClick={() => {
              setVendor("GCP");
            }}
            clicked={vendor === "GCP"}
            disabled={progress[vendor].loading}
          >
            <style.vendorIcon
              src={process.env.PUBLIC_URL + "/icon/gcpIcon.png"}
              alt="awsIcon"
            />
            <style.vendorTitle>Google Cloud Platform</style.vendorTitle>
          </style.vendorBtn>
          <style.vendorBtn
            onClick={() => {
              setVendor("AZURE");
            }}
            clicked={vendor === "AZURE"}
            disabled={progress[vendor].loading}
          >
            <style.vendorIcon
              src={process.env.PUBLIC_URL + "/icon/azureIcon.png"}
              alt="awsIcon"
            />
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
        {chartModal && (
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
          />
        )}
        <style.table>
          {vendor && progress[vendor].loading && (
            <style.progressBar vendor={vendor}>
              <LinearProgressWithLabel value={progress[vendor].percent} />
              <style.noticeMsg>
                After the data is loaded, you can change to other vendors.
              </style.noticeMsg>
            </style.progressBar>
          )}
          <DataTable
            rowData={
              vendor === "AWS"
                ? getData
                : vendor === "GCP"
                ? GCPData
                : AZUREData
            }
            vendor={vendor}
            toolBar={<CustomToolbar />}
            setSelectedData={setSelectedData}
          />
        </style.table>
      </style.demo>
      {/* Snackbar component displays error messages. */}
      <Snackbar
        open={snackbar.open}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
        autoHideDuration={5000}
        onClose={() => setSnackbar((prev) => ({ ...prev, open: false }))}
      >
        <Alert severity={snackbar.severity}>{snackbar.message}</Alert>
      </Snackbar>
    </div>
  );
}
export default Home;
