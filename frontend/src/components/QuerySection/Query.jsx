import * as style from "../../pages/home/styles";
import { FormControl } from "@mui/material";
import React, { useEffect, useState } from "react";
import axios from "axios";

const AWS_INSTANCE = {};
const AWS_REGION = {};
const AWS_AZ = {};

const AZURE_INSTANCE = {};
const AZURE_REGION = {};
const AZURE_TIER = {};

const GCP_INSTANCE = {};
const GCP_REGION = {};

// Zone ID와 Region 매핑을 동적으로 저장할 객체
const ZONE_REGION_MAP = {};

// AWS association 데이터에서 Zone ID와 Region 매핑을 동적으로 생성하는 함수
const buildZoneRegionMapping = () => {
  console.log("Building Zone-Region mapping...");
  
  // Region 이름에서 Zone prefix를 동적으로 생성하는 함수
  const regionToZonePrefix = (region) => {
    if (!region || region === "nan") return null;
    
    const parts = region.split('-');
    if (parts.length !== 3) return null;
    
    const [area, direction, number] = parts;
    
    // 기본 방향 단어들과 그 첫 글자
    const baseDirections = {
      'north': 'n',
      'south': 's', 
      'east': 'e',
      'west': 'w',
      'central': 'c'
    };
    
    // 방향 단어를 동적으로 파싱
    const parseDirection = (dir) => {
      // 단순한 방향 단어인 경우
      if (baseDirections[dir]) {
        return baseDirections[dir];
      }
      
      // 복합 방향 단어인 경우 파싱
      let result = '';
      let remaining = dir;
      
      // 가능한 모든 기본 방향 단어들을 찾아서 조합
      Object.keys(baseDirections).forEach(baseDir => {
        if (remaining.includes(baseDir)) {
          result += baseDirections[baseDir];
          remaining = remaining.replace(baseDir, '');
        }
      });
      
      return result || null;
    };
    
    const directionChar = parseDirection(direction);
    if (!directionChar) return null;
    
    // 결과: ap-southeast-3 → apse3, us-west-2 → usw2, eu-central-1 → euc1
    return area + directionChar + number;
  };
  
  // 실제 Instance 데이터에서 사용되는 모든 Zone들을 수집
  const allZones = new Set();
  Object.keys(AWS_INSTANCE).forEach(instanceType => {
    const zones = AWS_INSTANCE[instanceType].AZ;
    zones.forEach(zoneId => {
      if (zoneId && zoneId !== "nan") {
        allZones.add(zoneId);
      }
    });
  });
  
  // 실제 Instance 데이터에서 사용되는 모든 Region들을 수집
  const allRegions = new Set();
  Object.keys(AWS_INSTANCE).forEach(instanceType => {
    const regions = AWS_INSTANCE[instanceType].Region;
    regions.forEach(region => {
      if (region && region !== "nan") {
        allRegions.add(region);
      }
    });
  });
  
  // Region별로 Zone prefix 생성하고 매핑
  allRegions.forEach(region => {
    const zonePrefix = regionToZonePrefix(region);
    if (zonePrefix) {
      // 해당 prefix를 가진 Zone들을 이 Region에 매핑
      allZones.forEach(zoneId => {
        if (zoneId.startsWith(zonePrefix + '-')) {
          ZONE_REGION_MAP[zoneId] = region;
        }
      });
    }
  });
  
  console.log("Zone-Region mapping completed:", ZONE_REGION_MAP);
  console.log("Total mappings created:", Object.keys(ZONE_REGION_MAP).length);
  console.log("Sample mappings:", Object.entries(ZONE_REGION_MAP).slice(0, 10));
};

// Zone ID에서 Region을 찾는 함수
const mapZoneIdToRegion = (zoneId) => {
  if (!zoneId || zoneId === "nan") return null;
  return ZONE_REGION_MAP[zoneId] || null;
};

const Query = ({
  vendor,
  selectedData,
  setSelectedData,
  setGetdata,
  setGCPData,
  setAZUREData,
  setSnackbar,
}) => {
  const url = "https://d26bk4799jlxhe.cloudfront.net/query-api/";
  const [load, setLoad] = useState(false);
  const [region, setRegion] = useState();
  const [az, setAZ] = useState();
  const [instance, setInstance] = useState();
  const [assoRegion, setAssoRegion] = useState();
  const [assoInstance, setAssoInstance] = useState();
  const [assoAZ, setAssoAZ] = useState();
  const [searchFilter, setSearchFilter] = useState({
    instance: "",
    region: "",
    az: "",
    start_date: "",
    end_date: "",
  });
  const [dateRange, setDateRange] = useState({
    min: null,
    max: new Date().toISOString().split("T")[0],
  });

  const setDate = (name, value) => {
    const tmpMax = new Date(value);
    const today = new Date();
    tmpMax.setMonth(tmpMax.getMonth() + 1);
    if (tmpMax < today) {
      setDateRange({ ...dateRange, max: tmpMax.toISOString().split("T")[0] });
    } else
      setDateRange({ ...dateRange, max: today.toISOString().split("T")[0] });
  };
  const filterSort = (V) => {
    // V : vendor
    if (V === "AWS") {
      setInstance(Object.keys(AWS_INSTANCE));
      setRegion(["ALL", ...Object.keys(AWS_REGION)]);
      setAZ(["ALL"]);
    } else if (V === "AZURE") {
      setInstance(Object.keys(AZURE_INSTANCE));
      setRegion(["ALL", ...Object.keys(AZURE_REGION)]);
    } else {
      // GCP
      setInstance(Object.keys(GCP_INSTANCE));
      setRegion(["ALL", ...Object.keys(GCP_REGION)]);
    }
  };
  const setFilter = ({ target }) => {
    //filter value 저장
    const { name, value } = target;

    // 날짜가 입력 될 경우
    if (name.includes("start_date")) setDate(name, value);
    
    // AWS의 순차적 선택 제어
    if (vendor === "AWS") {
      if (name === "instance") {
        if (value && value !== "ALL") {
          // Instance 선택 시 해당 Instance가 사용 가능한 Region 설정
          let includeRegion = [...AWS_INSTANCE[value]["Region"]];
          setAssoRegion(["ALL"].concat(includeRegion));
          
          // 기존 Region이 새로 선택된 Instance에서 사용 가능한지 확인
          const currentRegion = searchFilter.region;
          const isRegionStillValid = currentRegion && currentRegion !== "ALL" && includeRegion.includes(currentRegion);
          
          if (isRegionStillValid) {
            // 기존 Region이 여전히 유효하면 해당 Region에 맞는 AZ만 표시
            const availableAZs = AWS_INSTANCE[value]["AZ"];
            const regionSpecificAZs = availableAZs.filter(zoneId => {
              const azRegion = mapZoneIdToRegion(zoneId);
              return azRegion === currentRegion;
            });
            setAssoAZ(["ALL", ...regionSpecificAZs]);
            setSearchFilter({
              ...searchFilter,
              [name]: value,
              az: "" // AZ만 초기화
            });
          } else {
            // 기존 Region이 유효하지 않으면 Region과 AZ 모두 초기화
            setAssoAZ(["ALL"]);
            setSearchFilter({
              ...searchFilter,
              [name]: value,
              region: "",
              az: ""
            });
          }
        } else {
          setAssoRegion(["ALL"]);
          setAssoAZ(["ALL"]);
          setSearchFilter({
            ...searchFilter,
            [name]: value,
            region: "",
            az: ""
          });
        }
        return;
      } else if (name === "region") {
        // Region 변경 시 AZ 초기화
        setSearchFilter({
          ...searchFilter,
          [name]: value,
          az: ""
        });
        
        if (value && value !== "ALL" && searchFilter.instance && searchFilter.instance !== "ALL") {
          // Instance와 Region이 모두 선택된 경우 AZ 설정
          try {
            const selectedInstance = searchFilter.instance;
            const availableAZs = AWS_INSTANCE[selectedInstance]["AZ"];
            
            console.log("Region selection debug:");
            console.log("Selected instance:", selectedInstance);
            console.log("Selected region:", value);
            console.log("Available AZs for instance:", availableAZs);
            console.log("Current ZONE_REGION_MAP:", ZONE_REGION_MAP);
            
            // 선택된 Region에 속하면서 해당 Instance에서 사용 가능한 AZ 필터링
            const regionSpecificAZs = availableAZs.filter(zoneId => {
              const azRegion = mapZoneIdToRegion(zoneId);
              console.log(`Zone ${zoneId} maps to region ${azRegion}, target region: ${value}`);
              return azRegion === value;
            });
            
            console.log("Filtered AZs for region:", regionSpecificAZs);
            
            if (regionSpecificAZs.length > 0) {
              setAssoAZ(["ALL", ...regionSpecificAZs]);
            } else {
              setAssoAZ(["ALL"]);
            }
          } catch (e) {
            console.log("Error in AZ filtering:", e);
            setAssoAZ(["ALL"]);
          }
        } else {
          setAssoAZ(["ALL"]);
        }
        return;
      } else if (name === "az") {
        setSearchFilter({ ...searchFilter, [name]: value });
        return;
      }
    }

    setSearchFilter({ ...searchFilter, [name]: value });
    if (value !== "ALL") {
      if (name === "region" && region.includes(value)) {
        if (vendor === "AZURE") {
          setAssoInstance([...AZURE_REGION[value]]);
        } else {
          //gcp
          setAssoInstance([...GCP_REGION[value].Instance]);
        }
      } else if (name === "instance") {
        let includeRegion = [];
        if (vendor === "AZURE") {
          includeRegion = [...AZURE_INSTANCE[value]["Region"]];
        } else {
          // gcp
          includeRegion = [...GCP_INSTANCE[value]];
        }
        setAssoRegion(["ALL"].concat(includeRegion));
      }
    } else {
      if (name === "region") {
        setAssoAZ(["ALL"]);
      }
    }
  };
  const querySubmit = async () => {
    // 쿼리를 날리기 전에 searchFilter에 있는 값들이 비어있지 않은지 확인.
    const invalidQuery = Object.keys(searchFilter)
      .map((data) => {
        if (!searchFilter[data]) return false;
      })
      .includes(false);
    const invalidQueryForVendor =
      vendor === "AWS" && !Boolean(searchFilter?.az);
    if (invalidQuery || invalidQueryForVendor) {
      setSnackbar({
        open: true,
        message: "The query is invalid. \nPlease check your search option.",
        severity: "error",
      });
      return;
    }
    //start_date , end_date 비교 후 start_date가 end_date보다 이전일 경우에만 데이터 요청
    if (searchFilter["start_date"] <= searchFilter["end_date"]) {
      // button load True로 설정
      setLoad(true);
      //guery 요청시 들어가는 Params, params의 값은 searchFilter에 저장되어 있음
      const params = {
        TableName: vendor.toLowerCase(),
        ...(vendor === "AWS" && {
          AZ: searchFilter["az"] === "ALL" ? "*" : searchFilter["az"],
        }),
        Region: searchFilter["region"] === "ALL" ? "*" : searchFilter["region"],
        InstanceType:
          searchFilter["instance"] === "ALL" ? "*" : searchFilter["instance"],
        ...(vendor === "AZURE" && {
          InstanceTier: "*", // 항상 ALL로 설정
          AvailabilityZone:
            searchFilter["az"] === "ALL" ? "*" : searchFilter["az"],
        }),
        Start:
          searchFilter["start_date"] === "" ? "*" : searchFilter["start_date"],
        End: searchFilter["end_date"] === "" ? "*" : searchFilter["end_date"],
      };

      await axios
        .get(url, { params })
        .then((res) => {
          if (res.data.Status === 403) {
            setSnackbar({
              open: true,
              message: "Invalid Access",
              severity: "error",
            });
          } else if (res.data.Status === 500) {
            setSnackbar({
              open: true,
              message: "Internal Server Error",
              severity: "error",
            });
          } else {
            // Status 성공 시,
            let parseData = res.data.Data;
            const setQueryData =
              vendor === "AWS"
                ? setGetdata
                : vendor === "GCP"
                ? setGCPData
                : setAZUREData;
            setQueryData(parseData);
            let dataCnt = parseData.length;
            if (dataCnt < 20000) {
              setSnackbar({
                open: true,
                message: `Total ${dataCnt} data points have been returned`,
                severity: "success",
              });
            } else if (dataCnt === 20000) {
              setSnackbar({
                open: true,
                message:
                  "The maximum number of data points has been returned (20,000)",
                severity: "warning",
              });
            }
          }
          // button load false로 설정
          setLoad(false);
        })
        .catch((e) => {
          setLoad(false);
          console.log(e);
          if (e.message === "Network Error") {
            setSnackbar({
              open: true,
              message: "A network error occurred. Try it again.",
              severity: "error",
            });
          }
        });
    } else {
      setSnackbar({
        open: true,
        message:
          "The date range for the query is invalid. Please set the date correctly.",
        severity: "error",
      });
    }
  };
  const ResetSelected = () => {
    if (selectedData.length !== 0) {
      document.querySelector(".PrivateSwitchBase-input").click();
      setSelectedData([]);
    }
  };

  const setFilterData = async () => {
    // fecth Query Association JSON
    let assoAWS = await axios.get(
      "https://d26bk4799jlxhe.cloudfront.net/query-selector/associated/association_aws.json"
    );
    let assoAzure = await axios.get(
      "https://d26bk4799jlxhe.cloudfront.net/query-selector/associated/association_azure.json"
    );
    let assoGCP = await axios.get(
      "https://d26bk4799jlxhe.cloudfront.net/query-selector/associated/association_gcp.json"
    );
    if (assoAWS && assoAWS.data) {
      assoAWS = assoAWS.data[0];
      console.log("AWS data loaded:", Object.keys(assoAWS).length, "instances");
      
      Object.keys(assoAWS).map((instance) => {
        AWS_INSTANCE[instance] = {
          ...assoAWS[instance],
          Region: assoAWS[instance]["Region"].filter(
            (region) => region !== "nan"
          ),
          AZ: assoAWS[instance]["AZ"].filter((AZ) => AZ !== "nan"),
        };
        assoAWS[instance]["Region"].map((region) => {
          if (region === "nan") return;
          if (!AWS_REGION[region]) AWS_REGION[region] = new Set();
          AWS_REGION[region].add(instance);
        });
        assoAWS[instance]["AZ"].map((az) => {
          if (az === "nan") return;
          if (!AWS_AZ[az]) AWS_AZ[az] = new Set();
          AWS_AZ[az].add(instance);
        });
      });
      
      console.log("AWS_INSTANCE populated:", Object.keys(AWS_INSTANCE).length, "instances");
      console.log("Sample instance data:", AWS_INSTANCE[Object.keys(AWS_INSTANCE)[0]]);
      
      // AWS 데이터 로드 완료 후 Zone-Region 매핑 생성
      buildZoneRegionMapping();
    } else {
      console.error("Failed to load AWS association data");
    }
    
    if (assoAzure && assoAzure.data) {
      assoAzure = assoAzure.data[0];
      Object.keys(assoAzure).map((instance) => {
        AZURE_INSTANCE[instance] = {
          ...assoAzure[instance],
          Region: assoAzure[instance]["Region"].filter(
            (region) => region !== "nan"
          ),
        };
        assoAzure[instance]["Region"].map((region) => {
          if (region === "nan") return;
          if (!AZURE_REGION[region]) AZURE_REGION[region] = new Set();
          AZURE_REGION[region].add(instance);
        });
      });
    }
    if (assoGCP && assoGCP.data) {
      assoGCP = assoGCP.data[0];
      assoGCP.map((obj) => {
        let region = Object.keys(obj)[0];
        GCP_REGION[region] = {
          Instance: obj[region],
        };
        obj[region].map((instance) => {
          if (!GCP_INSTANCE[instance]) GCP_INSTANCE[instance] = new Set();
          GCP_INSTANCE[instance].add(region);
        });
      });
    }
    filterSort(vendor);
  };

  useEffect(() => {
    setFilterData();
  }, []);
  useEffect(() => {
    const today = new Date();
    const yesterday = new Date();
    yesterday.setDate(today.getDate() - 1);
    setSearchFilter({
      instance: "",
      region: "",
      az: "",
      start_date: yesterday.toISOString().split("T")[0],
      end_date: today.toISOString().split("T")[0],
    });
    setAssoRegion();
    setAssoInstance();
    setAssoAZ(["ALL"]);
    filterSort(vendor);
    ResetSelected();
  }, [vendor]);

  useEffect(() => {
    // end_date가 max를 초과할 경우
    if (
      searchFilter.end_date &&
      new Date(searchFilter.end_date) > new Date(dateRange.max)
    ) {
      setSearchFilter({ ...searchFilter, end_date: dateRange.max });
    }
  }, [searchFilter.start_date]);

  return (
    <style.tablefilter vendor={vendor}>
      <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }}>
        <style.filterLabel id="instance-input-label" vendor={vendor}>
          Instance
        </style.filterLabel>
        <style.filterSelect
          labelId="instance-input-label"
          id="instance-input"
          value={searchFilter["instance"]}
          onChange={setFilter}
          label="Instance"
          name="instance"
          vendor={vendor}
        >
          {assoInstance
            ? assoInstance.map((e) => (
                <style.selectItem key={e} value={e} vendor={vendor}>
                  {e}
                </style.selectItem>
              ))
            : instance
            ? instance.map((e) => (
                <style.selectItem key={e} value={e} vendor={vendor}>
                  {e}
                </style.selectItem>
              ))
            : null}
        </style.filterSelect>
      </FormControl>
      <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }}>
        <style.filterLabel id="region-input-label" vendor={vendor}>
          Region
        </style.filterLabel>
        <style.filterSelect
          labelId="region-input-label"
          id="region-input"
          value={searchFilter["region"]}
          onChange={setFilter}
          label="Region"
          name="region"
          vendor={vendor}
          disabled={vendor === "AWS" && (!searchFilter["instance"] || searchFilter["instance"] === "")}
        >
          {assoRegion
            ? assoRegion.map((e) => (
                <style.selectItem key={e} value={e} vendor={vendor}>
                  {e}
                </style.selectItem>
              ))
            : region
            ? region.map((e) => (
                <style.selectItem key={e} value={e} vendor={vendor}>
                  {e}
                </style.selectItem>
              ))
            : null}
        </style.filterSelect>
      </FormControl>
      {vendor === "AWS" ? (
        <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }}>
          <style.filterLabel id="az-input-label" vendor={vendor}>
            AZ
          </style.filterLabel>
          <style.filterSelect
            labelId="az-input-label"
            id="az-input"
            value={searchFilter["az"]}
            onChange={setFilter}
            label="AZ"
            name="az"
            vendor={vendor}
            disabled={!searchFilter["region"] || searchFilter["region"] === ""}
          >
            {assoAZ
              ? assoAZ.map((e) => (
                  <style.selectItem key={e} value={e} vendor={vendor}>
                    {e}
                  </style.selectItem>
                ))
              : az
              ? az.map((e) => (
                  <style.selectItem key={e} value={e} vendor={vendor}>
                    {e}
                  </style.selectItem>
                ))
              : null}
          </style.filterSelect>
        </FormControl>
      ) : null}
      {vendor === "AZURE" && (
        <>
          <FormControl variant="standard" sx={{ m: 1, minWidth: 120 }}>
            <style.filterLabel id="az-input-label" vendor={vendor}>
              AZ
            </style.filterLabel>
            <style.filterSelect
              labelId="az-input-label"
              id="az-input"
              value={searchFilter["az"] ?? "ALL"}
              onChange={setFilter}
              label="AZ"
              name="az"
              vendor={vendor}
            >
              {["ALL", 1, 2, 3, "Single"].map((e) => (
                <style.selectItem key={e} value={e} vendor={vendor}>
                  {e}
                </style.selectItem>
              ))}
            </style.filterSelect>
          </FormControl>
        </>
      )}
      <FormControl
        variant="standard"
        sx={{ m: 1, minWidth: 135 }}
        className="date-input"
      >
        <style.dataLabel htmlFor="start_date-input">
          Start date :{" "}
        </style.dataLabel>
        <input
          type="date"
          id="start_date-input"
          name="start_date"
          onChange={setFilter}
          value={searchFilter.start_date}
          max={new Date().toISOString().split("T")[0]}
        />
      </FormControl>
      <FormControl
        variant="standard"
        sx={{ m: 1, minWidth: 135 }}
        className="date-input"
      >
        <style.dataLabel htmlFor="end_date-input">End date : </style.dataLabel>
        <input
          type="date"
          id="end_date-input"
          name="end_date"
          onChange={setFilter}
          value={searchFilter.end_date}
          max={dateRange.max}
        />
      </FormControl>
      <style.chartBtn onClick={querySubmit} vendor={vendor} loading={load}>
        Query
      </style.chartBtn>
      {/*{load?<ReactLoading type='spin' height='30px' width='30px' color='#1876d2' /> : null}*/}
    </style.tablefilter>
  );
};
export default Query;
