"use client"

import React, { useEffect, useState } from "react"
import axios from "axios"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent } from "@/components/ui/card"
import { DatePicker } from "@/components/ui/date-picker"


const AWS_INSTANCE: any = {}
const AWS_REGION: any = {}
const AWS_AZ: any = {}

const AZURE_INSTANCE: any = {}
const AZURE_REGION: any = {}

const GCP_INSTANCE: any = {}
const GCP_REGION: any = {}

const ZONE_REGION_MAP: any = {}

const buildZoneRegionMapping = () => {
  const regionToZonePrefix = (region: string) => {
    if (!region || region === "nan") return null
    const parts = region.split("-")
    if (parts.length !== 3) return null
    const [area, direction, number] = parts
    const baseDirections: any = {
      north: "n",
      south: "s",
      east: "e",
      west: "w",
      central: "c",
    }
    const parseDirection = (dir: string) => {
      if (baseDirections[dir]) return baseDirections[dir]
      let result = ""
      let remaining = dir
      Object.keys(baseDirections).forEach((baseDir) => {
        if (remaining.includes(baseDir)) {
          result += baseDirections[baseDir]
          remaining = remaining.replace(baseDir, "")
        }
      })
      return result || null
    }
    const directionChar = parseDirection(direction)
    if (!directionChar) return null
    return area + directionChar + number
  }

  const allZones = new Set<string>()
  Object.keys(AWS_INSTANCE).forEach((instanceType) => {
    const zones = AWS_INSTANCE[instanceType].AZ
    zones.forEach((zoneId: string) => {
      if (zoneId && zoneId !== "nan") allZones.add(zoneId)
    })
  })

  const allRegions = new Set<string>()
  Object.keys(AWS_INSTANCE).forEach((instanceType) => {
    const regions = AWS_INSTANCE[instanceType].Region
    regions.forEach((region: string) => {
      if (region && region !== "nan") allRegions.add(region)
    })
  })

  allRegions.forEach((region) => {
    const zonePrefix = regionToZonePrefix(region)
    if (zonePrefix) {
      allZones.forEach((zoneId) => {
        if (zoneId.startsWith(zonePrefix + "-")) {
          ZONE_REGION_MAP[zoneId] = region
        }
      })
    }
  })
}

const mapZoneIdToRegion = (zoneId: string) => {
  if (!zoneId || zoneId === "nan") return null
  return ZONE_REGION_MAP[zoneId] || null
}

interface QuerySectionProps {
  vendor: string
  onDataFetch: (data: any[], filters: { start: string, end: string, region: string }) => void
  setLoading: (loading: boolean) => void
}

const TITANS_ENDPOINT = "https://l641q7r2rb.execute-api.us-west-2.amazonaws.com"

export function QuerySection({ vendor, onDataFetch, setLoading }: QuerySectionProps) {
  const url = "https://d26bk4799jlxhe.cloudfront.net/query-api/"
  const [instance, setInstance] = useState<string[]>([])
  const [region, setRegion] = useState<string[]>([])
  const [az, setAZ] = useState<string[]>([])
  
  const [assoRegion, setAssoRegion] = useState<string[] | undefined>()
  const [assoInstance, setAssoInstance] = useState<string[] | undefined>()
  const [assoAZ, setAssoAZ] = useState<string[] | undefined>(["ALL"])

  const [searchFilter, setSearchFilter] = useState({
    instance: "",
    region: "",
    az: "",
    start_date: "",
    end_date: "",
  })

  const [startDate, setStartDate] = useState<Date | undefined>()
  const [endDate, setEndDate] = useState<Date | undefined>()

  const [dateRange, setDateRange] = useState({
    min: "",
    max: new Date().toISOString().split("T")[0],
  })

  const filterSort = (V: string) => {
    if (V === "AWS") {
      setInstance(Object.keys(AWS_INSTANCE))
      setRegion(["ALL", ...Object.keys(AWS_REGION)])
      setAZ(["ALL"])
    } else if (V === "AZURE") {
      setInstance(Object.keys(AZURE_INSTANCE))
      setRegion(["ALL", ...Object.keys(AZURE_REGION)])
    } else {
      setInstance(Object.keys(GCP_INSTANCE))
      setRegion(["ALL", ...Object.keys(GCP_REGION)])
    }
  }

  const setFilterData = async () => {
    try {
      let assoAWS = await axios.get(
        "https://d26bk4799jlxhe.cloudfront.net/query-selector/associated/association_aws.json"
      )
      let assoAzure = await axios.get(
        "https://d26bk4799jlxhe.cloudfront.net/query-selector/associated/association_azure.json"
      )
      let assoGCP = await axios.get(
        "https://d26bk4799jlxhe.cloudfront.net/query-selector/associated/association_gcp.json"
      )

      if (assoAWS && assoAWS.data) {
        const awsData = assoAWS.data[0]
        Object.keys(awsData).map((inst) => {
          AWS_INSTANCE[inst] = {
            ...awsData[inst],
            Region: awsData[inst]["Region"].filter((r: string) => r !== "nan"),
            AZ: awsData[inst]["AZ"].filter((a: string) => a !== "nan"),
          }
          awsData[inst]["Region"].map((r: string) => {
            if (r === "nan") return
            if (!AWS_REGION[r]) AWS_REGION[r] = new Set()
            AWS_REGION[r].add(inst)
          })
          awsData[inst]["AZ"].map((a: string) => {
            if (a === "nan") return
            if (!AWS_AZ[a]) AWS_AZ[a] = new Set()
            AWS_AZ[a].add(inst)
          })
        })
        buildZoneRegionMapping()
      }

      if (assoAzure && assoAzure.data) {
        const azureData = assoAzure.data[0]
        Object.keys(azureData).map((inst) => {
          AZURE_INSTANCE[inst] = {
            ...azureData[inst],
            Region: azureData[inst]["Region"].filter((r: string) => r !== "nan"),
          }
          azureData[inst]["Region"].map((r: string) => {
            if (r === "nan") return
            if (!AZURE_REGION[r]) AZURE_REGION[r] = new Set()
            AZURE_REGION[r].add(inst)
          })
        })
      }

      if (assoGCP && assoGCP.data) {
        const gcpData = assoGCP.data[0]
        gcpData.map((obj: any) => {
          let r = Object.keys(obj)[0]
          GCP_REGION[r] = {
            Instance: obj[r],
          }
          obj[r].map((inst: string) => {
            if (!GCP_INSTANCE[inst]) GCP_INSTANCE[inst] = new Set()
            GCP_INSTANCE[inst].add(r)
          })
        })
      }
      filterSort(vendor)
    } catch (error) {
      console.error("Failed to fetch association data", error)
    }
  }

  useEffect(() => {
    setFilterData()
  }, [])

  useEffect(() => {
    const today = new Date()
    const yesterday = new Date()
    yesterday.setDate(today.getDate() - 1)
    setStartDate(yesterday)
    setEndDate(today)
    setSearchFilter({
      instance: "",
      region: "",
      az: "",
      start_date: yesterday.toISOString().split("T")[0],
      end_date: today.toISOString().split("T")[0],
    })
    setAssoRegion(undefined)
    setAssoInstance(undefined)
    setAssoAZ(["ALL"])
    filterSort(vendor)
  }, [vendor])

  const handleFilterChange = (name: string, value: string) => {
    if (name === "start_date") {
        const tmpMax = new Date(value)
        const today = new Date()
        tmpMax.setMonth(tmpMax.getMonth() + 1)
        if (tmpMax < today) {
            setDateRange({ ...dateRange, max: tmpMax.toISOString().split("T")[0] })
        } else {
            setDateRange({ ...dateRange, max: today.toISOString().split("T")[0] })
        }
    }

    if (vendor === "AWS") {
      if (name === "instance") {
        if (value && value !== "ALL") {
          if (!AWS_INSTANCE[value]) {
             console.error(`Instance type ${value} not found in AWS_INSTANCE`)
             return
          }
          let includeRegion = [...AWS_INSTANCE[value]["Region"]]
          setAssoRegion(["ALL"].concat(includeRegion))
          
          const currentRegion = searchFilter.region
          const isRegionStillValid = currentRegion && currentRegion !== "ALL" && includeRegion.includes(currentRegion)
          
          if (isRegionStillValid) {
            const availableAZs = AWS_INSTANCE[value]["AZ"]
            const regionSpecificAZs = availableAZs.filter((zoneId: string) => {
              const azRegion = mapZoneIdToRegion(zoneId)
              return azRegion === currentRegion
            })
            setAssoAZ(["ALL", ...regionSpecificAZs])
            setSearchFilter((prev) => ({ ...prev, [name]: value, az: "" }))
          } else {
            setAssoAZ(["ALL"])
            setSearchFilter((prev) => ({ ...prev, [name]: value, region: "", az: "" }))
          }
        } else {
          setAssoRegion(["ALL"])
          setAssoAZ(["ALL"])
          setSearchFilter((prev) => ({ ...prev, [name]: value, region: "", az: "" }))
        }
        return
      } else if (name === "region") {
        setSearchFilter((prev) => ({ ...prev, [name]: value, az: "" }))
        if (value && value !== "ALL" && searchFilter.instance && searchFilter.instance !== "ALL") {
            try {
                const selectedInstance = searchFilter.instance
                const availableAZs = AWS_INSTANCE[selectedInstance]["AZ"]
                const regionSpecificAZs = availableAZs.filter((zoneId: string) => {
                    const azRegion = mapZoneIdToRegion(zoneId)
                    return azRegion === value
                })
                if (regionSpecificAZs.length > 0) {
                    setAssoAZ(["ALL", ...regionSpecificAZs])
                } else {
                    setAssoAZ(["ALL"])
                }
            } catch (e) {
                setAssoAZ(["ALL"])
            }
        } else {
            setAssoAZ(["ALL"])
        }
        return
      } else if (name === "az") {
          setSearchFilter((prev) => ({ ...prev, [name]: value }))
          return
      }
    }

    setSearchFilter((prev) => ({ ...prev, [name]: value }))
    
    if (value !== "ALL") {
        if (name === "region" && region.includes(value)) {
            if (vendor === "AZURE") {
                setAssoInstance([...AZURE_REGION[value]])
            } else if (GCP_REGION[value]) {
                setAssoInstance([...GCP_REGION[value].Instance])
            }
        } else if (name === "instance") {
            let includeRegion: string[] = []
            if (vendor === "AZURE") {
                includeRegion = [...AZURE_INSTANCE[value]["Region"]]
            } else if (GCP_INSTANCE[value]) {
                includeRegion = [...GCP_INSTANCE[value]]
            }
            setAssoRegion(["ALL"].concat(includeRegion))
        }
    } else {
        if (name === "region") {
            setAssoAZ(["ALL"])
        }
    }
  }

  const querySubmit = async () => {
    const invalidQuery = Object.keys(searchFilter).some((key) => {
        if (key === 'az' && vendor !== 'AWS') return false;
        return !searchFilter[key as keyof typeof searchFilter]
    })
    const invalidQueryForVendor = vendor === "AWS" && !searchFilter.az

    if (invalidQuery || invalidQueryForVendor) {
      alert("The query is invalid. Please check your search option.")
      return
    }

    if (searchFilter.start_date <= searchFilter.end_date) {
      setLoading(true)

      try {
        if (vendor === "AWS") {
          // TITANS Polars Lambda (AWS only)
          const body = {
            instance_types: searchFilter.instance === "ALL" ? ["all"] : [searchFilter.instance],
            regions: searchFilter.region === "ALL" ? ["all"] : [searchFilter.region],
            azs: searchFilter.az === "ALL" ? ["all"] : [searchFilter.az],
            start: searchFilter.start_date,
            end: searchFilter.end_date,
            strategy: "unified",
          }
          const resp = await fetch(`${TITANS_ENDPOINT}/query`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          })
          if (!resp.ok) {
            const errText = await resp.text()
            alert("TITANS query error: " + errText.slice(0, 200))
            return
          }
          // Decompress gzip response
          const blob = await resp.blob()
          const ds = new DecompressionStream("gzip")
          const decompressed = blob.stream().pipeThrough(ds)
          const text = await new Response(decompressed).text()
          const data = JSON.parse(text)
          console.log("TITANS response:", data.result_count, "rows, timing:", data.timing)
          onDataFetch(data.results || [], {
            start: searchFilter.start_date,
            end: searchFilter.end_date,
            region: searchFilter.region,
          })
        } else {
          // GCP/Azure: existing CloudFront TSDB API
          const params = {
            TableName: vendor.toLowerCase(),
            Region: searchFilter.region === "ALL" ? "*" : searchFilter.region,
            InstanceType: searchFilter.instance === "ALL" ? "*" : searchFilter.instance,
            ...(vendor === "AZURE" && {
              InstanceTier: "*",
              AvailabilityZone: searchFilter.az === "ALL" ? "*" : searchFilter.az,
            }),
            Start: searchFilter.start_date === "" ? "*" : searchFilter.start_date,
            End: searchFilter.end_date === "" ? "*" : searchFilter.end_date,
          }
          const res = await axios.get(url, { params })
          if (res.data.Data || res.data.Status === 200) {
            onDataFetch(res.data.Data || [], {
              start: searchFilter.start_date,
              end: searchFilter.end_date,
              region: searchFilter.region,
            })
          } else {
            alert("Error fetching data: " + res.data.Status)
          }
        }
      } catch (e) {
        console.error(e)
        alert("Network error")
      } finally {
        setLoading(false)
      }
    } else {
        alert("Invalid date range")
    }
  }

  return (
    <Card className="mb-6">
      <CardContent className="flex flex-wrap gap-4 p-1 items-end justify-center">
        <div className="flex flex-col space-y-1.5">
          <Label htmlFor="instance">Instance</Label>
          <Select
            value={searchFilter.instance}
            onValueChange={(val) => handleFilterChange("instance", val)}
          >
            <SelectTrigger id="instance" className="w-[180px]">
              <SelectValue placeholder="Select Instance" />
            </SelectTrigger>
            <SelectContent>
              {(assoInstance || instance).map((e) => (
                <SelectItem key={e} value={e}>
                  {e}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="flex flex-col space-y-1.5">
          <Label htmlFor="region">Region</Label>
          <Select
            value={searchFilter.region}
            onValueChange={(val) => handleFilterChange("region", val)}
            disabled={vendor === "AWS" && !searchFilter.instance}
          >
            <SelectTrigger id="region" className="w-[180px]">
              <SelectValue placeholder="Select Region" />
            </SelectTrigger>
            <SelectContent>
              {(assoRegion || region).map((e) => (
                <SelectItem key={e} value={e}>
                  {e}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {(vendor === "AWS" || vendor === "AZURE") && (
          <div className="flex flex-col space-y-1.5">
            <Label htmlFor="az">AZ</Label>
            <Select
              value={searchFilter.az}
              onValueChange={(val) => handleFilterChange("az", val)}
              disabled={vendor === "AWS" && !searchFilter.region}
            >
              <SelectTrigger id="az" className="w-[180px]">
                <SelectValue placeholder="Select AZ" />
              </SelectTrigger>
              <SelectContent>
                {vendor === "AZURE"
                  ? ["ALL", "1", "2", "3", "Single"].map((e) => (
                      <SelectItem key={e} value={e}>
                        {e}
                      </SelectItem>
                    ))
                  : (assoAZ || az).map((e) => (
                      <SelectItem key={e} value={e}>
                        {e}
                      </SelectItem>
                    ))}
              </SelectContent>
            </Select>
          </div>
        )}

        <div className="flex flex-col space-y-1.5">
          <Label htmlFor="start_date">Start Date</Label>
          <DatePicker
            date={startDate}
            onDateChange={(date) => {
              setStartDate(date)
              if (date) {
                const dateStr = date.toISOString().split("T")[0]
                handleFilterChange("start_date", dateStr)
              }
            }}
            placeholder="Select start date"
            maxDate={new Date()}
          />
        </div>

        <div className="flex flex-col space-y-1.5">
          <Label htmlFor="end_date">End Date</Label>
          <DatePicker
            date={endDate}
            onDateChange={(date) => {
              setEndDate(date)
              if (date) {
                const dateStr = date.toISOString().split("T")[0]
                handleFilterChange("end_date", dateStr)
              }
            }}
            placeholder="Select end date"
            maxDate={dateRange.max ? new Date(dateRange.max) : new Date()}
          />
        </div>

        <Button onClick={querySubmit}>Query</Button>
      </CardContent>
    </Card>
  )
}
