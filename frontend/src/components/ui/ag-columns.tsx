import { ColDef } from "ag-grid-community"

export type AWSData = {
  InstanceType: string
  Region: string
  AZ: string
  SPS: number
  T2: number
  T3: number
  IF: number
  SpotPrice: number
  OndemandPrice: number
  Time: string
}

export type GCPData = {
  id: string
  InstanceType: string
  Region: string
  "OnDemand Price": number
  "Spot Price": number
  time: string
}

export type AzureData = {
  id: number
  InstanceTier: string
  InstanceType: string
  Region: string
  AvailabilityZone: string
  OndemandPrice: number
  SpotPrice: number
  Savings: number
  IF: number
  Score: number
  DesiredCount: number
  T2: number
  T3: number
  Time: string
}

const formatNumber = (params: any) => {
  const num = params.value
  if (num === -1 || num === "-1" || num === undefined || num === null) return "N/A"
  return num
}

const formatSavings = (params: any) => {
  const num = params.value
  if (num === -1 || num === "-1" || num === undefined || num === null) return "N/A"
  return String(Math.round(num))
}

const calculateSavings = (params: any) => {
  const data = params.data
  let ondemand, spot
  
  if ('OndemandPrice' in data) {
      ondemand = data.OndemandPrice
      spot = data.SpotPrice
  } else {
      ondemand = data["OnDemand Price"]
      spot = data["Spot Price"]
  }

  if (!ondemand || !spot || ondemand === -1 || spot === -1) return "N/A"
  const savings = Math.round(((ondemand - spot) / ondemand) * 100)
  return isNaN(savings) ? "N/A" : savings
}

const formatDate = (params: any) => {
  const dateStr = params.value
  if (!dateStr) return "N/A"
  try {
    const date = new Date(dateStr)
    if (isNaN(date.getTime())) {
        return dateStr
    }
    
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    const hours = String(date.getHours()).padStart(2, '0')
    const minutes = String(date.getMinutes()).padStart(2, '0')
    const seconds = String(date.getSeconds()).padStart(2, '0')
    
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
  } catch (e) {
    return dateStr
  }
}

export const awsColDefs: ColDef<AWSData>[] = [
  { field: "InstanceType", headerName: "Type", valueFormatter: formatNumber },
  { field: "Region", valueFormatter: formatNumber },
  { field: "AZ", headerName: "AZ", valueFormatter: formatNumber },
  { field: "SPS", headerName: "Availability", valueFormatter: formatNumber },
  { field: "T2", valueFormatter: formatNumber, width: 70, maxWidth: 100 },
  { field: "T3", valueFormatter: formatNumber, width: 70, maxWidth: 100 },
  { field: "IF", headerName: "Interruption Ratio", valueFormatter: formatNumber },
  { field: "SpotPrice", headerName: "SpotPrice ($)", valueFormatter: formatNumber },
  { 
    headerName: "Savings (%)", 
    valueGetter: calculateSavings 
  },
  { field: "Time", headerName: "Date", valueFormatter: formatDate },
]

export const gcpColDefs: ColDef<GCPData>[] = [
  { field: "InstanceType", valueFormatter: formatNumber },
  { field: "Region", valueFormatter: formatNumber },
  { field: "OnDemand Price", valueFormatter: formatNumber },
  { field: "Spot Price", valueFormatter: formatNumber },
  { 
    headerName: "Savings (%)", 
    valueGetter: calculateSavings 
  },
  { field: "time", headerName: "Date", valueFormatter: formatDate },
]

export const azureColDefs: ColDef<AzureData>[] = [
  { field: "InstanceTier", headerName: "Tier", valueFormatter: formatNumber, width: 60, maxWidth: 100 },
  { field: "InstanceType", headerName: "Type", valueFormatter: formatNumber },
  { field: "Region", valueFormatter: formatNumber },
  { field: "AvailabilityZone", headerName: "AZ", valueFormatter: formatNumber, width: 40, maxWidth: 80 },
  { field: "SpotPrice", valueFormatter: formatNumber },
  { field: "Savings", headerName: "Savings (%)", valueFormatter: formatSavings },
  { field: "IF", headerName: "IF", valueFormatter: formatNumber, width: 60, maxWidth: 100 },
  { field: "Score", headerName: "Availability", valueFormatter: formatNumber },
  { field: "T2", valueFormatter: formatNumber, width: 70, maxWidth: 100 },
  { field: "T3", valueFormatter: formatNumber, width: 70, maxWidth: 100 },
  { field: "Time", headerName: "Date", valueFormatter: formatDate },
]
