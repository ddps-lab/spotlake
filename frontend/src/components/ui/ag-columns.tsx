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

const isNA = (v: any) => v === -1 || v === "-1" || v === undefined || v === null || v === "N/A"

/** Sort comparator that always pushes N/A (-1, null, undefined) to the bottom */
const naComparator = (a: any, b: any, _nodeA: any, _nodeB: any, isDescending: boolean) => {
  const aNA = isNA(a)
  const bNA = isNA(b)
  if (aNA && bNA) return 0
  if (aNA) return isDescending ? -1 : 1
  if (bNA) return isDescending ? 1 : -1
  return a - b
}

const formatNumber = (params: any) => {
  const num = params.value
  if (isNA(num)) return "N/A"
  return num
}

const formatSavings = (params: any) => {
  const num = params.value
  if (isNA(num)) return "N/A"
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
  { field: "InstanceType", headerName: "Type", headerTooltip: "Instance Type", minWidth: 140, valueFormatter: formatNumber },
  { field: "Region", headerTooltip: "Region", minWidth: 130, valueFormatter: formatNumber },
  { field: "AZ", headerName: "AZ", headerTooltip: "Availability Zone", minWidth: 80, valueFormatter: formatNumber },
  { field: "SPS", headerName: "Availability", headerTooltip: "Spot Placement Score", minWidth: 110, comparator: naComparator, valueFormatter: formatNumber },
  { field: "T2", headerTooltip: "T2", minWidth: 60, comparator: naComparator, valueFormatter: formatNumber },
  { field: "T3", headerTooltip: "T3", minWidth: 60, comparator: naComparator, valueFormatter: formatNumber },
  { field: "IF", headerName: "Interruption", headerTooltip: "Interruption Ratio", minWidth: 120, comparator: naComparator, valueFormatter: formatNumber },
  { field: "SpotPrice", headerName: "SpotPrice ($)", headerTooltip: "Spot Price (USD)", minWidth: 110, comparator: naComparator, valueFormatter: formatNumber },
  {
    headerName: "Savings (%)",
    headerTooltip: "Savings Percentage",
    minWidth: 100,
    comparator: naComparator,
    valueGetter: calculateSavings
  },
  { field: "Time", headerName: "Date", headerTooltip: "Timestamp", minWidth: 170, valueFormatter: formatDate },
]

export const gcpColDefs: ColDef<GCPData>[] = [
  { field: "InstanceType", headerTooltip: "Instance Type", minWidth: 140, valueFormatter: formatNumber },
  { field: "Region", headerTooltip: "Region", minWidth: 130, valueFormatter: formatNumber },
  { field: "OnDemand Price", headerTooltip: "On-Demand Price", minWidth: 130, comparator: naComparator, valueFormatter: formatNumber },
  { field: "Spot Price", headerTooltip: "Spot Price", minWidth: 110, comparator: naComparator, valueFormatter: formatNumber },
  {
    headerName: "Savings (%)",
    headerTooltip: "Savings Percentage",
    minWidth: 100,
    comparator: naComparator,
    valueGetter: calculateSavings
  },
  { field: "time", headerName: "Date", headerTooltip: "Timestamp", minWidth: 170, valueFormatter: formatDate },
]

export const azureColDefs: ColDef<AzureData>[] = [
  { field: "InstanceTier", headerName: "Tier", headerTooltip: "Instance Tier", minWidth: 70, valueFormatter: formatNumber },
  { field: "InstanceType", headerName: "Type", headerTooltip: "Instance Type", minWidth: 140, valueFormatter: formatNumber },
  { field: "Region", headerTooltip: "Region", minWidth: 130, valueFormatter: formatNumber },
  { field: "AvailabilityZone", headerName: "AZ", headerTooltip: "Availability Zone", minWidth: 60, valueFormatter: formatNumber },
  { field: "SpotPrice", headerTooltip: "Spot Price", minWidth: 100, comparator: naComparator, valueFormatter: formatNumber },
  { field: "Savings", headerName: "Savings (%)", headerTooltip: "Savings Percentage", minWidth: 100, comparator: naComparator, valueFormatter: formatSavings },
  { field: "IF", headerName: "IF", headerTooltip: "Interruption Frequency", minWidth: 60, comparator: naComparator, valueFormatter: formatNumber },
  { field: "Score", headerName: "Availability", headerTooltip: "Availability Score", minWidth: 110, comparator: naComparator, valueFormatter: formatNumber },
  { field: "T2", headerTooltip: "T2", minWidth: 60, comparator: naComparator, valueFormatter: formatNumber },
  { field: "T3", headerTooltip: "T3", minWidth: 60, comparator: naComparator, valueFormatter: formatNumber },
  { field: "Time", headerName: "Date", headerTooltip: "Timestamp", minWidth: 170, valueFormatter: formatDate },
]
