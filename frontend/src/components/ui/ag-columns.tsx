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

// Shared fields keep the same order across vendors; vendor-specific fields follow.
export const awsColDefs: ColDef<AWSData>[] = [
  { field: "InstanceType", minWidth: 190, flex: 2, headerName: "Type", headerTooltip: "Instance Type", valueFormatter: formatNumber },
  { field: "Region", minWidth: 155, flex: 1.5, headerTooltip: "Region", valueFormatter: formatNumber },
  { field: "SpotPrice", headerName: "SpotPrice ($)", minWidth: 130, headerTooltip: "Spot Price (USD)", comparator: naComparator, valueFormatter: formatNumber },
  {
    headerName: "Savings (%)",
    headerTooltip: "Savings Percentage",
    minWidth: 125,
    comparator: naComparator,
    valueGetter: calculateSavings
  },
  { field: "SPS", headerName: "Availability", minWidth: 125, headerTooltip: "In AWS, it is Spot Placement Score. For details, please refer to https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-placement-score.html", comparator: naComparator, valueFormatter: formatNumber },
  { field: "IF", headerName: "IF", headerTooltip: "In AWS, it is Interruption-free score. For details, please refer to “Frequency of interruption” in https://aws.amazon.com/ec2/spot/instance-advisor", comparator: naComparator, valueFormatter: formatNumber },
  { field: "T2", headerTooltip: "The maximum number of nodes whose Spot Placement Score (SPS) transitions from 2 to 1 denoted as T2", comparator: naComparator, valueFormatter: formatNumber },
  { field: "T3", headerTooltip: "The maximum number of nodes whose Spot Placement Score (SPS) transitions from 3 to 2 or 1 denoted as T3", comparator: naComparator, valueFormatter: formatNumber },
  { field: "AZ", headerName: "AZ", minWidth: 140, headerTooltip: "Availability Zone ID. For details, please refer to https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html", valueFormatter: formatNumber },
]

export const gcpColDefs: ColDef<GCPData>[] = [
  { field: "InstanceType", minWidth: 190, flex: 2, headerName: "Type", headerTooltip: "Instance Type", valueFormatter: formatNumber },
  { field: "Region", minWidth: 155, flex: 1.5, headerTooltip: "Region", valueFormatter: formatNumber },
  { field: "Spot Price", headerName: "SpotPrice ($)", minWidth: 130, headerTooltip: "Spot Price (USD)", comparator: naComparator, valueFormatter: formatNumber },
  {
    headerName: "Savings (%)",
    headerTooltip: "Savings Percentage",
    minWidth: 125,
    comparator: naComparator,
    valueGetter: calculateSavings
  },
  { field: "OnDemand Price", headerName: "OnDemand ($)", minWidth: 150, headerTooltip: "On-Demand Price (USD)", comparator: naComparator, valueFormatter: formatNumber },
]

export const azureColDefs: ColDef<AzureData>[] = [
  { field: "InstanceType", minWidth: 190, flex: 2, headerName: "Type", headerTooltip: "Instance Type", valueFormatter: formatNumber },
  { field: "Region", minWidth: 155, flex: 1.5, headerTooltip: "Region", valueFormatter: formatNumber },
  { field: "SpotPrice", headerName: "SpotPrice ($)", minWidth: 130, headerTooltip: "Spot Price (USD)", comparator: naComparator, valueFormatter: formatNumber },
  { field: "Savings", headerName: "Savings (%)", minWidth: 125, headerTooltip: "Savings Percentage", comparator: naComparator, valueFormatter: formatSavings },
  { field: "Score", headerName: "Availability", minWidth: 125, headerTooltip: "In Azure, it is Spot Placement Score. For details, please refer to https://learn.microsoft.com/en-us/azure/virtual-machine-scale-sets/spot-placement-score", comparator: naComparator, valueFormatter: formatNumber },
  { field: "IF", headerName: "IF", headerTooltip: "In Azure, it is Interruption-free score. For details, please refer to https://learn.microsoft.com/en-us/azure/virtual-machines/spot-vms#pricing-and-eviction-history", comparator: naComparator, valueFormatter: formatNumber },
  { field: "T2", headerTooltip: "The maximum number of nodes whose Spot Placement Score (SPS) transitions from 2 to 1 denoted as T2", minWidth: 80, comparator: naComparator, valueFormatter: formatNumber },
  { field: "T3", headerTooltip: "The maximum number of nodes whose Spot Placement Score (SPS) transitions from 3 to 2 or 1 denoted as T3", minWidth: 80, comparator: naComparator, valueFormatter: formatNumber },
  { field: "AvailabilityZone", headerName: "AZ", minWidth: 140, headerTooltip: "Availability Zone", valueFormatter: formatNumber },
  { field: "InstanceTier", headerName: "Tier", headerTooltip: "Instance Tier", valueFormatter: formatNumber },
]
