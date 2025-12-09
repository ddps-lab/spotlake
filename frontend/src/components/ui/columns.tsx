"use client"

import { ColumnDef } from "@tanstack/react-table"
import { DataTableColumnHeader } from "./data-table-column-header"

export type AWSData = {
  id: string
  InstanceType: string
  Region: string
  AZ: string
  SPS: number
  T2: number
  T3: number
  IF: number
  SpotPrice: number
  OndemandPrice: number
  TimeStamp: never // Removed
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
  id: string
  InstanceTier: string
  InstanceType: string
  Region: string
  OndemandPrice: number
  SpotPrice: number
  IF: number
  Score: string
  AvailabilityZone: string
  SPS_Update_Time: string
}

const formatNumber = (num: number | string) => {
  if (num === -1 || num === "-1" || num === undefined || num === null) return "N/A"
  return num
}

const calculateSavings = (ondemand: number, spot: number) => {
  if (!ondemand || !spot || ondemand === -1 || spot === -1) return "N/A"
  const savings = Math.round(((ondemand - spot) / ondemand) * 100)
  return isNaN(savings) ? "N/A" : savings
}

const formatDate = (dateStr: string) => {
  if (!dateStr) return "N/A"
  // Check if it's already in YYYY-MM-DD HH:mm:ss format (AWS/Azure often are)
  // If it's an ISO string, convert it.
  try {
    const date = new Date(dateStr)
    if (isNaN(date.getTime())) {
        // If new Date fails, return original string if it looks like a date, or try to parse manually if needed.
        // For now, assuming the string is readable if not parseable by Date constructor directly in some browsers
        // But for "2025-11-20 14:40:00", new Date() works in modern browsers.
        return dateStr
    }
    
    // Format to YYYY-MM-DD HH:mm:ss
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

export const awsColumns: ColumnDef<AWSData>[] = [
  {
    accessorKey: "InstanceType",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="InstanceType" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("InstanceType")),
  },
  {
    accessorKey: "Region",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Region" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("Region")),
  },
  {
    accessorKey: "AZ",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="AZ" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("AZ")),
  },
  {
    accessorKey: "SPS",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Availability" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("SPS")),
  },
  {
    accessorKey: "T2",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="T2" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("T2")),
  },
  {
    accessorKey: "T3",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="T3" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("T3")),
  },
  {
    accessorKey: "IF",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Interruption Ratio" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("IF")),
  },
  {
    accessorKey: "SpotPrice",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="SpotPrice ($)" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("SpotPrice")),
  },
  {
    id: "Savings",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Savings (%)" />
    ),
    cell: ({ row }) => {
      return calculateSavings(row.original.OndemandPrice, row.original.SpotPrice)
    },
  },
  {
    accessorKey: "Time",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Date" />
    ),
    cell: ({ row }) => formatDate(row.getValue("Time")),
  },
]

export const gcpColumns: ColumnDef<GCPData>[] = [
  {
    accessorKey: "InstanceType",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="InstanceType" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("InstanceType")),
  },
  {
    accessorKey: "Region",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Region" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("Region")),
  },
  {
    accessorKey: "OnDemand Price",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="OnDemand Price" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("OnDemand Price")),
  },
  {
    accessorKey: "Spot Price",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Spot Price" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("Spot Price")),
  },
  {
    id: "Savings",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Savings (%)" />
    ),
    cell: ({ row }) => {
      return calculateSavings(row.original["OnDemand Price"], row.original["Spot Price"])
    },
  },
  {
    accessorKey: "time",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Date" />
    ),
    cell: ({ row }) => formatDate(row.getValue("time")),
  },
]

export const azureColumns: ColumnDef<AzureData>[] = [
  {
    accessorKey: "InstanceTier",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="InstanceTier" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("InstanceTier")),
  },
  {
    accessorKey: "InstanceType",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="InstanceType" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("InstanceType")),
  },
  {
    accessorKey: "Region",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Region" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("Region")),
  },
  {
    accessorKey: "OndemandPrice",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="OndemandPrice" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("OndemandPrice")),
  },
  {
    accessorKey: "SpotPrice",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="SpotPrice" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("SpotPrice")),
  },
  {
    accessorKey: "IF",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="IF" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("IF")),
  },
  {
    id: "Savings",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Savings (%)" />
    ),
    cell: ({ row }) => {
      return calculateSavings(row.original.OndemandPrice, row.original.SpotPrice)
    },
  },
  {
    accessorKey: "Score",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Availability" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("Score")),
  },
  {
    accessorKey: "AvailabilityZone",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="AZ" />
    ),
    cell: ({ row }) => formatNumber(row.getValue("AvailabilityZone")),
  },
  {
    accessorKey: "SPS_Update_Time",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Date" />
    ),
    cell: ({ row }) => formatDate(row.getValue("SPS_Update_Time")),
  },
]
