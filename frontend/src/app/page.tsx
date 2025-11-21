"use client"

import React, { useEffect, useState } from "react"
import axios from "axios"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { AgGridTable } from "@/components/ui/ag-grid-table"
import { awsColDefs, gcpColDefs, azureColDefs, AWSData, GCPData, AzureData } from "@/components/ui/ag-columns"
import { QuerySection } from "@/components/query-section"
import Image from "next/image"


export default function Home() {
  const [selectedVendor, setSelectedVendor] = useState("AWS")
  const [data, setData] = useState<any[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true)
      try {
        let url = ""
        if (selectedVendor === "AWS") {
          url = "https://d26bk4799jlxhe.cloudfront.net/latest_data/latest_aws.json"
        } else if (selectedVendor === "GCP") {
          url = "https://d26bk4799jlxhe.cloudfront.net/latest_data/latest_gcp.json"
        } else if (selectedVendor === "AZURE") {
          url = "https://d26bk4799jlxhe.cloudfront.net/latest_data/latest_azure.json"
        }

        const response = await fetch(url)
        const jsonData = await response.json()
        setData(jsonData)
      } catch (error) {
        console.error("Error fetching data:", error)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [selectedVendor])

  const getColumns = () => {
    switch (selectedVendor) {
      case "AWS":
        return awsColDefs
      case "GCP":
        return gcpColDefs
      case "AZURE":
        return azureColDefs
      default:
        return awsColDefs
    }
  }

  const handleDataFetch = (newData: any[]) => {
    console.log("Data fetched from query:", newData)
    // Normalize data: convert strings to numbers and ensure consistent keys
    const normalizedData = newData.map(item => ({
      ...item,
      // Ensure Time field exists (query api returns 'time', latest_data returns 'Time' and 'time')
      Time: item.Time || item.time,
      // Convert numeric strings to numbers
      SpotPrice: typeof item.SpotPrice === 'string' ? parseFloat(item.SpotPrice) : item.SpotPrice,
      OndemandPrice: typeof item.OndemandPrice === 'string' ? parseFloat(item.OndemandPrice) : item.OndemandPrice,
      SPS: typeof item.SPS === 'string' ? parseFloat(item.SPS) : item.SPS,
      IF: typeof item.IF === 'string' ? parseFloat(item.IF) : item.IF,
      T2: typeof item.T2 === 'string' ? parseFloat(item.T2) : item.T2,
      T3: typeof item.T3 === 'string' ? parseFloat(item.T3) : item.T3,
      "OnDemand Price": typeof item["OnDemand Price"] === 'string' ? parseFloat(item["OnDemand Price"]) : item["OnDemand Price"],
      "Spot Price": typeof item["Spot Price"] === 'string' ? parseFloat(item["Spot Price"]) : item["Spot Price"],
    }))
    console.log("Normalized data:", normalizedData)
    setData(normalizedData)
  }

  return (
    <div className="space-y-8">
      <section className="space-y-4 text-center">
        <h1 className="text-4xl font-bold tracking-tighter sm:text-5xl md:text-6xl bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-cyan-500">
          SpotLake
        </h1>
        <p className="mx-auto text-gray-500 md:text-xl/relaxed lg:text-base/relaxed xl:text-xl/relaxed dark:text-gray-400">
          Spot Instance pricing and availability analysis across major cloud providers.
        </p>
      </section>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card 
          className={`cursor-pointer transition-all hover:border-primary ${selectedVendor === "AWS" ? "border-primary ring-2 ring-primary/20" : ""}`}
          onClick={() => setSelectedVendor("AWS")}
        >
          <CardContent className="flex flex-col items-center justify-center py-1 gap-1">
            <Image src="/images/ic_aws.png" alt="AWS" width={36} height={36} className="object-contain" />
            <p className="text-md font-medium">Amazon Web Services</p>
          </CardContent>
        </Card>
        <Card 
          className={`cursor-pointer transition-all hover:border-primary ${selectedVendor === "GCP" ? "border-primary ring-2 ring-primary/20" : ""}`}
          onClick={() => setSelectedVendor("GCP")}
        >
          <CardContent className="flex flex-col items-center justify-center py-1 gap-1">
            <Image src="/images/ic_gcp.png" alt="GCP" width={36} height={36} className="object-contain" />
            <p className="text-md font-medium">Google Cloud Platform</p>
          </CardContent>
        </Card>
        <Card 
          className={`cursor-pointer transition-all hover:border-primary ${selectedVendor === "AZURE" ? "border-primary ring-2 ring-primary/20" : ""}`}
          onClick={() => setSelectedVendor("AZURE")}
        >
          <CardContent className="flex flex-col items-center justify-center py-1 gap-1">
            <Image src="/images/ic_azure.png" alt="Azure" width={36} height={36} className="object-contain" />
            <p className="text-md font-medium">Microsoft Azure</p>
          </CardContent>
        </Card>
      </div>

      <QuerySection 
        vendor={selectedVendor}
        onDataFetch={handleDataFetch}
        setLoading={setLoading}
      />

      <div className="space-y-4">
      
        {loading ? (
          <div className="flex items-center justify-center h-[600px]">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
          </div>
        ) : (
          <AgGridTable rowData={data} columnDefs={getColumns()} />
        )}
      </div>
    </div>
  )
}
