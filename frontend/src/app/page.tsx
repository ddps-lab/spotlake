"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Image from "next/image"
import { useTheme } from "next-themes"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { AgGridTable } from "@/components/ui/ag-grid-table"
import {
  awsColDefs,
  gcpColDefs,
  azureColDefs,
  type AWSData,
  type GCPData,
  type AzureData,
} from "@/components/ui/ag-columns"
import {
  DataFilter,
  applyFilters,
  emptyFilters,
  isFiltered,
  type Filters,
} from "@/components/data-filter"
import {
  VENDORS,
  VENDOR_INFO,
  fetchMeta,
  fetchVendorData,
  formatUtc,
  lastUpdated,
  vendorIcon,
  type LatestMeta,
  type Vendor,
} from "@/lib/latest-data"

type Row = AWSData | GCPData | AzureData

const COLUMNS = {
  AWS: awsColDefs,
  GCP: gcpColDefs,
  AZURE: azureColDefs,
} as const

export default function Home() {
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  const [vendor, setVendor] = useState<Vendor>("AWS")
  const [rows, setRows] = useState<Row[]>([])
  const [meta, setMeta] = useState<LatestMeta | null>(null)
  const [filters, setFilters] = useState<Filters>(emptyFilters)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // 아이콘이 테마를 타므로 서버와 클라이언트가 같은 것을 그리도록 기다린다.
  useEffect(() => {
    setMounted(true)
  }, [])

  // meta.json 은 벤더와 무관하게 한 번만 받는다.
  useEffect(() => {
    const ac = new AbortController()
    fetchMeta(ac.signal)
      .then(setMeta)
      .catch(() => {
        // 표는 그대로 보여줄 수 있으므로 갱신 시각만 포기한다.
      })
    return () => ac.abort()
  }, [])

  const load = useCallback((v: Vendor, ac?: AbortController) => {
    setLoading(true)
    setError(null)
    fetchVendorData<Row>(v, ac?.signal)
      .then((data) => {
        setRows(data)
        setLoading(false)
      })
      .catch((e: unknown) => {
        if (ac?.signal.aborted) return
        setRows([])
        setError(e instanceof Error ? e.message : "Failed to load data")
        setLoading(false)
      })
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    setFilters(emptyFilters) // 벤더가 바뀌면 이전 조건은 의미가 없다
    load(vendor, ac)
    return () => ac.abort()
  }, [vendor, load])

  const visible = useMemo(
    () => applyFilters(rows as Record<string, unknown>[], vendor, filters) as Row[],
    [rows, vendor, filters],
  )

  const isDark = mounted && resolvedTheme === "dark"
  const updatedAt = formatUtc(lastUpdated(meta, vendor))

  return (
    <div className="space-y-8">
      <section className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Latest Dataset</h1>
        <p className="text-muted-foreground leading-relaxed">
          The most recent publicly available snapshot of spot instance
          availability and pricing, updated once a day.
        </p>
      </section>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        {VENDORS.map((v) => (
          <Card
            key={v}
            role="button"
            tabIndex={0}
            aria-pressed={v === vendor}
            onClick={() => setVendor(v)}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault()
                setVendor(v)
              }
            }}
            className={`cursor-pointer transition-all hover:border-primary ${
              v === vendor ? "border-primary ring-2 ring-primary/20" : ""
            }`}
          >
            <CardContent className="flex flex-col items-center justify-center gap-2 py-2">
              {mounted && (
                <Image
                  src={vendorIcon(v, isDark)}
                  alt=""
                  width={36}
                  height={36}
                  className="object-contain"
                />
              )}
              <p className="text-md font-medium">{VENDOR_INFO[v].name}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardContent className="space-y-6">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <p className="text-sm text-muted-foreground">
              {loading
                ? "Loading…"
                : isFiltered(filters)
                  ? `${visible.length.toLocaleString()} of ${rows.length.toLocaleString()} rows`
                  : `${rows.length.toLocaleString()} rows`}
            </p>
            <p className="text-xs text-muted-foreground">
              Last updated{" "}
              <span className="font-medium tabular-nums text-foreground">
                {updatedAt}
              </span>
            </p>
          </div>

          <DataFilter
            vendor={vendor}
            rows={rows as Record<string, unknown>[]}
            filters={filters}
            onChange={setFilters}
            disabled={loading}
          />

          {error ? (
            <div className="space-y-4 py-8 text-center">
              <p className="text-muted-foreground">
                Could not load the dataset. {error}
              </p>
              <Button variant="outline" onClick={() => load(vendor)}>
                Retry
              </Button>
            </div>
          ) : loading ? (
            <div className="flex h-[600px] items-center justify-center">
              <div className="h-8 w-8 animate-spin rounded-full border-b-2 border-primary" />
            </div>
          ) : (
            <AgGridTable rowData={visible} columnDefs={COLUMNS[vendor] as never} />
          )}
        </CardContent>
      </Card>
    </div>
  )
}
