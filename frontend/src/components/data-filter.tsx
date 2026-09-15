"use client"

import { useMemo } from "react"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { MultiSelect } from "@/components/ui/multi-select"
import { FIELDS, type Vendor } from "@/lib/latest-data"

export interface Filters {
  instance: string[]
  region: string[]
  az: string[]
}

export const emptyFilters: Filters = { instance: [], region: [], az: [] }

export const isFiltered = (f: Filters) =>
  f.instance.length > 0 || f.region.length > 0 || f.az.length > 0

/**
 * 선택한 조건에 맞는 행만 남긴다.
 *
 * 항목을 비워 두면 그 조건은 걸지 않는다. 예전 쿼리 기능의 ALL 항목이
 * 하던 역할을 "아무것도 고르지 않음"이 대신한다.
 */
export function applyFilters<T extends Record<string, unknown>>(
  rows: T[],
  vendor: Vendor,
  filters: Filters,
): T[] {
  const f = FIELDS[vendor]
  const instance = new Set(filters.instance)
  const region = new Set(filters.region)
  const az = new Set(filters.az)
  if (!instance.size && !region.size && !(f.az && az.size)) return rows

  return rows.filter((row) => {
    if (instance.size && !instance.has(String(row[f.instance]))) return false
    if (region.size && !region.has(String(row[f.region]))) return false
    if (f.az && az.size && !az.has(String(row[f.az]))) return false
    return true
  })
}

/** 실제 데이터에 있는 값만 고를 수 있게 한다. */
function optionsOf<T extends Record<string, unknown>>(rows: T[], key?: string): string[] {
  if (!key) return []
  const seen = new Set<string>()
  for (const row of rows) {
    const v = row[key]
    if (v !== null && v !== undefined && v !== "") seen.add(String(v))
  }
  return [...seen].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
}

interface DataFilterProps<T extends Record<string, unknown>> {
  vendor: Vendor
  rows: T[]
  filters: Filters
  onChange: (next: Filters) => void
  disabled?: boolean
}

export function DataFilter<T extends Record<string, unknown>>({
  vendor,
  rows,
  filters,
  onChange,
  disabled = false,
}: DataFilterProps<T>) {
  const fields = FIELDS[vendor]

  // 리전을 고르면 인스턴스와 AZ 목록도 그 리전 것만 남도록 서로 좁혀 준다.
  const scoped = useMemo(() => {
    if (filters.region.length === 0) return rows
    const region = new Set(filters.region)
    return rows.filter((r) => region.has(String(r[fields.region])))
  }, [rows, fields, filters.region])

  const instanceOptions = useMemo(
    () => optionsOf(scoped, fields.instance), [scoped, fields],
  )
  const regionOptions = useMemo(() => optionsOf(rows, fields.region), [rows, fields])
  const azOptions = useMemo(() => optionsOf(scoped, fields.az), [scoped, fields])

  return (
    <div className="flex flex-wrap items-end gap-4">
      <div className="min-w-56 flex-1 space-y-2">
        <Label htmlFor="filter-instance">Instance Type</Label>
        <MultiSelect
          id="filter-instance"
          value={filters.instance}
          onValueChange={(instance) => onChange({ ...filters, instance })}
          options={instanceOptions}
          placeholder="All instance types"
          disabled={disabled}
        />
      </div>

      <div className="min-w-56 flex-1 space-y-2">
        <Label htmlFor="filter-region">Region</Label>
        <MultiSelect
          id="filter-region"
          value={filters.region}
          onValueChange={(region) =>
            // 리전이 바뀌면 그 안에 없는 인스턴스나 AZ 가 남아 결과가 0건이
            // 되어 버린다. 하위 항목을 함께 되돌린다.
            onChange({ ...emptyFilters, region })
          }
          options={regionOptions}
          placeholder="All regions"
          disabled={disabled}
        />
      </div>

      {fields.az && (
        <div className="min-w-44 flex-1 space-y-2">
          <Label htmlFor="filter-az">Availability Zone</Label>
          <MultiSelect
            id="filter-az"
            value={filters.az}
            onValueChange={(az) => onChange({ ...filters, az })}
            options={azOptions}
            placeholder="All zones"
            disabled={disabled}
          />
        </div>
      )}

      <Button
        variant="outline"
        onClick={() => onChange(emptyFilters)}
        disabled={disabled || !isFiltered(filters)}
      >
        Reset
      </Button>
    </div>
  )
}
