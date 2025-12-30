"use client"

import { useMemo } from "react"
import { Area, AreaChart, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from "recharts"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { useTheme } from "next-themes"

interface QueryChartProps {
  data: any[]
  dateRange?: { start: string, end: string }
}

export function QueryChart({ data, dateRange }: QueryChartProps) {
  const { resolvedTheme } = useTheme()

  const { processedData, groups, numericKeys } = useMemo(() => {
    if (!data || data.length === 0) return { processedData: [], groups: [], numericKeys: [] }

    const firstItem = data[0]
    // Identify numeric keys to chart
    const numKeys = Object.keys(firstItem).filter(key => {
      const value = firstItem[key]
      return typeof value === 'number' && !['id'].includes(key)
    })

    // Identify unique groups based on InstanceType, Region, AZ
    const uniqueGroups = new Set<string>()
    const dataByTime = new Map<string, any>()

    data.forEach(item => {
      const time = item.Time || item.time
      if (!time) return

      // Construct group key
      let groupKey = ""
      if (item.AZ) { // AWS
        groupKey = `${item.InstanceType}-${item.Region}-${item.AZ}`
      } else if (item.AvailabilityZone) { // Azure
        groupKey = `${item.InstanceType}-${item.Region}-${item.AvailabilityZone}`
      } else { // GCP or others without explicit AZ
        groupKey = `${item.InstanceType}-${item.Region}`
      }
      uniqueGroups.add(groupKey)

      if (!dataByTime.has(time)) {
        dataByTime.set(time, { Time: time })
      }
      const timeEntry = dataByTime.get(time)
      
      // Add values for each numeric key with group suffix
      numKeys.forEach(key => {
        timeEntry[`${key}__${groupKey}`] = item[key]
      })
    })

    // Convert map to array and sort by time
    const sortedData = Array.from(dataByTime.values())
      .map(d => ({ ...d, Time: new Date(d.Time).getTime() }))
      .sort((a, b) => a.Time - b.Time)

    // If date range is provided, extend data to cover the full range
    if (dateRange && dateRange.start && dateRange.end) {
      const startDate = new Date(dateRange.start)
      startDate.setHours(0, 0, 0, 0)
      const endDate = new Date(dateRange.end)
      endDate.setHours(23, 59, 59, 999)

      const startTimestamp = startDate.getTime()
      const endTimestamp = endDate.getTime()

      // 1. Create Start Point (Time = startTimestamp)
      // It should carry the latest values from data points <= startTimestamp
      const startPoint: any = { Time: startTimestamp }
      let hasStartData = false

      numKeys.forEach(key => {
        uniqueGroups.forEach(group => {
          const dataKey = `${key}__${group}`
          
          // Try to find last value <= startTimestamp
          let found = false
          for (let i = sortedData.length - 1; i >= 0; i--) {
            const t = sortedData[i].Time
            if (t <= startTimestamp) {
              if (sortedData[i][dataKey] !== undefined && sortedData[i][dataKey] !== null) {
                startPoint[dataKey] = sortedData[i][dataKey]
                hasStartData = true
                found = true
                break
              }
            }
          }
          
          // If not found, BACKFILL with the first available value > startTimestamp
          if (!found) {
             for (let i = 0; i < sortedData.length; i++) {
                const t = sortedData[i].Time
                if (t > startTimestamp) {
                   if (sortedData[i][dataKey] !== undefined && sortedData[i][dataKey] !== null) {
                      startPoint[dataKey] = sortedData[i][dataKey]
                      hasStartData = true
                      break
                   }
                }
             }
          }
        })
      })

      // 2. Create End Point (Time = endTimestamp)
      // It should carry the latest values from data points <= endTimestamp
      const endPoint: any = { Time: endTimestamp }
      let hasEndData = false

      numKeys.forEach(key => {
        uniqueGroups.forEach(group => {
          const dataKey = `${key}__${group}`
          // Search backwards for the last non-null value <= endTimestamp
          for (let i = sortedData.length - 1; i >= 0; i--) {
            const t = sortedData[i].Time
            if (t <= endTimestamp) {
              if (sortedData[i][dataKey] !== undefined && sortedData[i][dataKey] !== null) {
                endPoint[dataKey] = sortedData[i][dataKey]
                hasEndData = true
                break
              }
            }
          }
        })
      })

      // 3. Filter Middle Points (startTimestamp < Time <= endTimestamp)
      const middlePoints = sortedData.filter(d => {
        const t = d.Time
        return t > startTimestamp && t <= endTimestamp
      })


      const finalData = []
      if (hasStartData || middlePoints.length > 0 || hasEndData) {
         finalData.push(startPoint)
         finalData.push(...middlePoints)
         finalData.push(endPoint)
      }
      
      return { 
        processedData: finalData, 
        groups: Array.from(uniqueGroups), 
        numericKeys: numKeys 
      }
    }

    return { 
      processedData: sortedData, 
      groups: Array.from(uniqueGroups), 
      numericKeys: numKeys 
    }
  }, [data, dateRange])

  // Generate colors for groups
  const groupColors = useMemo(() => {
    const colors: Record<string, string> = {}
    groups.forEach((group, index) => {
      // Use HSL to generate distinct colors
      const hue = (index * 137.508) % 360 // Golden angle approximation
      colors[group] = `hsl(${hue}, 70%, 50%)`
    })
    return colors
  }, [groups])

  const domain = useMemo(() => {
    if (!dateRange || !dateRange.start || !dateRange.end) return ['auto', 'auto']
    const start = new Date(dateRange.start)
    start.setHours(0, 0, 0, 0)
    const end = new Date(dateRange.end)
    end.setHours(23, 59, 59, 999)
    return [start.getTime(), end.getTime()]
  }, [dateRange])

  if (!data || data.length === 0) return null

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      {numericKeys.map((key) => (
        <Card key={key} className="w-full">
          <CardHeader>
            <CardTitle className="text-sm font-medium">{key}</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[300px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={processedData}
                  margin={{
                    top: 10,
                    right: 10,
                    left: 0,
                    bottom: 0,
                  }}
                >
                  <defs>
                    {groups.map(group => (
                      <linearGradient key={group} id={`color${key}${group}`} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={groupColors[group]} stopOpacity={0.3} />
                        <stop offset="95%" stopColor={groupColors[group]} stopOpacity={0} />
                      </linearGradient>
                    ))}
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" vertical={false} />
                  <XAxis 
                    dataKey="Time" 
                    type="number"
                    domain={domain}
                    scale="time"
                    tickFormatter={(value) => {
                      if (!value) return ""
                      try {
                        const date = new Date(value)
                        return `${date.getMonth() + 1}/${date.getDate()} ${date.getHours()}:${date.getMinutes()}`
                      } catch {
                        return ""
                      }
                    }}
                    className="text-xs text-muted-foreground"
                    tickLine={false}
                    axisLine={false}
                  />
                  <YAxis 
                    className="text-xs text-muted-foreground"
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(value) => `${value}`}
                  />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: resolvedTheme === 'dark' ? 'hsl(var(--popover))' : 'white',
                      borderColor: 'hsl(var(--border))',
                      borderRadius: 'var(--radius)',
                      color: 'hsl(var(--popover-foreground))'
                    }}
                    labelStyle={{ color: 'hsl(var(--muted-foreground))' }}
                    labelFormatter={(label) => {
                      try {
                         return new Date(label).toLocaleString()
                      } catch {
                        return label
                      }
                    }}
                  />
                  <Legend />
                  {groups.map(group => (
                    <Area
                      key={group}
                      type="stepAfter"
                      dataKey={`${key}__${group}`}
                      name={group}
                      stroke={groupColors[group]}
                      fillOpacity={1}
                      fill={`url(#color${key}${group})`}
                      connectNulls
                    />
                  ))}
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
