import { useMemo, useState, useEffect, useRef, useCallback } from "react"
import { AgGridReact } from "ag-grid-react"
import { ColDef, ModuleRegistry, themeQuartz, colorSchemeDark } from "ag-grid-community"
import { 
  ClientSideRowModelModule, 
  ValidationModule,
  PaginationModule,
  TextFilterModule,
  NumberFilterModule,
  DateFilterModule,
  CustomFilterModule,
  CsvExportModule
} from "ag-grid-community";
import { useTheme } from "next-themes"
import { Button } from "@/components/ui/button"
import { Download } from "lucide-react"

ModuleRegistry.registerModules([
  ClientSideRowModelModule, 
  ValidationModule,
  PaginationModule,
  TextFilterModule,
  NumberFilterModule,
  DateFilterModule,
  CustomFilterModule,
  CsvExportModule
]);

interface AgGridTableProps<TData> {
  rowData: TData[]
  columnDefs: ColDef<TData>[]
}

export function AgGridTable<TData>({ rowData, columnDefs }: AgGridTableProps<TData>) {
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  const gridRef = useRef<AgGridReact>(null)

  useEffect(() => {
    setMounted(true)
  }, [])

  useEffect(() => {
    console.log("AgGridTable received rowData:", rowData)
  }, [rowData])

  const defaultColDef = useMemo<ColDef>(() => {
    return {
      flex: 1,
      minWidth: 100,
      filter: true,
      sortable: true,
      resizable: true,
    }
  }, [])

  const gridTheme = useMemo(() => {
    return resolvedTheme === 'dark' ? themeQuartz.withPart(colorSchemeDark) : themeQuartz
  }, [resolvedTheme])

  const onExportClick = useCallback(() => {
    const params = {
      fileName: 'spotlake-data',
      columnSeparator: ',',
      suppressQuotes: false,
      skipColumnGroupHeaders: false,
      skipColumnHeaders: false,
      allColumns: false,
      onlySelected: false,
      skipPinnedTop: false,
      skipPinnedBottom: false
    }
    gridRef.current!.api.exportDataAsCsv(params)
  }, [])

  if (!mounted) return null

  return (
    <div className="space-y-4">
      <div className="flex justify-end">
        <Button
          variant="outline"
          size="sm"
          className="ml-auto h-8 lg:flex"
          onClick={onExportClick}
        >
          <Download className="mr-2 h-4 w-4" />
          Export CSV
        </Button>
      </div>
      <div
        style={{ height: 600, width: "100%" }}
      >
        <AgGridReact
          ref={gridRef}
          rowData={rowData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          pagination={true}
          paginationPageSize={20}
          paginationPageSizeSelector={[10, 20, 50, 100]}
          theme={gridTheme}
        />
      </div>
    </div>
  )
}
