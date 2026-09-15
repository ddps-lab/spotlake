"use client"

import { useId, useMemo, useState } from "react"
import { Check, ChevronsUpDown, X } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { cn } from "@/lib/utils"

interface MultiSelectProps {
  /** 고른 값들. 비어 있으면 전부 보여준다는 뜻이다. */
  value: string[]
  onValueChange: (value: string[]) => void
  options: string[]
  placeholder: string
  searchPlaceholder?: string
  emptyMessage?: string
  disabled?: boolean
  id?: string
  className?: string
}

/**
 * 검색해서 여러 개를 고르는 선택기.
 *
 * 예전 쿼리 기능은 서버에 한 번에 하나씩만 물어볼 수 있어 값을 하나만
 * 고르게 했고 그래서 ALL 항목이 따로 있었다. 지금은 브라우저에서 거르므로
 * 여러 개를 고를 수 있고, 아무것도 안 고른 상태가 곧 전체다.
 */
export function MultiSelect({
  value,
  onValueChange,
  options,
  placeholder,
  searchPlaceholder = "Search...",
  emptyMessage = "No results found.",
  disabled = false,
  id,
  className,
}: MultiSelectProps) {
  const generatedId = useId()
  const inputId = id ?? generatedId
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState("")

  const selected = useMemo(() => new Set(value), [value])
  const normalizedQuery = query.trim().toLowerCase()
  const filtered = normalizedQuery
    ? options.filter((o) => o.toLowerCase().includes(normalizedQuery))
    : options

  const toggle = (option: string) => {
    const next = new Set(selected)
    if (next.has(option)) next.delete(option)
    else next.add(option)
    onValueChange([...next])
  }

  // 고른 것이 많으면 버튼 안에 다 못 넣는다. 첫 항목과 나머지 개수만 쓴다.
  const label = value.length === 0
    ? placeholder
    : value.length === 1
      ? value[0]
      : `${value[0]} +${value.length - 1}`

  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        setOpen(next)
        if (!next) setQuery("")
      }}
    >
      <PopoverTrigger asChild>
        <Button
          id={inputId}
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={disabled}
          className={cn("w-full justify-between font-normal", className)}
        >
          <span className={cn("truncate", value.length === 0 && "text-muted-foreground")}>
            {label}
          </span>
          <ChevronsUpDown className="size-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>

      <PopoverContent className="w-[var(--radix-popover-trigger-width)] min-w-56 p-0" align="start">
        <div className="border-b p-2">
          <div className="relative">
            <Input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Escape" && query) {
                  e.preventDefault()
                  e.stopPropagation()
                  if ("nativeEvent" in e) e.nativeEvent.stopImmediatePropagation()
                  setQuery("")
                }
              }}
              placeholder={searchPlaceholder}
              autoFocus
              className="pr-8"
            />
            {query ? (
              <button
                type="button"
                onClick={() => setQuery("")}
                className="absolute top-1/2 right-2 -translate-y-1/2 rounded-sm p-0.5 text-muted-foreground transition-colors hover:text-foreground"
                aria-label="Clear search"
              >
                <X className="size-4" />
              </button>
            ) : null}
          </div>
          <div className="mt-2 flex items-center justify-between px-1 text-xs text-muted-foreground">
            <span>
              {query ? `${filtered.length} results` : `${options.length} options`}
              {value.length > 0 ? ` · ${value.length} selected` : ""}
            </span>
            {value.length > 0 ? (
              <button
                type="button"
                onClick={() => onValueChange([])}
                className="underline underline-offset-2 transition-colors hover:text-foreground"
              >
                Clear
              </button>
            ) : null}
          </div>
        </div>

        <div role="listbox" aria-multiselectable className="max-h-64 overflow-y-auto p-1">
          {filtered.length > 0 ? (
            filtered.map((option) => {
              const isOn = selected.has(option)
              return (
                <button
                  key={option}
                  type="button"
                  role="option"
                  aria-selected={isOn}
                  className={cn(
                    "flex w-full items-center justify-between rounded-sm px-2 py-1.5 text-left text-sm transition-colors hover:bg-accent hover:text-accent-foreground",
                    isOn && "bg-accent/60",
                  )}
                  // 여러 개를 고르는 중이므로 고를 때마다 닫지 않는다.
                  onClick={() => toggle(option)}
                >
                  <span className="truncate">{option}</span>
                  <Check className={cn("size-4 shrink-0", isOn ? "opacity-100" : "opacity-0")} />
                </button>
              )
            })
          ) : (
            <div className="px-2 py-4 text-center text-sm text-muted-foreground">
              <div>{emptyMessage}</div>
              <button
                type="button"
                onClick={() => setQuery("")}
                className="mt-2 inline-flex items-center rounded-sm text-sm text-foreground underline underline-offset-4 transition-colors hover:text-primary"
              >
                Clear search
              </button>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}
