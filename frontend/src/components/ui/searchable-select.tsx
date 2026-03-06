"use client"

import { useId, useState } from "react"
import { Check, ChevronsUpDown } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { cn } from "@/lib/utils"

interface SearchableSelectProps {
  value: string
  onValueChange: (value: string) => void
  options: string[]
  placeholder: string
  searchPlaceholder?: string
  emptyMessage?: string
  disabled?: boolean
  id?: string
  className?: string
}

export function SearchableSelect({
  value,
  onValueChange,
  options,
  placeholder,
  searchPlaceholder = "Search...",
  emptyMessage = "No results found.",
  disabled = false,
  id,
  className,
}: SearchableSelectProps) {
  const generatedId = useId()
  const inputId = id ?? generatedId
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState("")

  const normalizedQuery = query.trim().toLowerCase()
  const filteredOptions = normalizedQuery
    ? options.filter((option) => option.toLowerCase().includes(normalizedQuery))
    : options

  return (
    <Popover
      open={open}
      onOpenChange={(nextOpen) => {
        setOpen(nextOpen)
        if (!nextOpen) {
          setQuery("")
        }
      }}
    >
      <PopoverTrigger asChild>
        <Button
          id={inputId}
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          aria-controls={`${inputId}-listbox`}
          disabled={disabled}
          className={cn("w-[220px] justify-between font-normal", className)}
        >
          <span className={cn("truncate", !value && "text-muted-foreground")}>
            {value || placeholder}
          </span>
          <ChevronsUpDown className="size-4 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[220px] p-0" align="start">
        <div className="border-b p-2">
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={searchPlaceholder}
            autoFocus
          />
        </div>
        <div
          id={`${inputId}-listbox`}
          role="listbox"
          className="max-h-64 overflow-y-auto p-1"
        >
          {filteredOptions.length > 0 ? (
            filteredOptions.map((option) => {
              const selected = option === value

              return (
                <button
                  key={option}
                  type="button"
                  role="option"
                  aria-selected={selected}
                  className={cn(
                    "flex w-full items-center justify-between rounded-sm px-2 py-1.5 text-left text-sm outline-hidden transition-colors hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground",
                    selected && "bg-accent/60"
                  )}
                  onClick={() => {
                    onValueChange(option)
                    setOpen(false)
                  }}
                >
                  <span className="truncate">{option}</span>
                  <Check className={cn("size-4", selected ? "opacity-100" : "opacity-0")} />
                </button>
              )
            })
          ) : (
            <div className="px-2 py-4 text-center text-sm text-muted-foreground">
              {emptyMessage}
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}
