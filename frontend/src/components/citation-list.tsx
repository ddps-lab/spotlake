"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { ExternalLink } from "lucide-react"
import { Button } from "@/components/ui/button"
import { PublicationList } from "@/components/publication-list"
import { groupByYear } from "@/lib/publication-data"
import { CITATIONS_URL, SCHOLAR_CITATIONS, parseCitationSnapshot, type CitationSnapshot } from "@/lib/citations"
import { formatUtc } from "@/lib/latest-data"
type CitationState =
  | { status: "loading" }
  | { status: "loaded"; snapshot: CitationSnapshot }
  | { status: "error" }

export function CitationList() {
  const [state, setState] = useState<CitationState>({ status: "loading" })
  const snapshot = state.status === "loaded" ? state.snapshot : null

  useEffect(() => {
    const controller = new AbortController()
    let active = true
    const timeout = setTimeout(() => controller.abort(), 10_000)
    fetch(CITATIONS_URL, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`Citations: ${response.status}`)
        return response.json()
      })
      .then(parseCitationSnapshot)
      .then((next) => {
        if (active && !controller.signal.aborted) {
          setState({ status: "loaded", snapshot: next })
        }
      })
      .catch(() => {
        if (active) setState({ status: "error" })
      })
      .finally(() => clearTimeout(timeout))
    return () => {
      active = false
      clearTimeout(timeout)
      controller.abort()
    }
  }, [])

  return (
    <div className="space-y-3 pt-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-lg font-semibold">Papers citing SpotLake</h3>
        <div className="flex flex-wrap items-center gap-3">
          <p className="text-xs text-muted-foreground">
            Last Updated:{" "}
            {snapshot?.updatedAt ? (
              <time dateTime={snapshot.updatedAt} className="font-medium tabular-nums text-foreground">
                {formatUtc(snapshot.updatedAt)}
              </time>
            ) : (
              <span>{state.status === "loading" ? "Loading…" : "Not available"}</span>
            )}
          </p>
          <Button asChild variant="outline" size="sm">
            <Link href={SCHOLAR_CITATIONS} target="_blank" rel="noreferrer">
              <ExternalLink className="mr-2 h-4 w-4" />
              All citations on Google Scholar
            </Link>
          </Button>
        </div>
      </div>
      <p className="text-muted-foreground text-sm">
        This list is refreshed monthly from open citation databases and is not
        exhaustive. Google Scholar tracks more citations.
      </p>
      {state.status === "loading" && (
        <p role="status" className="text-sm text-muted-foreground">Loading citations…</p>
      )}
      {state.status === "error" && (
        <p role="alert" className="text-sm text-muted-foreground">
          Unable to load citations. Please reload this page or view all citations on Google Scholar.
        </p>
      )}
      {snapshot && <PublicationList groups={groupByYear(snapshot.papers)} />}
    </div>
  )
}
