import { DATA_ORIGIN } from "./latest-data"
import type { Publication } from "./publication-data"

export const CITATIONS_URL = `${DATA_ORIGIN}/citations/citations.json`
export const SCHOLAR_CITATIONS =
  "https://scholar.google.com/scholar?cites=18351368357501524870"

export interface CitationSnapshot {
  schemaVersion: 1
  updatedAt: string | null
  source: string
  papers: Publication[]
}

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null

// Treat remote data as data, and retain the bundled list on a malformed response.
export function parseCitationSnapshot(value: unknown): CitationSnapshot {
  if (!record(value) || value.schemaVersion !== 1 ||
      typeof value.source !== "string" || !Array.isArray(value.papers) ||
      value.papers.length === 0 ||
      (value.updatedAt !== null &&
        (typeof value.updatedAt !== "string" || !Number.isFinite(Date.parse(value.updatedAt))))) {
    throw new Error("Invalid citation snapshot")
  }
  for (const paper of value.papers) {
    if (!record(paper) || typeof paper.title !== "string" || !paper.title.trim() ||
        typeof paper.authors !== "string" || typeof paper.venue !== "string" ||
        !Number.isInteger(paper.year) || !Array.isArray(paper.links) ||
        paper.links.some((link: unknown) => !record(link) ||
          typeof link.name !== "string" || typeof link.url !== "string" ||
          !/^https?:\/\//i.test(link.url))) {
      throw new Error("Invalid citation entry")
    }
  }
  return value as unknown as CitationSnapshot
}
