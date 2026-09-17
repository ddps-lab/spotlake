import { readFileSync } from "node:fs"
import { join } from "node:path"
import { load } from "js-yaml"

import { groupByYear, type Publication } from "./publication-data"

const DATA_FILE = join(process.cwd(), "src", "data", "publications.yaml")

/**
 * 항목 하나를 화면에서 쓸 모양으로 맞춘다.
 *
 * 손으로 쓰는 파일이라 title 만 있고 나머지가 비어 있을 수 있다. 빈 값은
 * 빈 문자열로 두고, 화면에서 그 줄을 통째로 건너뛴다.
 */
function normalize(raw: unknown): Publication[] {
  if (!Array.isArray(raw)) return []
  return raw
    .filter((p): p is Record<string, unknown> => typeof p === "object" && p !== null)
    .map((p) => ({
      title: String(p.title ?? "").trim(),
      authors: String(p.authors ?? "").trim(),
      venue: String(p.venue ?? "").trim(),
      // 연도가 비면 목록 맨 뒤로 가도록 0으로 둔다.
      year: Number(p.year) || 0,
      links: Array.isArray(p.links)
        ? (p.links as Record<string, unknown>[])
            .filter((l) => l && l.url)
            .map((l) => ({ name: String(l.name ?? "LINK"), url: String(l.url) }))
        : [],
    }))
    .filter((p) => p.title !== "")
}

/**
 * Read the manually maintained DDPS Lab publications.
 *
 * 정적 export라 빌드할 때 한 번만 돈다. 파일 읽기가 클라이언트 번들에
 * 들어가지 않도록 서버 컴포넌트에서만 부른다.
 */
export function getPublications() {
  const data = load(readFileSync(DATA_FILE, "utf8")) as Record<string, unknown> | null

  return {
    ddps: groupByYear(normalize(data?.ddps)),
  }
}
