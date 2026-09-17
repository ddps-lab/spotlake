export interface PublicationLink {
  name: string
  url: string
}

export interface Publication {
  title: string
  authors: string
  venue: string
  year: number
  links: PublicationLink[]
}

/** 한 해에 묶인 논문들. 화면에서 연도 제목 옆에 카드로 늘어놓는다. */
export interface YearGroup {
  year: number
  items: Publication[]
}

/** 최신 연도부터, 같은 해 안에서는 제목 순. */
export function groupByYear(items: Publication[]): YearGroup[] {
  const byYear = new Map<number, Publication[]>()
  for (const p of items) {
    const bucket = byYear.get(p.year)
    if (bucket) bucket.push(p)
    else byYear.set(p.year, [p])
  }
  return [...byYear.entries()]
    .sort(([a], [b]) => b - a)
    .map(([year, items]) => ({
      year,
      items: items.sort((a, b) => a.title.localeCompare(b.title)),
    }))
}
