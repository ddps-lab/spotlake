import Link from "next/link"
import { Card, CardContent } from "@/components/ui/card"
import type { YearGroup } from "@/lib/publications"

/**
 * 연도를 왼쪽에 두고 그 해 논문 카드를 오른쪽에 쌓는다.
 * ddps.cloud/publication 과 같은 구성이다.
 *
 * 좁은 화면에서는 두 단이 무너져 연도가 카드 위로 올라간다.
 */
export function PublicationList({ groups }: { groups: YearGroup[] }) {
  if (groups.length === 0) {
    return (
      <p className="text-muted-foreground text-sm">
        No publications listed yet.
      </p>
    )
  }

  return (
    <div className="space-y-10">
      {groups.map((group) => (
        <div key={group.year} className="flex flex-col gap-4 sm:flex-row sm:gap-8">
          <h3 className="text-3xl font-bold tracking-tight text-muted-foreground sm:w-24 sm:shrink-0 sm:text-right">
            {group.year || "—"}
          </h3>
          <div className="flex-1 space-y-4">
            {group.items.map((pub) => (
              <Card key={pub.title}>
                <CardContent className="space-y-3">
                  <h4 className="font-semibold leading-snug text-balance">
                    {pub.title}
                  </h4>
                  {pub.authors && (
                    <p className="text-sm text-muted-foreground">{pub.authors}</p>
                  )}
                  {pub.venue && (
                    <p className="text-sm text-muted-foreground">{pub.venue}</p>
                  )}
                  {pub.links.length > 0 && (
                    <div className="flex flex-wrap gap-2 pt-1">
                      {pub.links.map((link) => (
                        <Link
                          key={link.url}
                          href={link.url}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center rounded-md border px-2.5 py-1 text-xs font-medium uppercase tracking-wide transition-colors hover:bg-accent hover:text-accent-foreground"
                        >
                          {link.name}
                        </Link>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}
