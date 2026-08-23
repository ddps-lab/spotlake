import { ArrowUpRight, Construction, Mail } from "lucide-react"

const contactEmail = "ddpslab@hanyang.ac.kr"

export default function Home() {
  return (
    <section className="relative isolate flex min-h-[calc(100vh-7.5rem)] items-center justify-center overflow-hidden rounded-3xl border bg-card px-6 py-16 shadow-sm sm:px-12">
      <div
        className="absolute inset-0 -z-20 bg-[radial-gradient(circle_at_top_left,oklch(0.72_0.17_244_/_0.18),transparent_42%),radial-gradient(circle_at_bottom_right,oklch(0.78_0.14_190_/_0.15),transparent_38%)]"
        aria-hidden="true"
      />
      <div
        className="absolute inset-0 -z-10 opacity-[0.035] [background-image:linear-gradient(to_right,currentColor_1px,transparent_1px),linear-gradient(to_bottom,currentColor_1px,transparent_1px)] [background-size:32px_32px]"
        aria-hidden="true"
      />

      <div className="mx-auto flex max-w-3xl flex-col items-center text-center">
        <div className="mb-8 flex h-20 w-20 items-center justify-center rounded-2xl border border-blue-500/20 bg-blue-500/10 text-blue-600 shadow-lg shadow-blue-500/10 dark:text-blue-400">
          <Construction className="h-10 w-10" strokeWidth={1.75} aria-hidden="true" />
        </div>

        <div className="mb-5 inline-flex items-center gap-2 rounded-full border bg-background/70 px-4 py-2 text-xs font-semibold uppercase tracking-[0.22em] text-muted-foreground backdrop-blur-sm">
          <span className="h-2 w-2 animate-pulse rounded-full bg-amber-500" />
          Service update in progress
        </div>

        <h1 className="text-balance text-5xl font-bold tracking-tight sm:text-6xl md:text-7xl">
          Under Construction
        </h1>
        <p className="mt-6 max-w-2xl text-pretty text-base leading-7 text-muted-foreground sm:text-lg sm:leading-8">
          SpotLake를 더 나은 모습으로 개선하고 있습니다.
          <br className="hidden sm:block" />
          페이지 및 데이터 공유 관련 문의는 아래 이메일로 연락해 주세요.
        </p>

        <a
          href={`mailto:${contactEmail}`}
          className="group mt-10 inline-flex min-h-12 items-center gap-3 rounded-full bg-foreground px-6 py-3 font-medium text-background shadow-lg transition-transform hover:-translate-y-0.5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
        >
          <Mail className="h-5 w-5" aria-hidden="true" />
          <span>{contactEmail}</span>
          <ArrowUpRight className="h-4 w-4 transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5" aria-hidden="true" />
        </a>

        <p className="mt-12 text-sm text-muted-foreground">
          DDPS Lab · Hanyang University
        </p>
      </div>
    </section>
  )
}
