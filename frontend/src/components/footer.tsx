import Image from "next/image"
import Link from "next/link"
import { Github, Mail, MapPin, Phone } from "lucide-react"

// ddps.cloud 하단과 같은 구성이다. 배경이 늘 어두우므로 로고는 테마와
// 무관하게 밝은 쪽(logo_ddps_dark.svg)을 쓴다.
const DEPARTMENTS = [
  { name: "Department of Data Science", url: "https://hyds.hanyang.ac.kr/" },
  { name: "Department of Artificial Intelligence", url: "https://nextai.hanyang.ac.kr/" },
]

export function Footer() {
  return (
    <footer className="mt-16 w-full bg-[#131720] text-slate-300">
      <div className="mx-auto grid w-full max-w-7xl gap-10 px-4 py-12 md:grid-cols-2">
        <div className="space-y-3 text-sm">
          <Image
            src="/images/logo_ddps_dark.svg"
            alt="DDPS Lab"
            width={0}
            height={0}
            className="h-8 w-auto"
            style={{ width: "auto", height: "2rem" }}
          />
          <div className="space-y-1 text-slate-400">
            <p>Hanyang University</p>
            <p>Distributed Data Processing Systems Laboratory</p>
            <p>한양대학교 분산데이터처리시스템연구실</p>
          </div>
          <div className="space-y-1 text-slate-400">
            {DEPARTMENTS.map((d) => (
              <p key={d.url}>
                <Link
                  href={d.url}
                  target="_blank"
                  rel="noreferrer"
                  className="transition-colors hover:text-slate-200"
                >
                  - {d.name}
                </Link>
              </p>
            ))}
          </div>
          <p className="pt-2 font-medium text-slate-200">
            © {new Date().getFullYear()} DDPS Lab. All rights reserved.
          </p>
        </div>

        <div className="space-y-4 text-sm md:justify-self-end">
          <h2 className="text-base font-semibold text-white">Contact</h2>

          <div className="flex gap-3">
            <MapPin className="mt-0.5 h-4 w-4 shrink-0 text-slate-500" aria-hidden="true" />
            <div className="text-slate-400">
              <p className="font-medium text-slate-200">HANYANG UNIVERSITY SEOUL</p>
              <p>
                #521, Fusion Technology Center(FTC), 222, Wangsimni-ro,
                <br className="hidden sm:block" /> Seongdong-gu, Seoul, 04763, Korea
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <Mail className="h-4 w-4 shrink-0 text-slate-500" aria-hidden="true" />
            <Link
              href="mailto:ddpslab@hanyang.ac.kr"
              className="text-slate-400 transition-colors hover:text-slate-200"
            >
              ddpslab@hanyang.ac.kr
            </Link>
          </div>

          <div className="flex items-center gap-3">
            <Phone className="h-4 w-4 shrink-0 text-slate-500" aria-hidden="true" />
            <Link
              href="tel:+82-2-2220-2656"
              className="text-slate-400 transition-colors hover:text-slate-200"
            >
              +82 2-2220-2656
            </Link>
          </div>

          <Link
            href="https://github.com/ddps-lab/spotlake"
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-2 rounded-md border border-slate-700 px-3 py-2 font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
          >
            <Github className="h-4 w-4" aria-hidden="true" />
            GitHub
          </Link>
        </div>
      </div>
    </footer>
  )
}
