/**
 * SpotLake 논문을 인용한 외부 논문 목록을 받아 publications.yaml 의
 * citing 항목에 더한다.
 *
 *   node scripts/fetch-citations.mjs
 *   node scripts/fetch-citations.mjs --dry-run    파일을 고치지 않고 결과만 본다
 *
 * 출처는 Semantic Scholar 다. Google Scholar 가 가장 많이 잡지만 자동 수집을
 * 막아두어 쓸 수 없고, iframe 임베드도 X-Frame-Options 로 차단된다. 공개
 * API 중에서는 Semantic Scholar 가 가장 많이 잡는다.
 *
 *   Google Scholar    46편   자동 수집 불가
 *   Semantic Scholar  32편   이 스크립트가 쓰는 출처
 *   OpenAlex          19편
 *
 * 그래도 Google Scholar 보다는 적으므로 About 페이지에 Scholar 인용 목록
 * 링크를 같이 두어 전체를 볼 수 있게 한다.
 *
 * 학회명이 비는 항목은 DOI 로 Crossref 에 한 번 더 물어 채운다.
 *
 * citing 항목에 새로 찾은 논문만 더한다. 이미 있는 항목은 손대지 않으므로
 * 학회명을 다듬거나 링크를 추가해 둔 것이 다시 실행해도 그대로 남는다.
 * 목록에서 빼고 싶은 논문은 직접 지우면 되는데, 다음 실행에서 다시 들어온다.
 * 영구히 제외하려면 아래 EXCLUDE 에 DOI 를 적는다.
 *
 * ddps 항목과 파일의 주석은 건드리지 않는다. 연구실 논문은 손으로 관리하는
 * 영역이고, 인용 목록에 섞여 나오면 같은 논문이 두 번 보이므로 저자 이름과
 * DOI 두 가지로 걸러낸다.
 */

import { readFileSync, writeFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
// js-yaml 은 CommonJS 라 ESM 에서 이름 붙은 내보내기를 못 쓴다.
import { dump as yamlDump, load as yamlLoad } from "js-yaml"

const MAILTO = process.env.CONTACT_EMAIL ?? "ddpslab@hanyang.ac.kr"

// SpotLake 를 인용한 것으로 볼 논문들. 판본과 후속 데모 논문이 각각 따로
// 집계되므로 전부 본다. Google Scholar 는 이런 것들을 한 묶음으로 세기
// 때문에 하나만 보면 수가 크게 모자란다.
const SPOTLAKE_PAPERS = [
  "DOI:10.1109/IISWC55918.2022.00029", // IISWC 2022 (원 논문)
  "arXiv:2202.02973",                  // arXiv 판
  "DOI:10.1145/3543873.3587314",       // WWW 2023 데모
]

// 이 이름이 저자에 있으면 DDPS Lab 논문으로 보고 제외한다.
//
// Semantic Scholar 는 PDF 에서 저자를 뽑다 보니 표기가 흔들린다. 같은 분이
// Kyungyong Lee / Kyung-Koo Lee / Kyung-A Lee 로 제각각 나온다. 하이픈과
// 공백을 지우고 비교해서 이런 변형을 함께 잡는다.
const LAB_AUTHORS = [
  "Kyungyong Lee",
  "Kyung-Koo Lee",
  "Kyung-A Lee",
  "Kyungyong Lee",
]
const flat = (s) => s.toLowerCase().replace(/[-.\s]/g, "")
const LAB_KEYS = new Set(LAB_AUTHORS.map(flat))

// 목록에 넣고 싶지 않은 논문의 DOI. 여기 적으면 다시 실행해도 안 들어온다.
const EXCLUDE = new Set([
  // "10.1109/example.2026.12345678",
])

const YAML_FILE = join(
  dirname(fileURLToPath(import.meta.url)), "..", "src", "data", "publications.yaml",
)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

/**
 * Semantic Scholar 는 키 없이 쓰면 속도 제한이 빡빡해서 429 가 자주 난다.
 * 기다렸다 다시 시도한다. 간격을 점점 늘린다.
 */
const get = async (url, tries = 8) => {
  for (let i = 0; ; i++) {
    const res = await fetch(url, { headers: { "User-Agent": `spotlake-site (${MAILTO})` } })
    if (res.ok) return res.json()
    if (res.status !== 429 || i >= tries - 1) {
      throw new Error(`${res.status} ${res.statusText} — ${url}`)
    }
    const wait = 5000 * (i + 1)
    console.log(`  속도 제한, ${wait / 1000}초 후 재시도 (${i + 1}/${tries - 1})`)
    await sleep(wait)
  }
}

/** Semantic Scholar 는 한 번에 1000건까지 준다. offset 으로 넘긴다. */
async function citingPapers(paperId) {
  const fields = "title,year,authors,venue,externalIds"
  const found = []
  for (let offset = 0; ; offset += 100) {
    const page = await get(
      `https://api.semanticscholar.org/graph/v1/paper/${encodeURIComponent(paperId)}`
      + `/citations?fields=${fields}&limit=100&offset=${offset}`,
    )
    const rows = page.data ?? []
    found.push(...rows.map((r) => r.citingPaper))
    if (rows.length < 100) break
    await sleep(1200) // 연속 호출도 제한에 걸린다
  }
  return found
}

/** Semantic Scholar 에 학회명이 없을 때만 Crossref 를 부른다. */
async function venueFromCrossref(doi) {
  if (!doi) return null
  try {
    const m = (await get(`https://api.crossref.org/works/${doi}?mailto=${MAILTO}`)).message
    return m["container-title"]?.[0] ?? m.event?.name ?? null
  } catch {
    return null // 등록 안 된 DOI 도 있다. 그 경우 학회명 없이 둔다.
  }
}

const bareDoi = (doi) => (doi ?? "").replace(/^https?:\/\/(dx\.)?doi\.org\//, "").toLowerCase()

const normalize = (p) => {
  const ext = p.externalIds ?? {}
  return {
    title: (p.title ?? "").trim(),
    authors: (p.authors ?? []).map((a) => a.name).filter(Boolean),
    venue: p.venue || null,
    year: p.year ?? null,
    doi: bareDoi(ext.DOI),
    arxiv: ext.ArXiv ?? null,
    id: p.paperId ?? null,
  }
}

/** 항목에서 DOI 만 뽑는다. 같은 논문인지 비교할 때 쓴다. */
const doiOf = (entry) =>
  (entry.links ?? []).map((l) => bareDoi(l.url)).find(Boolean) ?? ""

/**
 * 새 항목을 citing 끝에 덧붙인다.
 *
 * 전체를 다시 찍으면 따옴표 서식이 바뀌어 손으로 쓴 줄까지 diff 에 잡힌다.
 * 기존 줄은 한 글자도 건드리지 않고 뒤에만 붙여서 변경분이 새 논문만 남게
 * 한다. 연도 순 정렬은 화면에서 하므로 파일 안의 순서는 상관없다.
 */
function appendCiting(original, entries) {
  const marker = original.search(/^citing:[ \t]*$/m)
  if (marker === -1) {
    throw new Error("publications.yaml 에서 'citing:' 줄을 찾지 못했습니다.")
  }
  const rest = original.slice(marker).split("\n").slice(1)
  if (rest.findIndex((l) => /^[A-Za-z_][\w-]*:/.test(l)) !== -1) {
    throw new Error(
      "citing 뒤에 다른 항목이 있습니다. 이 스크립트는 citing 이 마지막일 때만"
      + " 안전하게 덧붙입니다. 순서를 되돌리거나 스크립트를 고치세요.",
    )
  }

  // 항목마다 따로 찍어야 사이에 빈 줄을 넣을 수 있다. flowLevel 은 links 의
  // { name, url } 을 손으로 쓴 ddps 항목과 같은 한 줄 형태로 유지한다.
  const body = entries
    .map((e) =>
      yamlDump([e], { lineWidth: -1, noRefs: true, flowLevel: 3 })
        .replace(/\n+$/, "")
        .split("\n")
        .map((l) => (l ? "  " + l : l))
        .join("\n"),
    )
    .join("\n\n")
  return original.replace(/\n+$/, "") + "\n\n" + body + "\n"
}

const main = async () => {
  const dryRun = process.argv.includes("--dry-run")
  const original = readFileSync(YAML_FILE, "utf8")
  const parsed = yamlLoad(original) ?? {}
  const labDois = new Set((parsed.ddps ?? []).map(doiOf).filter(Boolean))

  const seen = new Map()
  for (const [i, id] of SPOTLAKE_PAPERS.entries()) {
    if (i) await sleep(3000) // 논문 사이에도 쉬어야 제한에 덜 걸린다
    for (const p of await citingPapers(id)) {
      const n = normalize(p)
      if (!n.title) continue
      const key = n.doi || n.id || n.title.toLowerCase()
      if (!seen.has(key)) seen.set(key, n)
    }
  }

  const all = [...seen.values()]

  // PDF 파싱이 깨진 항목이 섞여 들어온다. 연도가 없거나 제목이 문장 조각인
  // 것들("This paper is", 학술지 이름만 있는 것 등)을 걸러낸다.
  const usable = all.filter(
    (p) => Number.isInteger(p.year) && p.title.length >= 20 && p.title.split(/\s+/).length >= 4,
  )
  const junk = all.length - usable.length
  if (junk) console.log(`제목이나 연도가 깨진 항목 ${junk}편 제외`)

  const external = usable.filter(
    (p) => !p.authors.some((a) => LAB_KEYS.has(flat(a)))
      && !labDois.has(p.doi)
      && !EXCLUDE.has(p.doi),
  )
  console.log(
    `인용 ${usable.length}편 중 외부 ${external.length}편`
    + ` (연구실 논문과 제외 목록 ${usable.length - external.length}편 뺌)`,
  )

  // 이미 목록에 있는 논문은 손대지 않는다. 학회명을 다듬거나 링크를 더해 둔
  // 것이 덮어써지면 안 되기 때문이다. 새 논문만 골라 학회명을 채운다.
  const existing = parsed.citing ?? []
  const known = new Set(existing.map(doiOf).filter(Boolean))
  const knownTitles = new Set(
    existing.map((e) => String(e.title ?? "").trim().toLowerCase()).filter(Boolean),
  )
  // DOI 가 없는 논문도 있어서 제목으로도 한 번 더 거른다.
  const fresh = external.filter(
    (p) => !(p.doi && known.has(p.doi)) && !knownTitles.has(p.title.toLowerCase()),
  )

  let filled = 0
  for (const p of fresh) {
    if (!p.venue) {
      p.venue = await venueFromCrossref(p.doi)
      if (p.venue) filled++
    }
  }

  const added = fresh.map((p) => {
    const links = []
    if (p.doi) links.push({ name: "DOI", url: `https://doi.org/${p.doi}` })
    else if (p.arxiv) links.push({ name: "ARXIV", url: `https://arxiv.org/abs/${p.arxiv}` })
    return {
      title: p.title,
      authors: p.authors.join(", "),
      venue: p.venue ?? "",
      year: p.year,
      links,
    }
  })

  console.log(`기존 ${existing.length}편 유지, 새로 ${added.length}편 추가`)
  if (added.length) {
    console.log(`  (그중 학회명을 Crossref 로 채운 것 ${filled}편)`)
    for (const e of added) console.log(`  + ${e.year}  ${e.title.slice(0, 66)}`)
  }

  if (!added.length) {
    console.log("추가할 논문이 없어 파일을 고치지 않았습니다.")
    return
  }
  if (dryRun) {
    console.log("\n--dry-run 이라 파일을 고치지 않았습니다.")
    return
  }

  writeFileSync(YAML_FILE, appendCiting(original, added), "utf8")
  console.log(
    `\npublications.yaml 의 citing 이 ${existing.length + added.length}편이 되었습니다.`,
  )
  console.log("기존 줄과 ddps 항목, 주석은 그대로입니다. git diff 로 확인하세요.")
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
