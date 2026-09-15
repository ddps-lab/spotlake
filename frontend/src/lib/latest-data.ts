/**
 * 웹에 공개하는 최신 데이터.
 *
 * spotlake-public-daily Lambda가 하루 한 번 만들어 CloudFront로 서빙한다.
 * 내부에서 쓰는 10분 주기 데이터가 아니라 공개용 저해상도 사본이다.
 *
 * 교차 출처 요청이므로 배포에 CORS 응답 헤더 정책이 붙어 있어야 한다. 허용
 * 출처는 정책에 목록으로 박혀 있고 http://localhost:3000 이 들어 있으므로,
 * 로컬에서 확인할 때는 기본 포트인 3000 으로 띄워야 한다.
 *
 * 파일은 gzip으로 저장되어 있고 Content-Encoding이 붙어 있어서 브라우저가
 * 알아서 푼다. fetch 쪽에서 따로 할 일은 없다.
 */

export const DATA_ORIGIN = "https://d2krkjqajp4l0e.cloudfront.net"

export const VENDORS = ["AWS", "GCP", "AZURE"] as const
export type Vendor = (typeof VENDORS)[number]

/** 화면에 쓰는 이름과 아이콘. 아이콘은 테마에 따라 두 벌이 있다. */
export const VENDOR_INFO: Record<Vendor, { name: string; icon: string }> = {
  AWS: { name: "Amazon Web Services", icon: "ic_aws" },
  GCP: { name: "Google Cloud Platform", icon: "ic_gcp" },
  AZURE: { name: "Microsoft Azure", icon: "ic_azure" },
}

export const vendorIcon = (vendor: Vendor, dark: boolean) =>
  `/images/${VENDOR_INFO[vendor].icon}_${dark ? "dark" : "light"}.png`

const FILE: Record<Vendor, string> = {
  AWS: "latest_aws.json",
  GCP: "latest_gcp.json",
  AZURE: "latest_azure.json",
}

/** meta.json 구조. 벤더별로 원본이 마지막으로 바뀐 시각이 들어 있다. */
export interface LatestMeta {
  generated_at: string
  files: Record<string, { source_last_modified: string; bytes: number; gzip_bytes: number }>
}

export async function fetchMeta(signal?: AbortSignal): Promise<LatestMeta> {
  const res = await fetch(`${DATA_ORIGIN}/meta.json`, { signal })
  if (!res.ok) throw new Error(`meta.json ${res.status}`)
  return res.json()
}

export async function fetchVendorData<T>(vendor: Vendor, signal?: AbortSignal): Promise<T[]> {
  const res = await fetch(`${DATA_ORIGIN}/${FILE[vendor]}`, { signal })
  if (!res.ok) throw new Error(`${FILE[vendor]} ${res.status}`)
  return res.json()
}

/** 그 벤더 데이터가 실제로 수집된 시각. 없으면 전체 생성 시각으로 대신한다. */
export function lastUpdated(meta: LatestMeta | null, vendor: Vendor): string | null {
  if (!meta) return null
  return meta.files?.[FILE[vendor]]?.source_last_modified ?? meta.generated_at ?? null
}

/** "2026-09-10T23:54:31Z" -> "2026-09-10 23:54 UTC" */
export function formatUtc(iso: string | null): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  const p = (n: number) => String(n).padStart(2, "0")
  return (
    `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} `
    + `${p(d.getUTCHours())}:${p(d.getUTCMinutes())} UTC`
  )
}

/**
 * 벤더마다 컬럼 이름이 달라서 필터가 쓸 키를 여기서 정리한다.
 * az가 없는 벤더(GCP)는 그 항목을 화면에서 통째로 숨긴다.
 */
export const FIELDS: Record<Vendor, { instance: string; region: string; az?: string }> = {
  AWS: { instance: "InstanceType", region: "Region", az: "AZ" },
  GCP: { instance: "InstanceType", region: "Region" },
  AZURE: { instance: "InstanceType", region: "Region", az: "AvailabilityZone" },
}
