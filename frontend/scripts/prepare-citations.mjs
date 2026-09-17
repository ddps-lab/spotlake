import { readFileSync, writeFileSync, mkdirSync } from "node:fs"
import { resolve, dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { load } from "js-yaml"

const frontend = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const output = process.argv[2]
if (!output) throw new Error("Usage: node scripts/prepare-citations.mjs <output-directory>")
const { ddps } = load(readFileSync(join(frontend, "src/data/publications.yaml"), "utf8"))
if (!Array.isArray(ddps) || !ddps.length) throw new Error("Missing DDPS publications")
const papers = ddps.map(({ title, links = [] }) => ({ title, links }))
mkdirSync(output, { recursive: true })
writeFileSync(join(output, "lab-papers.json"), JSON.stringify(papers))
