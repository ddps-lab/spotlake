/** Optional local refresh of the fallback snapshot. AWS handles routine monthly updates. */
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { dirname, resolve, join } from "node:path"
import { fileURLToPath } from "node:url"
import { spawnSync } from "node:child_process"

const frontend = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const build = mkdtempSync(join(tmpdir(), "spotlake-citations-"))
try {
  const prepared = spawnSync(process.execPath, [join(frontend, "scripts/prepare-citations.mjs"), build], { stdio: "inherit" })
  if (prepared.error) throw prepared.error
  if (prepared.status !== 0) throw new Error("Could not prepare citation configuration")
  const args = [resolve(frontend, "../utility/monthly_citation_updater/lambda_function.py"),
    "--seed", join(build, "seed.json"), "--lab-papers", join(build, "lab-papers.json")]
  if (!process.argv.includes("--dry-run")) args.push("--output", join(frontend, "src/data/citations.json"))
  const result = spawnSync("python3", args, { stdio: "inherit" })
  if (result.error) throw result.error
  process.exitCode = result.status ?? 1
} finally {
  rmSync(build, { recursive: true, force: true })
}
