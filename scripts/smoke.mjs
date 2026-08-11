import { spawnSync } from "node:child_process"
import { mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"

const entries = {
	".": ["createJobs", "defineJob", "defineHandler", "redisDriver"],
	"./testing": ["memoryDriver"],
	"./dashboard": ["JuibsDashboard"],
}
const subpaths = Object.keys(entries)
const root = fileURLToPath(new URL("..", import.meta.url))
const runtime = typeof Bun === "undefined" ? `node ${process.version}` : `bun ${Bun.version}`

function run(command, args, cwd) {
	const result = spawnSync(command, args, { cwd, encoding: "utf8" })

	if (result.error) {
		throw new Error(`${command} ${args.join(" ")} failed to start: ${result.error.message}`)
	}

	if (result.status !== 0) {
		throw new Error(
			`${command} ${args.join(" ")} exited ${result.status}\n${result.stdout}${result.stderr}`,
		)
	}

	return result.stdout
}

function findTarball(directory) {
	const tarballs = readdirSync(directory).filter((entry) => entry.endsWith(".tgz"))

	if (tarballs.length !== 1) {
		throw new Error(`expected one tarball in ${directory}, found ${tarballs.length}`)
	}

	return join(directory, tarballs[0])
}

const check = `
const seen = new Map()

for (const [subpath, expected] of Object.entries(${JSON.stringify(entries)})) {
	const specifier = \`@juicerq/juibs\${subpath.slice(1)}\`
	const namespace = await import(specifier)
	const missing = expected.filter((name) => namespace[name] === undefined)

	if (missing.length) {
		throw new Error(\`\${subpath} is missing \${missing.join(", ")}\`)
	}

	const resolved = import.meta.resolve(specifier)

	if (seen.has(resolved)) {
		throw new Error(\`\${subpath} resolves to \${resolved}, already used by \${seen.get(resolved)}\`)
	}

	seen.set(resolved, subpath)
	console.log(\`ok \${subpath.padEnd(12)} -> \${resolved.split("/").at(-1)}\`)
}

if (seen.size !== ${subpaths.length}) {
	throw new Error(\`expected ${subpaths.length} distinct files, got \${seen.size}\`)
}
`

const workspace = mkdtempSync(join(tmpdir(), "juibs-smoke-"))
const packed = join(workspace, "packed")

console.log(`smoke: ${runtime}`)

try {
	mkdirSync(packed)
	run("bun", ["pm", "pack", "--destination", packed], root)

	writeFileSync(
		join(workspace, "package.json"),
		`${JSON.stringify({ name: "juibs-smoke", private: true, type: "module" }, null, 2)}\n`,
	)
	writeFileSync(join(workspace, "check.mjs"), check)

	run("bun", ["install", findTarball(packed)], workspace)
	process.stdout.write(run(process.execPath, [join(workspace, "check.mjs")], workspace))
	console.log(`smoke: ${subpaths.length} subpaths resolved to distinct files`)
} catch (error) {
	console.error(`smoke failed under ${runtime}`)
	console.error(error.message)
	process.exitCode = 1
} finally {
	rmSync(workspace, { recursive: true, force: true })
}
