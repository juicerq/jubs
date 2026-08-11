import { defineConfig, type UserConfig } from "tsdown"

const config: UserConfig = defineConfig({
	entry: {
		index: "src/index.ts",
		testing: "src/testing/index.ts",
		dashboard: "src/dashboard/index.ts",
	},
	format: "esm",
	platform: "node",
	target: "node20",
	fixedExtension: false,
	dts: true,
	exports: false,
	clean: true,
	treeshake: true,
})

export default config
