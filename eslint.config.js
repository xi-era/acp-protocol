import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  {
    ignores: ["**/dist/**", "**/node_modules/**", "**/*.cjs"],
  },
  {
    rules: {
      "no-undef": "off", // TypeScript handles undefined globals; no-undef misfires on Node built-ins
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "@typescript-eslint/consistent-type-imports": "error",
      "@typescript-eslint/no-explicit-any": "off", // protocol boundary code needs deliberate any
    },
  }
);
