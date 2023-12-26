const stylistic = require("@stylistic/eslint-plugin");

const customized = stylistic.configs.customize({
  indent: 2,
  quotes: "double",
  semi: true,
  jsx: true,
});

module.exports = {
  extends: ["eslint:recommended", "plugin:@typescript-eslint/recommended"],
  parser: "@typescript-eslint/parser",
  plugins: ["@typescript-eslint", "@stylistic"],
  root: true,
  rules: {
    ...customized.rules,
    "@stylistic/brace-style": ["error", "1tbs"],
    "@typescript-eslint/no-non-null-assertion": "off",
    "@typescript-eslint/no-explicit-any": "off",
    "@typescript-eslint/no-var-requires": "off",
  },
  ignorePatterns: ["test/*", "dist/*", "*.js", "murmurHash3.ts", "packages/**/dist/*", "packages/**/build/*"],
};
