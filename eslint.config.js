export default [
  {
    ignores: [
      "**/node_modules/**",
      "**/logs/**",
      "**/data/**",
      "**/public/**",
    ],
  },
  {
    files: ["**/*.{js,cjs}"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
    },
    rules: {},
  },
];
