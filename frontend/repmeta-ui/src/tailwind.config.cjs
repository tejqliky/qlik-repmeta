module.exports = {
  content: [
    "./index.html",
    "./src/**/*.{ts,tsx,js,jsx}"
  ],
  theme: { extend: {} },
  plugins: [],
  // Optional: safelist for any dynamic classes you construct elsewhere
  // safelist: [
  //   { pattern: /(bg|text|border)-(blue|green|purple|orange|red|gray)-(50|100|600|700)/ }
  // ]
};