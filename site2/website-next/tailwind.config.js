const plugin = require("tailwindcss/plugin");

module.exports = {
  content: ["./src/**/*.html", "./src/**/*.js", "./src/**/*.tsx"],
  theme: {
    extend: {
      colors: {
        primary: "#198fff",
      },
    },
  },
  corePlugins: {
    preflight: false,
  },
};
