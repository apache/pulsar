const plugin = require("tailwindcss/plugin");
const colors = require("tailwindcss/colors");

module.exports = {
  purge: ["./src/**/*.html", "./src/**/*.js", "./src/**/*.tsx"],
  darkMode: false,
  theme: {
    colors: {
      ...colors,
      primary: "#198fff",
    },
  },
  variants: {
    extend: {},
  },
  plugins: [
    plugin(function ({ addBase, config }) {
      addBase({
        h1: {
          fontSize: config("theme.fontSize.4xl"),
          fontWeight: config("theme.fontWeight.bold"),
        },
        h2: {
          fontSize: config("theme.fontSize.3xl"),
          fontWeight: config("theme.fontWeight.bold"),
        },
        h3: {
          fontSize: config("theme.fontSize.2xl"),
          fontWeight: config("theme.fontWeight.bold"),
        },
        h4: {
          fontSize: config("theme.fontSize.xl"),
          fontWeight: config("theme.fontWeight.bold"),
        },
        h5: {
          fontSize: config("theme.fontSize.lg"),
          fontWeight: config("theme.fontWeight.bold"),
        },
        h6: {
          fontSize: config("theme.fontSize.base"),
          fontWeight: config("theme.fontWeight.bold"),
        },
      });
    }),
  ],
};
