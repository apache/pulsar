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
    extend: {
      zIndex: {
        '1': '1',
        '2': '2',
        '3': '3',
        '4': '4',
        '5': '5',
      },
      backgroundColor: {
        'dark-grey' : '#464E56',
        'light-grey' : '#5d687b',
        'grey-blue' : '#6c93a7',
      },
      textColor: {
        'grey-blue' : '#6c93a7'
      }
    }
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
