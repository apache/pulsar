const plugin = require("tailwindcss/plugin");

module.exports = {
  content: ["./src/**/*.html", "./src/**/*.js", "./src/**/*.tsx"],
  theme: {
    extend: {
      colors: {
        primary: "#198fff",
      },
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
        'dark-blue' : '#084a88'
      },
      textColor: {
        'grey-blue' : '#6c93a7'
      }
    }
  },
  corePlugins: {
    preflight: false,
  },
};
