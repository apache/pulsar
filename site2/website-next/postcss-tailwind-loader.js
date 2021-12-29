module.exports = function (context, options) {
  return {
    name: "postcss-tailwindcss-loader",
    configurePostCss(postcssOptions) {
      postcssOptions.plugins.push(
        require("postcss-import"),
        require("tailwindcss"),
        require("postcss-nested"),
        require("autoprefixer")
      );
      return postcssOptions;
    },
  };
};
