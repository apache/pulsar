const fs = require("fs");
const path = require("path");
const lodash = require("lodash");

function travel(dir, callback) {
  fs.readdirSync(dir).forEach((file) => {
    var pathname = path.join(dir, file);
    if (fs.statSync(pathname).isDirectory()) {
      travel(pathname, callback);
    } else {
      callback(pathname);
    }
  });
}

function fixMd(filepath) {
  let data = fs.readFileSync(filepath, "utf8");
  if (
    /^[ ]+<!--DOCUSAURUS_CODE_TABS-->/gm.test(data) ||
    /<!--DOCUSAURUS_CODE_TABS-->\s+([^<!--]+)$/gm.test(data)
  ) {
    //(fr|ko|pt-PT|zh-CN|zh-TW)
    //(version-(\d\.?)+)
    let locale = /(fr|ko|pt-PT|zh-CN|zh-TW)/.exec(filepath)[1];
    let filename = filepath.substr(filepath.lastIndexOf("/") + 1);
    let version = /(version-(\d\.?)+)/.exec(filepath);
    if (version) {
      version = version[1];
    }
    console.log(filepath, locale, version, filename);
    if (version) {
      fs.copyFileSync(
        path.join(__dirname, "../versioned_docs/", version, filename),
        filepath
      );
    } else {
      fs.copyFileSync(path.join(__dirname, "../../docs", filename), filepath);
    }
  }
}

travel(path.join(__dirname, "../translated_docs"), fixMd);
