const { constants } = require("buffer");
const fs = require("fs");
const path = require("path");

const code_map = {};
let index = 0;

function fix(data) {
  const _reg = /^(([\t ]*)>[\t ]*[^><].*\n)+/m;
  const _type_reg = /#*[\t ]*\**(Note|Tip)+\**[\t ]*/i;
  const _match = _reg.exec(data);
  if (_match) {
    let txt = _match[0];
    txt = txt.replace(/^([\t ]*)>[\t ]*/gm, "$1");

    let type_match = _type_reg.exec(txt);
    let type = "note";
    if (type_match) {
      type = type_match[1] + "";
      type = type.toLowerCase();
    }
    txt = txt.replace(_type_reg, "\n");
    data = data
      .replace(_reg, "$2:::" + type + "\n\n" + txt + "\n$2:::\n\n")
      .replace(/^([ ]{4})\s*:::/gm, "$1:::");
    data = data.replace(/:::(note|tip)(\s*\n)*/g, ":::$1\n\n");
    return fix(data);
  } else {
    return data;
  }
}

function test() {
  let data = fs.readFileSync(
    path.join(__dirname, "../bak/io-quickstart.md"),
    "utf8"
  );
  data = fix(data);
  // console.log(data);
  fs.writeFileSync(path.join(__dirname, "../bak/fix-tip-note.md"), data);
}

test();

module.exports = fix;
