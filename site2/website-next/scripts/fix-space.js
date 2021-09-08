const fs = require("fs");
const path = require("path");

const code_map = {};
let index = 0;

function fixSpace(data) {
  const code_reg = /(```\w*)((((?!```).)*\n*)+)```/;
  const _match = code_reg.exec(data);
  if (_match) {
    const code = _match[0];
    const TMP_KEY = "_TMP_CODE_KEY_" + index + "_";
    code_map[TMP_KEY] = code;
    index++;
    data = data.replace(code_reg, TMP_KEY);
    return fixSpace(data);
  } else {
    return data;
  }
}

function fixTmp(data) {
  const tmp_reg = /_TMP_CODE_KEY_(\d+)_/;
  const _match = tmp_reg.exec(data);
  if (_match) {
    data = data.replace(tmp_reg, code_map[_match[0]]);
    return fixTmp(data);
  } else {
    return data;
  }
}

function fix(data) {
  data = fixSpace(data);
  data = data.replace(/^[\t ]+/gm, "");
  data = fixTmp(data);
  return data;
}

function test() {
  let data = fs.readFileSync(
    path.join(__dirname, "../bak/io-quickstart.md"),
    "utf8"
  );
  data = fixSpace(data);
  data = data.replace(/^[\t ]+/gm, "");
  data = fixTmp(data);
  // console.log(data);
  fs.writeFileSync(path.join(__dirname, "../bak/io-quickstart-fixed.md"), data);
}

// test();

module.exports = fix;
