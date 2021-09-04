const fs = require("fs");
const path = require("path");
const TAB_REG =
  /<!--DOCUSAURUS_CODE_TABS-->(((?!<!--END_DOCUSAURUS_CODE_TABS-->).)*\n*)*<!--END_DOCUSAURUS_CODE_TABS-->/;

function removeTabTag(tab) {
  return tab
    .replace(/<!--DOCUSAURUS_CODE_TABS-->/, "")
    .replace(/<!--END_DOCUSAURUS_CODE_TABS-->/, "");
}

function findTabItemNameList(tab) {
  const _match = tab.match(/<!--(.*)-->/g);
  return _match.map((item) => {
    return item.replace("<!--", "").replace("-->", "");
  });
}

function replaceTabItemTag(tab) {
  const tab_item_reg = /<!--(.*)-->((((?!<!--).)*\n*)*)/;
  const _match = tab_item_reg.exec(tab);
  if (_match) {
    const tab_item_name = _match[1];
    const tab_item_content = _match[2];
    let tab_item_str = '<TabItem value="';
    tab_item_str += tab_item_name + '">' + tab_item_content + "</TabItem>\n";
    tab = tab.replace(tab_item_reg, tab_item_str);
    return replaceTabItemTag(tab);
  } else {
    return tab;
  }
}

function importTabComponent(data) {
  if (!/import Tabs from '@theme\/Tabs';/g.exec(data)) {
    return data.replace(
      /---((((?!---).)*\n*)*)---/,
      "---$1---" +
        "\n\nimport Tabs from '@theme/Tabs';\nimport TabItem from '@theme/TabItem';"
    );
  }
  return data;
}

function fixTab(data) {
  data = importTabComponent(data);
  const _match = TAB_REG.exec(data);
  if (_match) {
    let tab = _match[0];
    tab = removeTabTag(tab);
    const names = findTabItemNameList(tab);
    tab = replaceTabItemTag(tab);
    const names_map = names.map((item) => {
      return {
        label: item,
        value: item,
      };
    });
    const tab_tag_begin =
      '<Tabs \n  defaultValue="' +
      names[0] +
      '"\n  values={' +
      JSON.stringify(names_map, null, 2) +
      "}>";

    const tab_tag_end = "\n</Tabs>";
    tab = tab_tag_begin + tab + tab_tag_end;
    return fixTab(data.replace(TAB_REG, tab));
  } else {
    return data;
  }
}

function test() {
  let data = fs.readFileSync(
    path.join(__dirname, "../bak/schema-manage.md"),
    "utf8"
  );
  data = fixTab(data);
  console.log(data);
}

// test()

module.exports = fixTab;
