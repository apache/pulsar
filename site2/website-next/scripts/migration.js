//Usage:
//Example
//node scripts/migration.js 2.7.3 "Concepts and Architecture" concepts
//node scripts/migration.js 2.7.3 "Concepts and Architecture" concepts fix

const fs = require("fs");
const path = require("path");
const nextSidebar = require("../sidebars");
const _ = require("lodash");
const fixTab = require("./fix-tab");

function travel(dir, callback) {
  fs.readdirSync(dir).forEach((file) => {
    var pathname = path.join(dir, file);
    if (fs.statSync(pathname).isDirectory()) {
      // travel(pathname, callback);
    } else {
      callback(pathname);
    }
  });
}

function fixTd(data, reg) {
  const _match = reg.exec(data);
  if (_match) {
    let _data = _match[0];
    _data = _data
      .replace(/^\s*\n/gm, "")
      .replace(/<td/, "<TMP")
      .replace(/<\/td/, "</TMP");
    let _new_data = data.replace(reg, _data);
    return fixTd(_new_data, reg);
  } else {
    return data.replace(/<TMP/g, "<td").replace(/<\/TMP/g, "</td");
  }
}

try {
  const args = process.argv.slice(2);
  const version = args[0];
  const category = args[1];
  const prefix = args[2];
  const fix = args[3];

  console.log(
    "args: ",
    args,
    "version: ",
    version,
    "category: ",
    category,
    "prefix: ",
    prefix,
    "fix: ",
    fix
  );

  let version_full = "version-" + version;
  let src = "../../website/versioned_docs/" + version_full;
  let dest = "../../website-next/versioned_docs/" + version_full;
  if (version == "next") {
    src = "../../docs";
    dest = "../../website-next/docs";
  }
  src = path.join(__dirname, src);
  dest = path.join(__dirname, dest);

  let sidebar_file = path.join(
    __dirname,
    "../../website/versioned_sidebars/" + version_full + "-sidebars.json"
  );
  if (version == "next") {
    sidebar_file = path.join(__dirname, "../../website/sidebars.json");
  }
  let sidebar = fs.readFileSync(sidebar_file, "utf8");
  sidebar = JSON.parse(sidebar);

  sidebar =
    sidebar[version == "next" ? "docs" : version_full + "-docs"][category];
  if (version != "next") {
    sidebar = sidebar.map((item) => {
      return item.substr(version_full.length + 1);
    });
  }
  if (!sidebar) {
    return;
  }
  let new_sidebar_file = path.join(
    __dirname,
    "../../website-next/versioned_sidebars/" + version_full + "-sidebars.json"
  );
  let new_sidebar = nextSidebar;
  if (version == "next") {
    new_sidebar_file = path.join(__dirname, "../../website-next/sidebars.json");
    if (!_.keyBy(new_sidebar.docsSidebar, "label")[category]) {
      new_sidebar.docsSidebar.push({
        type: "category",
        label: category,
        items: sidebar,
      });
    }
  } else {
    new_sidebar = fs.readFileSync(new_sidebar_file, "utf8");
    new_sidebar = JSON.parse(new_sidebar);
    if (
      !_.keyBy(new_sidebar[version_full + "/docsSidebar"], "label")[category]
    ) {
      new_sidebar[version_full + "/docsSidebar"].push({
        type: "category",
        label: category,
        items: sidebar.map((item) => {
          return {
            type: "doc",
            id: version_full + "/" + item,
          };
        }),
        collapsible: true,
        collapsed: true,
      });
    }
  }
  fs.writeFileSync(new_sidebar_file, JSON.stringify(new_sidebar, null, 2));

  // console.log("path: ", src, dest);
  travel(src, function (pathname) {
    const filename = path.basename(pathname);
    if (
      sidebar.includes(filename.substr(0, filename.length - 3)) ||
      (prefix && filename.startsWith(prefix))
    ) {
      console.log(filename);
      if (fix) {
        let reg = new RegExp("id:\\s*version-" + version + "-");
        let data = fs.readFileSync(pathname, "utf8");
        data = fixTab(data);
        data = data
          .replace(reg, "id: ")
          .replace(/<\/br>/g, "<br />")
          .replace(/<br>/g, "<br />")
          .replace(
            /<span style="(((?!:).)+):(((?!>).)+);"/g,
            '<span style={{color: "$3"}}'
          )
          .replace(/\]\(assets\//g, "](/assets/")
          .replace(/<table style="table"/g, '<table className={"table"}')
          .replace(/(<table.+>)/g, "$1\n<tbody>")
          .replace(/<\/\s*table.*>/g, "</tbody>\n</table>")
          // .replace(/<!--(.*)-->/g, "====$1====")
          .replace(/(```\w+)/gm, "\r\n$1")
          .replace(/^\s*```$/gm, "```");
        data = fixTd(data, /<td>((?!<\/td>).)*(\n((?!<\/td>).)*)+<\/td>/);

        fs.writeFileSync(path.join(dest, filename), data);
      } else {
        fs.copyFileSync(pathname, path.join(dest, filename));
      }
    }
  });
} catch (e) {
  console.error(e);
}
