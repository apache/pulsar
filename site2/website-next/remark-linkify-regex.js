const visitWithParents = require("unist-util-visit-parents");
const flatMap = require("unist-util-flatmap");

function removeExtremes(regex, optionalFlags) {
  return new RegExp(
    regex.source.replace(/^\^/, "").replace(/\$$/, ""),
    optionalFlags || regex.flags
  );
}

function notInMarkdownLink(regex, optionalFlags) {
  return new RegExp(
    regex.source + "(?! *\\))(?! *])",
    optionalFlags || regex.flags
  );
}

function buildTextNode(props) {
  return { type: "text", value: props.value };
}

function buildLinkNode(props, children) {
  return {
    type: "link",
    title: props.title ? props.title : null,
    url: props.url,
    children,
  };
}

function h(type, props, children) {
  if (type === "text") return buildTextNode(props, children);
  if (type === "link") return buildLinkNode(props, children);
  throw new Error("mdast hyperscript not supported for type " + type);
}

function splitTextNode(textNode, inputRegex, parseFn) {
  const oldText = textNode.value;
  const regex = notInMarkdownLink(removeExtremes(inputRegex), "g");
  const newNodes = [];
  let startTextIdx = 0;
  let output;
  while ((output = regex.exec(oldText)) !== null) {
    const endTextIdx = output.index;
    if (startTextIdx !== endTextIdx) {
      newNodes.push(
        h("text", { value: oldText.slice(startTextIdx, endTextIdx) })
      );
    }
    const { link, text } = parseFn(output);
    newNodes.push(h("link", { url: link }, [h("text", { value: text })]));
    startTextIdx = regex.lastIndex;
  }
  const remainingText = oldText.slice(startTextIdx);
  if (remainingText.length > 0) {
    newNodes.push(h("text", { value: remainingText }));
  }
  return newNodes;
}

function linkifyRegex(regex, parseFn) {
  return () => (ast) => {
    visitWithParents(ast, "text", (textNode, parents) => {
      if (parents.length > 0 && parents[parents.length - 1].type === "link") {
        textNode._ignoreMe = true;
        return;
      }
    });

    flatMap(ast, (node) => {
      if (node.type !== "text") {
        return [node];
      }
      if (node._ignoreMe) {
        delete node._ignoreMe;
        return [node];
      }
      return splitTextNode(node, regex, parseFn);
    });

    return ast;
  };
}

module.exports = linkifyRegex;
