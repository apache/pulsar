import React from "react";
import { ReactSVG } from "react-svg";
import useBaseUrl from "@docusaurus/useBaseUrl";

export default function Svg(props) {
  console.log(JSON.stringify(props, null, 2));
  return (
    <ReactSVG
      src={useBaseUrl(props.src)}
      beforeInjection={(svg) => {
        svg.setAttribute(
          "style",
          "" +
            (!isNaN(parseFloat(props.width))
              ? "width: " + props.width + ";"
              : "") +
            (!isNaN(parseFloat(props.height))
              ? "height: " + props.height + ";"
              : "")
        );
        svg.setAttribute("class", props.className);
      }}
    />
  );
}
