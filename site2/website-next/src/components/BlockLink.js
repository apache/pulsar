import React from "react";
import { docUrl } from "../utils/index";


const BlockLink = (props) => {
  return(
      <div className="block-link">
        <a className="h-full flex justify-center items-center" href={props.url}>
            <span>{props.title}</span>
        </a>
      </div>
  )
}
export default BlockLink;