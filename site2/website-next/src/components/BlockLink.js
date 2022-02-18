import React from "react";
import { docUrl } from "../utils/index";


const BlockLink = (props) => {
  return(
      <div className="block-link my-4">
        <a className="h-full flex justify-center items-center z5 relative" href={props.url}>
            <span>{props.title}</span>
        </a>
      </div>
  )
}
export default BlockLink;