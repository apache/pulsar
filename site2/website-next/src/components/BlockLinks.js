import React from "react";
import { docUrl } from "../utils/index";


const BlockLinks = (props) => {
  return(
    <div className="tailwind">
      <div className="py-12  sm:grid sm:grid-cols-2 md:grid-cols-3 gap-x-6 gap-y-6">
        {props.children}
      </div>
    </div>
  )
}
export default BlockLinks;