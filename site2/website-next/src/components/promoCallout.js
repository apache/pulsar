import React from "react";
import { docUrl } from "../utils/index";


const PromoCallout = (props) => {
  return(
    <div className="promo-callout py-24 px-4 flex content-center">
      <div className="border-solid border-2 border-gray-800 w-auto mx-auto max-w-screen-sm py-2 px-8 rounded-full text--center">
        <strong>{props.text} <a href={props.url}>{props.linkText}</a></strong>
      </div>
    </div>
  )
}
export default PromoCallout;