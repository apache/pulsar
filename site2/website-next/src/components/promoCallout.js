import React from "react";
import { docUrl } from "../utils/index";


const PromoCallout = (props) => {
  return(
    <div className="promo-callout py-24 flex content-center">
      <div className="promo w-auto mx-auto max-w-screen-sm py-2 px-8 rounded-full text--center">
        <p><strong>{props.text} <a href={props.url}>{props.linkText}</a></strong></p>
      </div>
    </div>
  )
}
export default PromoCallout;