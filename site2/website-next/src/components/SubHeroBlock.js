import React from "react";
import styles from "./SubHeroBlock.module.css";
import ReactMarkdown from "react-markdown";
import { docUrl } from "../utils/index";


const SubHeroBlock = (props) => {
  return(
      <section className={`container subhero content-block ${props.className}`}>
        <div className="inner inner--narrow text--center">
            <h2>{props.heading}</h2>
            <p>{props.content} </p>
        </div>
      </section>
  )
}
export default SubHeroBlock;