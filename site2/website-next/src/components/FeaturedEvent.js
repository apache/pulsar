import React from "react";
import clsx from "clsx";
const FeaturedEvent = (props) => {
  return(
    <div className={`col col-6 p-8 relative featured-card ${props.hidden === 'true' ? 'hidden' : ''}` }>
        <div className="featured-card--inner p-4">
            <h2 className="mb-8">Featured Event</h2>
            <h3 className="mb-2">{props.title}</h3>
            <h4 className="mb-8">{props.date}</h4>
            <p className="mb-4">{props.description}</p>
            <a className="secondary-cta" href={props.link} target="_blank">Learn More</a>
        </div>
    </div>
  )
}
export default FeaturedEvent;