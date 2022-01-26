import React from "react";


export default function CaseStudyCards(props) {
  const csArr = props.cards;
  function ResCard({ company, link,  image, description }) {
    return (
      <div className="mb-4 sm:mb-0 resource-card bg-white p-6 shadow-lg relative flex flex-col">
        <div className="cs-logo flex content-center mb-4 relative z-5">
          {image && <img src={image} alt={company} />}
          
        </div>
        <h3 className="mb-2 relative"><a href={link} target="_blank">{ company }</a></h3>
        {description && <p className="mb-4 mt-4 font-light relative z-5">{ description }</p> }
        
        <a href={link} className="secondary-cta secondary-cta--small" target="_blank">See Case Study</a>
      </div>
    );
  }


  return (
    <section className="resource-cards py-12 mx-auto">
      <div className="inner sm:grid sm:grid-cols-2 md:grid-cols-3 gap-x-6 gap-y-6">
        {csArr.map((props, idx) => (
          <ResCard key={idx} {...props} />
        ))}
      </div>
    </section>
  );
}

