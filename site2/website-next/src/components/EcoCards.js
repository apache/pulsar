import React from "react";
export default function EcoCards(props) {
  // This is the string being entered into the search form field
  const searchString = props.search;
  const ecoList = props.resources;
  // we only want to return cards who's title includes the search string.
  const filteredRes = ecoList.filter((r)=>{
    return (r.name && r.name.toLowerCase().includes(searchString.toLowerCase())) ||  (r.description && r.description.toLowerCase().includes(searchString.toLowerCase()));
  });
  const type = props.type;
  function ResCard({ name, description, link }) {
    return (
      <div className="mb-4 sm:mb-0 resource-card type-resource bg-white p-6 shadow-lg relative flex flex-col">
      
        <h3 className="mb-2 relative z-5"><a href={link}>{ name }</a></h3>
        {description && <p className="mb-4 mt-4 font-light relative z-5">{description}</p>}
        <div className="mb-6"></div>
        <a href={link} className="secondary-cta secondary-cta--small" target="_blank">See {type}</a>
      </div>
    );
  }
  if(filteredRes.length){
    return (
      <section className="resource-cards py-12 mx-auto">
        <div className="inner sm:grid sm:grid-cols-2 md:grid-cols-3 gap-x-6 gap-y-6">
          {filteredRes.map((props, idx) => (
            <ResCard key={idx} {...props} />
          ))}
        </div>
      </section>
    );
  } else {
    return (
      <section className="resource-cards py-12 mx-auto text--center">
       <h3>Sorry, no {props.type.toLowerCase()} match your search.</h3>
      </section>
    )
  }
}
