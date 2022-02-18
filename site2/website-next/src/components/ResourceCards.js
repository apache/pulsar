import React from "react";
export default function ResourceCards(props) {
  // This is the string being entered into the search form field
  const searchString = props.search;
  const resList = props.resources;
  // we only want to return cards who's title includes the search string.
  const filteredRes = resList.filter((r)=>{
    return r.title.toLowerCase().includes(searchString.toLowerCase()) || (r.tags && r.tags.toLowerCase().includes(searchString.toLowerCase())) || (r.forum && r.forum.toLowerCase().includes(searchString.toLowerCase())) || (r.presenter && r.presenter.toLowerCase().includes(searchString.toLowerCase()));
  });
  const type = props.type;
  function ResCard({ forum, forum_link, presenter, date, title, link, tags }) {
    return (
      <div className="mb-4 sm:mb-0 resource-card type-resource bg-white p-6 shadow-lg relative flex flex-col">
        {forum && <h4 className="mb-4 uppercase relative z-5"><span className="font-light">FORUM //</span> <a href={forum_link}>{ forum }</a></h4>}
        <h3 className="mb-2 relative z-5"><a href={link}>{ title }</a></h3>
        {date && <h5>{ date }</h5>}
        {presenter && <p className="font-light relative z-5">Presented by <strong>{ presenter }</strong></p> }
        {tags && <div className="tags mt-4 text-sm font-light"><small>Tags: { tags }</small></div>}
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
       <h3>Sorry, no resources match your search.</h3>
      </section>
    )
  }
}
