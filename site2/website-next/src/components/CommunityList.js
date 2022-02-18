import React from "react";
export default function CommunityList(props) {
  const teamList = props.list;
  function TeamMember({ name, apacheId, roles }) {
    return (
        <div className="team-member pa-4">
            <p className="mb-0">
                { name } <span className="slanted-separator">//</span> <strong> {apacheId} </strong> <span className="slanted-separator">//</span> <em>{roles}</em>
            </p>
        </div>
    );
  }
  return (
    <section className="resource-cards pb-12 mx-auto">
      <div className=" md:grid md:grid-cols-2 lg:grid-cols-3 gap-x-2 gap-b-3">
        {teamList.map((props, idx) => (
          <TeamMember key={idx} {...props} />
        ))}
      </div>
    </section>
  );
}
