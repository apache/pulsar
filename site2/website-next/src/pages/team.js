import * as React from "react";
import Layout from "@theme/Layout";
import TeamTable from "../components/TeamTable";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Translate, { translate } from "@docusaurus/Translate";
import team from "../../data/team";

export default function page(props) {
  return (
    <Layout>
      <div className="tailwind">
        <div className="my-12 container">
          <header className="postHeader">
            <h1>
              <translate>Team</translate>
            </h1>
            <hr />
          </header>
          <p>
            <translate>
              A successful project requires many people to play many roles. Some
              write code or documentation, while others are valuable as testers,
              submitting patches and suggestions.
            </translate>
          </p>
          <p>
            <translate>
              The team is comprised of PMC members, Committers and Contributors.
              Committers have direct access to the source of a project and
              actively evolve the codebase. Contributors improve the project
              through submission of patches and suggestions to be reviewed by
              the Committers. The number of Committers and Contributors to the
              project is unbounded. Get involved today. All contributions to the
              project are greatly appreciated.
            </translate>
          </p>

          <h2>
            <translate>Committers</translate>
          </h2>
          <p>
            <translate>
              The following is a list of developers with commit privileges that
              have directly contributed to the project in one way or another.
            </translate>
          </p>
          <TeamTable data={team.committers} />
        </div>
      </div>
    </Layout>
  );
}
