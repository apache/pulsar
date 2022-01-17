import * as React from "react";
import Layout from "@theme/Layout";
import CommonTable from "../components/CommonTable";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Translate, { translate } from "@docusaurus/Translate";
import resources from "../../data/resources.js";

export default function page(props) {
  return (
    <Layout>
      <div className="tailwind">
        <div className="my-12 container">
          <header className="postHeader">
            <h1>
              <Translate>Resources</Translate>
            </h1>
            <hr />
          </header>
          <h2>
            <Translate>Articles</Translate>
          </h2>
          <CommonTable
            header={["Forum", "Date", "Link"]}
            data={resources.articles.map((item) => {
              return {
                forum: {
                  text: item.forum,
                  href: item.forum_link,
                },
                date: item.date,
                link: {
                  text: item.title,
                  href: item.link,
                },
              };
            })}
          />
          <h2>
            <Translate>Presentations</Translate>
          </h2>
          <CommonTable
            header={["Forum", "Date", "Presenter", "Link"]}
            data={resources.presentations.map((item) => {
              return {
                forum: {
                  text: item.forum,
                  href: item.forum_link,
                },
                date: item.date,
                presenter: item.presenter,
                link: {
                  text: item.title,
                  href: item.link,
                },
              };
            })}
          />
          <h2>
            <Translate>Older Articles</Translate>
          </h2>
          <CommonTable
            header={["Forum", "Date", "Link"]}
            data={resources.older_articles.map((item) => {
              return {
                forum: {
                  text: item.forum,
                  href: item.forum_link,
                },
                date: item.date,
                link: [
                  {
                    text: item.title,
                    href: item.link,
                  },
                  {
                    text: item.title2,
                    href: item.link2,
                  },
                  {
                    text: item.title3,
                    href: item.link3,
                  },
                ],
              };
            })}
          />
          <h2>
            <Translate>Older Presentations</Translate>
          </h2>
          <CommonTable
            header={["Forum", "Date", "Presenter", "Link"]}
            data={resources.older_presentations.map((item) => {
              return {
                forum: {
                  text: item.forum,
                  href: item.forum_link,
                },
                date: item.date,
                presenter: item.presenter,
                link: {
                  text: item.title,
                  href: item.link,
                },
              };
            })}
          />
        </div>
      </div>
    </Layout>
  );
}
