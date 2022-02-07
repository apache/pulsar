import * as React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Translate, { translate } from "@docusaurus/Translate";
import users from "../../data/users";

export default function page(props) {
  return (
    <Layout>
      <div className="tailwind">
        <div className="my-12 container">
          <header className="postHeader">
            <h1>
              <translate>
                Companies using or contributing to Apache Pulsar
              </translate>
            </h1>
            <hr />
          </header>

          <div class="logo-wrapper">
            {users.map((c) =>
              (() => {
                if (c.hasOwnProperty("logo_white")) {
                  return (
                    <div className="logo-box-background-for-white">
                      <a href={c.url} title={c.name} target="_blank">
                        <img
                          src={c.logo}
                          alt={c.name}
                          className={c.logo.endsWith(".svg") ? "logo-svg" : ""}
                        />
                      </a>
                    </div>
                  );
                } else {
                  return (
                    <div className="logo-box">
                      <a href={c.url} title={c.name} target="_blank">
                        <img
                          src={c.logo}
                          alt={c.name}
                          className={c.logo.endsWith(".svg") ? "logo-svg" : ""}
                        />
                      </a>
                    </div>
                  );
                }
              })()
            )}
          </div>
        </div>
      </div>
    </Layout>
  );
}
