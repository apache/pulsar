import React, { useState } from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import EcoCards from "../components/EcoCards";
import TabsUnstyled from '@mui/base/TabsUnstyled';
import TabsListUnstyled from '@mui/base/TabsListUnstyled';
import TabPanelUnstyled from '@mui/base/TabPanelUnstyled';
import TabUnstyled from '@mui/base/TabUnstyled';
import ecoObj from '@site/data/ecosystem.js';


// create combine the arrays from each category.
let allArr = [];
Object.keys(ecoObj).forEach(key => {
  allArr = [...allArr, ...ecoObj[key]];
});

export default function Home() {
  const [searchString, setSearch] = useState('');
  return (
    <Layout
      title={`Ecosystem`}
      description="Learn about the basics of using Apache Pulsar"
    >    
     <div className="page-wrap tailwind">
        <section className="hero">
          <div className="inner text--left">
            <div className="row">
              <div className="col col--8">
                <h1>Ecosystem</h1>
                <p>Our ecosystem includes a range of third-party Pulsar projects and tools that may be useful to end users. Please note that not all of these are supported by the community.</p>

              </div>
            </div>
          </div>
        </section>
        <section className="main-content waves-bg py-12 mb-24">
          
          <TabsUnstyled defaultValue={0} className="tabs tabs--resources block my-24 relative z-5">
            <TabsListUnstyled className="block text--center tabs-bar py-8 px-4">
              <TabUnstyled className="mx-2">All</TabUnstyled>
              <TabUnstyled className="mx-2">Connectors</TabUnstyled>
              <TabUnstyled className="mx-2">Adapters</TabUnstyled>
              <TabUnstyled className="mx-2">Tools</TabUnstyled>
            </TabsListUnstyled>
            <form className="search-form relative z10 text--center">
              <label className="block mb-4">Search by name or description key word: </label>
              <input type="text" className="ml-2 px-2" name="search" value={searchString} onChange={e => setSearch(e.target.value)} />
              <div className="inline-block px-4 cursor-pointer ml-4 underline underline-offset-1 text-sm font-light" onClick={e => setSearch('')} >Clear Search</div>
            </form>
            <TabPanelUnstyled value={0}><EcoCards search={searchString} type="Connector" resources={allArr} /></TabPanelUnstyled>
            <TabPanelUnstyled value={1}><EcoCards search={searchString} type="Connector" resources={ecoObj.connectors} /></TabPanelUnstyled>
            <TabPanelUnstyled value={2}><EcoCards search={searchString} type="Adapter" resources={ecoObj.adapters} /></TabPanelUnstyled>
            <TabPanelUnstyled value={3}><EcoCards search={searchString} type="Tool" resources={ecoObj.tools} /></TabPanelUnstyled>
          </TabsUnstyled>
        </section>
      </div>
    </Layout>
  );
}
