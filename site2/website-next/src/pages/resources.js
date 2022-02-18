import React, { useState } from "react";
import Layout from "@theme/Layout";
import ResourceCards from "../components/ResourceCards";
import TabsUnstyled from '@mui/base/TabsUnstyled';
import TabsListUnstyled from '@mui/base/TabsListUnstyled';
import TabPanelUnstyled from '@mui/base/TabPanelUnstyled';
import TabUnstyled from '@mui/base/TabUnstyled';
import resObj from '../../data/resources.js';

// this file contains the resource data needed to fill the cards.
export default function Resources() {
  // This is the search field state.
  // state is set to an empty string by default but changes onChange of the search field
  const [searchString, setSearch] = useState('');
  return (
    <Layout
      title={`Resources`}
      description="Learn about the basics of using Apache Pulsar"
    >    
      <div className="page-wrap tailwind">
        <section className="hero">
          <div className="inner text--left">
            <div className="row">
              <div className="col col--8">
                <h1>Resources</h1>
                <p>Find Apache Pulsar tutorials, how-tos and other technical content by searching with keywords.</p>

              </div>
            </div>
          </div>
        </section>
        <section className="main-content waves-bg py-12 mb-24">
          
          <TabsUnstyled defaultValue={0} className="tabs tabs--resources block my-24 relative z-5">
            <TabsListUnstyled className="block text--center tabs-bar py-8 px-4">
              <TabUnstyled className="mx-2">Articles</TabUnstyled>
              <TabUnstyled className="mx-2">Presentations</TabUnstyled>
            </TabsListUnstyled>
            <form className="search-form relative z10 text--center">
              <label className="block mb-4">Search by keyword, presenter, forum, or tag: </label>
              <input type="text" className="ml-2 px-2" name="search" value={searchString} onChange={e => setSearch(e.target.value)} />
              <div className="inline-block px-4 cursor-pointer ml-4 underline underline-offset-1 text-sm font-light" onClick={e => setSearch('')} >Clear Search</div>
            </form>
            <TabPanelUnstyled value={0}><ResourceCards search={searchString} type="Article" resources={resObj.articles} /></TabPanelUnstyled>
            <TabPanelUnstyled value={1}><ResourceCards search={searchString} type="Presentation" resources={resObj.presentations} /></TabPanelUnstyled>
          </TabsUnstyled>
        </section>
      </div>
    </Layout>
  );
}
