const users = require(`../../data/users.js`);
const featuredUsers = users.filter((x) => x.hasOwnProperty("featured"));
featuredUsers.sort((a, b) => (a.featured > b.featured ? 1 : -1));

const siteConfig = require(`../../docusaurus.config.js`);

export function imgUrl(img) {
  return siteConfig.baseUrl + "img/" + img;
}

export function docUrl(doc, language, version) {
  return (
    siteConfig.baseUrl +
    "docs/" +
    (language ? language + "/" : "") +
    (version ? version + "/" : "") +
    (doc ? doc : "")
  );
}

export function pageUrl(page, language) {
  return siteConfig.baseUrl + (language ? language + "/" : "") + page;
}

export function githubUrl() {
  return siteConfig.customFields.githubUrl;
}
