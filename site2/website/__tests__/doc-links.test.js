const CWD = process.cwd();

const path = require('path');
const fs = require('fs');

const docsDir = path.join(CWD, '../', 'docs');


test('markdown doc links', async () => {
  const docsDir = path.join(CWD, '../', 'docs');
  const pages = new Set();

  // collect all doc pages
  fs.readdirSync(docsDir).forEach(file => {
    if (file.endsWith('.md')) {
      pages.add(file);
    }
  });

  const mdBrokenLinks = []
  pages.forEach(page => {
    const pageFile = path.join(docsDir, page)
    const mdLinks = getPageMDLinks(pageFile);

    const brokenLinks = []
    new Set(mdLinks).forEach(link => {
      if (!pages.has(link)) {
        brokenLinks.push(link);
      }
    })

    if (brokenLinks.length > 0) {
      mdBrokenLinks.push({page: page, brokenLinks: brokenLinks})
    }
  })
  
  // did we find some broken links?
  if (mdBrokenLinks.length > 0) {
    const brokenLinksJson = JSON.stringify(mdBrokenLinks, null, 2);
    const errorMessage = `Found the following broken links in docs pages:\n${brokenLinksJson}`
    console.log(errorMessage);
    expect(mdBrokenLinks.length).toBe(0)
  }
})


function getPageMDLinks(file) {
  if (!fs.existsSync(file)) {
    return [];
  }
  const content = fs.readFileSync(file, 'utf8');

  const mdLinks = [];
  // find any inline-style links to markdown files
  const linkRegex = /(?:\]\()(?:\.\/)?([^'")\]\s>]+\.md)/g;
  let linkMatch = linkRegex.exec(content);
  while (linkMatch !== null) {
    const link = linkMatch[1]
    // we are only checking links to other docs in this test
    if (!link.startsWith('http')) {
      mdLinks.push(linkMatch[1]);
    }
    linkMatch = linkRegex.exec(content);
  }

  return mdLinks;
}
