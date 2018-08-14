const puppeteer = require('puppeteer')
const axios = require('axios');


const CWD = process.cwd();
const siteConfig = require(`${CWD}/siteConfig.js`);

const timeoutMs = 60000;

let browser;
let navLinks = [];
let navGroups = [];

function standaloneDocUrl() {
  return `http://localhost:3000${siteConfig.baseUrl}docs/standalone`
}

async function loadNavLinks() {
  let page = await browser.newPage();
  await page.goto(standaloneDocUrl());
  const hrefs = await page.evaluate(
    () => Array.from(document.body.querySelectorAll('a.navItem[href]'), ({ href }) => href)
  );
  navLinks = hrefs;
  const ng = await page.evaluate(
    () => Array.from(document.body.querySelectorAll('.navGroups'))
  );
  navGroups = ng;
}

async function newPage() {
  const page = await browser.newPage();
  await page.goto(standaloneDocUrl());
  return page
}

expect.extend({
  toBeTruthyWithMessage(received, errMsg) {
    const result = {
      pass: received,
      message: () => errMsg
    };
    return result;
  }
});

beforeAll(async () => {
  browser = await puppeteer.launch({})
  await loadNavLinks()
})

async function docLinks(page) {
  const hrefs = await page.evaluate(function() {
    const main = document.body.querySelector('.mainContainer');
    const article = document.querySelector('article')
    // https://github.com/GoogleChrome/puppeteer/issues/2479
    return Array.from(article.querySelectorAll('a[href]'), ({href})  => href.split('#')[0]);
  });
  return new Set(hrefs)
}


async function findNavLinks(page, title) {
  const links = await page.evaluate(function(title) {
    const navGroups = Array.from(document.body.querySelectorAll('.navGroup'));
    for (ng of navGroups) {
      if (title === ng.children[0].innerText) {
        return Array.from(ng.querySelectorAll('a[href]'), ({href})  => href)
      }
    }

    return []
  }, title)

  return links
}

function ok(response) {
  return response.status() === 200 || response.status() === 304
}

function shouldSkipLink(link) {
  if (link.includes('mailto:')) {
    return true
  }
  if (link.includes('/api/')) {
    return true
  }
  if (link.includes('localhost:8001')) {
    return true
  }
  if (link.includes('localhost:8080')) {
    return true
  }
  if (link.includes('localhost:4000')) {
    return true
  }
  if (link.includes('localhost:6650')) {
    return true
  }
  if (link.includes('localhost:3000')) {
    return true
  }
  if (link.includes('.dcos')) {
    return true
  }
  if (link.includes('192.168')) {
    return true
  }
  if (link.includes('org.apache.pulsar:')) {
    return true
  }
  if (link.includes('websockets.')) {
    return true
  }
  if (link.includes('logging.apache.org')) {
    return true
  }
  if (link.includes('pulsar:')) {
    return true
  }
  return false
}

async function testDocLinks(page, links) {
  const results = []

  for (l of links) {
    // console.log("checking doc links for", l)
    await page.goto(l)
    const doclinks = await docLinks(page)
    const result = {
      url: l,
      broken: []
    }

    for (dl of doclinks) {
      if (shouldSkipLink(dl)) {
        continue
      }
      // console.log("  doc link", dl)
      try {
        const response = await axios.get(dl);
      } catch (error) {
        console.log(error);
        result.broken.push(dl);
      }
    }

    if (result.broken.length > 0) {
      results.push(result)
    }
  }
  return results
}

function assertResults(results) {
  let errMsg = ""
  for (r of results) {
    let msg = `${r.url} contains broken links:\n`
    for (bl of r.broken) {
      msg += `\t${bl}\n`
    }
    errMsg += msg
  }

  expect(results.length == 0).toBeTruthyWithMessage(errMsg)
}


test('Getting started', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Getting started')
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


test('Pulsar Functions', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Pulsar Functions')
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


test('Pulsar IO', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Pulsar IO')
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, timeoutMs)



test('Deployment', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Deployment')
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, 180000)



test('Pulsar administration', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Pulsar administration')
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, timeoutMs)


test('Security', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Security')
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, 180000)


test('Client libraries', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Client libraries')
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, timeoutMs)



test('Admin API', async() => {
  const page = await newPage();

  const links = await findNavLinks(page, 'Admin API');
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, timeoutMs)


test('Adaptors', async() => {
  const page = await newPage();

  const links = await findNavLinks(page, 'Adaptors');
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, timeoutMs)


test('Development', async() => {
  const page = await newPage();

  const links = await findNavLinks(page, 'Development');
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, 180000)


test('Reference', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Reference')
  expect(links.length).toBeGreaterThan(0);
  for (l of links) {
    const response = await axios.get(l);
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);
  assertResults(results);

}, timeoutMs)


afterAll(() => {
  if (browser) {
    browser.close()
  }
})
