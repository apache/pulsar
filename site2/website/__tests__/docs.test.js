const puppeteer = require('puppeteer')
const axios = require('axios');


const CWD = process.cwd();

const timeoutMs = 60000;

function startSite() {
  //shell.exec('yarn build');
}

let browser;
let navLinks = [];
let navGroups = [];

async function loadNavLinks() {
  let page = await browser.newPage();
  await page.goto('http://localhost:3000/docs/standalone');
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
  //await blockImages(page)
  await page.goto('http://localhost:3000/docs/standalone');

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
  startSite()
  console.log("BEFORE ALL")
  console.log("CWD:", CWD)
  browser = await puppeteer.launch({})
  await loadNavLinks()
})



test('adds 1 + 2 to equal 3', () => {
  expect(3).toBe(3);
});

/*
test('check nav links', async () => {
  const page = await browser.newPage();
  for (let i = 0; i < navLinks.length; i++) {
    const link = navLinks[i];
    const response = await page.goto(link);
    //await page.waitForNavigation({ waitUntil: 'networkidle2' });
    //console.log(link, response.status())
    console.log("checking link:", link)
    expect(response.status()).toBe(200)
  }
  //for (link of navLinks) {
  //  const response = await page.goto(link);
  //  console.log(link, response.status())
  //}
}, 10000)
*/

/*async function blockImages(page) {
  await page.setRequestInterception(true);
  page.on("request", (request) => {
    if (request.resourceType === "image") {
      request.abort();
    } else {
      request.continue();
    }
  });
}
*/

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
    console.log("checking for title", title)
    const navGroups = Array.from(document.body.querySelectorAll('.navGroup'));
    for (ng of navGroups) {
      console.log(ng)
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
    console.log("checking doc links for", l)
    await page.goto(l)
    const doclinks = await docLinks(page)
    //console.log(doclinks)
    //console.log(doclinks)

    const result = {
      url: l,
      broken: []
    }

    for (dl of doclinks) {
      if (shouldSkipLink(dl)) {
        continue
      }
      console.log("  doc link", dl)
      try {
        const response = await axios.get(dl);
        //expect(response.status).toBe(200)
        console.log(response.status);
      } catch (error) {
        console.log(error);
        result.broken.push(dl)
      } 
      //const response = await page.goto(dl);
      //const errMsg = `${l} contains a broken link [${dl}]`;
      //if (response.status != 200) {
      //  result.broken.push(dl)
      //}
      //expect(ok(response)).toBeTruthyWithMessage(errMsg)
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
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


test('Pulsar Functions', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Pulsar Functions')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


test('Pulsar IO', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Pulsar IO')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


/*
test('Deployment', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Deployment')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, 180000)
*/


test('Pulsar administration', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Pulsar administration')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


test('Security', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Security')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, 180000)


test('Client libraries', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Client libraries')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)



test('Admin API', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Admin API')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


test('Adaptors', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Adaptors')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


test('Development', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Development')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, 180000)


test('Reference', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Reference')
  for (l of links) {
    const response = await axios.get(l);
    //expect(ok(response)).toBeTruthy();
    expect(response.status).toBe(200);
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)
}, timeoutMs)


/*
test('Getting started', async() => {
  //let page = await browser.newPage();
  //await blockImages(page)
  //await page.goto('http://localhost:3000/docs/standalone');

  const page = await newPage()

  const links = await findNavLinks(page, 'Getting started')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }

  


  let results = []

  for (l of links) {
    console.log("checking doc links for", l)
    await page.goto(l)
    const doclinks = await docLinks(page)
    //console.log(doclinks)
    //console.log(doclinks)

    const result = {
      url: l,
      broken: []
    }

    for (dl of doclinks) {
      if (dl === l) continue
      //console.log(dl)
      const response = await page.goto(dl);
      const errMsg = `${l} contains a broken link [${dl}]`;
      if (!ok(response)) {
        result.broken.push(dl)
      }
      //expect(ok(response)).toBeTruthyWithMessage(errMsg)
    }

    if (result.broken.length > 0) {
      results.push(result)
    }

  }

  let errMsg = ""
  for (r of results) {
    let msg = `${r.url} contains broken links:\n`
    for (bl of r.broken) {
      msg += `\t${bl}\n`
    }
    errMsg += msg
  }

  expect(results.length == 0).toBeTruthyWithMessage(errMsg)

  //console.log(results)


}, 120000)
*/

/*
test('Pulsar Functions', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Pulsar Functions')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }  

  const results = await testDocLinks(page, links);

  let errMsg = ""
  for (r of results) {
    let msg = `${r.url} contains broken links:\n`
    for (bl of r.broken) {
      msg += `\t${bl}\n`
    }
    errMsg += msg
  }

  expect(results.length == 0).toBeTruthyWithMessage(errMsg)


}, 120000)
*/

/*
test('Deployment', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Deployment')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)

}, 180000)
*/

/*
test('Pulsar administration', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Pulsar administration')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)

}, 180000)
*/

/*
test('Client libraries', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Client libraries')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)

}, 180000)
*/

/*
test('Admin API', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Admin API')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }

  const results = await testDocLinks(page, links);

  assertResults(results)

}, 180000)
*/

/*
test('Adaptors', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Adaptors')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)

}, 180000)
*/

/*
test('Cookbooks', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Cookbooks')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)

}, 180000)
*/

/*
test('Development', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Development')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }  

  const results = await testDocLinks(page, links);

  assertResults(results)

}, 180000)
*/

/*
test('Reference', async() => {
  const page = await newPage()

  const links = await findNavLinks(page, 'Reference')
  for (l of links) {
    const response = await page.goto(l);
    expect(response.status()).toBe(200)
  }

  const results = await testDocLinks(page, links);

  assertResults(results)

}, 180000)
*/

/*
test('links', async () => {
  let page = await browser.newPage();
  const response = await page.goto('http://localhost:3000/docs/standalone-404');
  console.log("response status");
  console.log(response.status());
  //await page.toClick('a', {text: 'Read the docs'})
  //await browser.close();

  //const hrefs = await page.evaluate(
  //  () => Array.from(document.body.querySelectorAll('a.navItem[href]'), ({ href }) => href)
  //);
  console.log(navLinks)
})
*/

afterAll(() => {
  if (browser) {
    browser.close()
  }
})
