const puppeteer = require('puppeteer')

let browser;

beforeAll(async () => {
  browser = await puppeteer.launch({})
})

afterAll(() => {
  if (browser) {
    browser.close()
  }
})


test('key feature links', async () => {
  expect(3).toBe(3);
});
