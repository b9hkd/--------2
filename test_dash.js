const { chromium } = require('playwright-chromium');
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  page.on('console', msg => console.log('console:', msg.type(), msg.text()));
  page.on('pageerror', err => console.error('pageerror:', err));
  await page.goto('http://localhost:8001/dashboard.html', { waitUntil: 'networkidle' });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: 'dash.png', fullPage: true });
  const html = await page.content();
  console.log('len html', html.length);
  await browser.close();
})();
