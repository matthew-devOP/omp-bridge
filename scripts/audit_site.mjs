#!/usr/bin/env node
// Dependency-free CDP audit. Uses an isolated Chrome profile; never a user's browser.
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const url = new URL(process.argv[2] || 'http://127.0.0.1:4321');
if (url.protocol !== 'http:' || !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) {
  throw new Error('Audit target must be a local HTTP server');
}
const output = resolve(process.argv[3] || 'evidence/v2/browser');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const clean = value => JSON.parse(JSON.stringify(value).replace(/\/Users\/[^/\s"\\]+/g, '/Users/<redacted>'));
const report = { schema: 1, auditedAt: new Date().toISOString(), target: url.origin,
  browser: 'Chrome headless, isolated temporary profile, default sandbox', errors: [], checks: {}, screenshots: [],
  limitations: ['DOM click dispatch exercises handlers; not a full physical-pointer or accessibility audit.', 'Local static deployment only; no claim about a published website.'] };
let profile, chrome, ws, cdp, nextId = 1;
const pending = new Map();
async function evaluate(expression) {
  const result = await cdp('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true, userGesture: true });
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.text);
  return result.result.value;
}
async function screenshot(name) {
  const result = await cdp('Page.captureScreenshot', { format: 'png', captureBeyondViewport: false });
  await writeFile(join(output, name), Buffer.from(result.data, 'base64'));
  report.screenshots.push(name);
}
async function navigate() {
  const loaded = new Promise((resolve, reject) => {
    const listener = event => {
      if (JSON.parse(event.data).method === 'Page.loadEventFired') {
        clearTimeout(timer); ws.removeEventListener('message', listener); resolve();
      }
    };
    const timer = setTimeout(() => { ws.removeEventListener('message', listener); reject(new Error('Page load event timed out')); }, 15000);
    ws.addEventListener('message', listener);
  });
  await cdp('Page.navigate', { url: url.href });
  await loaded;
  for (let i = 0; i < 100; i++) {
    if (await evaluate('document.readyState === "complete" && location.href !== "about:blank"')) break;
    if (i === 99) throw new Error('Page load timed out');
    await sleep(100);
  }
  await sleep(600);
}
const layout = () => evaluate(`({width:innerWidth,height:innerHeight,documentWidth:document.documentElement.scrollWidth,
 horizontalOverflow:document.documentElement.scrollWidth>innerWidth,title:document.title,
 brokenImages:[...document.images].filter(i=>i.complete&&!i.naturalWidth).map(i=>new URL(i.src).pathname)})`);
try {
  await mkdir(output, { recursive: true });
  const available = await fetch(url, { signal: AbortSignal.timeout(5000), redirect: 'error' });
  if (!available.ok) throw new Error(`Static server returned HTTP ${available.status}`);
  profile = await mkdtemp(join(tmpdir(), 'omp-site-audit-'));
  chrome = spawn(process.env.CHROME_BIN || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', [
    '--headless=new', '--remote-debugging-port=0', `--user-data-dir=${profile}`,
    '--no-first-run', '--no-default-browser-check', '--disable-background-networking', '--disable-sync', 'about:blank'
  ], { stdio: ['ignore', 'ignore', 'ignore'] });
  let launchError;
  chrome.on('error', error => { launchError = error; });
  let port;
  for (let i = 0; i < 100; i++) {
    if (launchError) throw launchError;
    try { port = (await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]; break; } catch {}
    if (chrome.exitCode !== null) throw new Error('Chrome exited before CDP became ready');
    await sleep(100);
  }
  if (!port) throw new Error('Chrome debugging endpoint timed out');
  const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  ws = new WebSocket(targets.find(t => t.type === 'page').webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { ws.addEventListener('open', resolve, {once:true}); ws.addEventListener('error', reject, {once:true}); });
  ws.addEventListener('message', event => {
    const message = JSON.parse(event.data);
    if (message.id) {
      const item = pending.get(message.id);
      if (!item) return;
      pending.delete(message.id); clearTimeout(item.timer);
      if (message.error) item.reject(new Error(message.error.message)); else item.resolve(message.result);
    } else if (message.method === 'Runtime.exceptionThrown') {
      report.errors.push({ kind: 'exception', details: message.params.exceptionDetails });
    } else if (message.method === 'Runtime.consoleAPICalled' && message.params.type === 'error') {
      report.errors.push({ kind: 'console.error', values: message.params.args.map(a => a.value ?? a.description) });
    } else if (message.method === 'Log.entryAdded' && message.params.entry.level === 'error') {
      report.errors.push({ kind: 'log.error', entry: message.params.entry });
    }
  });
  cdp = (method, params = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`CDP timeout: ${method}`)); }, 15000);
    pending.set(id, { resolve, reject, timer }); ws.send(JSON.stringify({id,method,params}));
  });
  await cdp('Page.enable'); await cdp('Runtime.enable'); await cdp('Log.enable');
  await cdp('Emulation.setDeviceMetricsOverride', { width:1920,height:1080,deviceScaleFactor:1,mobile:false });
  await navigate();
  report.checks.desktop = await layout();
  await screenshot('desktop-1920x1080.png');
  report.checks.tabs = await evaluate(`(() => {
    const buttons=[...document.querySelectorAll('[role="tab"]')];
    return buttons.map(button=>{ button.click(); const panel=document.getElementById(button.getAttribute('aria-controls'));
      return {name:button.dataset.name||button.textContent.trim(),selected:button.getAttribute('aria-selected'),
        panelHeading:panel?.querySelector('h3')?.textContent||null,
        headingMatches:!!panel&&panel.textContent.includes(button.dataset.name||button.textContent.trim()),
        selectedCount:document.querySelectorAll('[role="tab"][aria-selected="true"]').length}; });
  })()`);
  report.checks.replay = await evaluate(`(async () => {
    const button=document.querySelector('#replayBtn'); if(!button)return {present:false};
    document.querySelector('#replay')?.scrollIntoView(); await new Promise(r=>setTimeout(r,150)); button.click();
    const immediate=document.querySelectorAll('.term-body .ln.on').length;
    const lines=[...document.querySelectorAll('.term-body .ln')];
    const maxDelay=Math.max(0,...lines.map(e=>Number(e.dataset.t)||0));
    const waitMs=Math.min(maxDelay+500,10000); await new Promise(r=>setTimeout(r,waitMs));
    return {present:true,totalLines:lines.length,immediateVisible:immediate,
      visibleAfterWait:document.querySelectorAll('.term-body .ln.on').length,waitMs,maxDelay,
      completedWithinAuditWindow:maxDelay+350<=waitMs};
  })()`);
  await screenshot('desktop-replay.png');
  try {
    await cdp('Browser.grantPermissions', { origin:url.origin,permissions:['clipboardReadWrite','clipboardSanitizedWrite'] });
    report.checks.copy = await evaluate(`(async () => {
      const results=[];for(const b of document.querySelectorAll('[data-copy]')) {
        b.click(); await new Promise(r=>setTimeout(r,80));
        const actual=await navigator.clipboard.readText();
        const expected=document.getElementById('snip-'+b.dataset.copy)?.textContent;
        results.push({name:b.dataset.copy,label:b.textContent,clipboardMatches:actual===expected,characters:actual.length});
      }return results;
    })()`);
  } catch(error) { report.checks.copy = {verified:false,error:error.message}; }
  await cdp('Emulation.setDeviceMetricsOverride', {width:390,height:844,deviceScaleFactor:1,mobile:true});
  await evaluate('scrollTo(0,0)'); await sleep(250);
  report.checks.mobile = await layout(); await screenshot('mobile-390x844.png');
  await cdp('Emulation.setEmulatedMedia', {features:[{name:'prefers-reduced-motion',value:'reduce'}]});
  await navigate();
  report.checks.reducedMotion = await evaluate(`({matches:matchMedia('(prefers-reduced-motion: reduce)').matches,
    videos:[...document.querySelectorAll('video')].map(v=>({paused:v.paused,autoplay:v.autoplay,controls:v.controls})),
    smil:[...document.querySelectorAll('svg')].filter(s=>s.querySelector('animate,animateMotion,animateTransform')).map(s=>({paused:s.animationsPaused()}))})`);
  const c = report.checks;
  report.passed = report.errors.length===0 && !c.desktop.horizontalOverflow && !c.mobile.horizontalOverflow
    && !c.desktop.brokenImages.length && !c.mobile.brokenImages.length
    && c.tabs.length>0 && c.tabs.every(t=>t.selected==='true'&&t.headingMatches&&t.selectedCount===1)
    && c.replay.present && c.replay.totalLines>0 && c.replay.immediateVisible===0 && c.replay.visibleAfterWait>0
    && Array.isArray(c.copy) && c.copy.length>0 && c.copy.every(x=>x.clipboardMatches)
    && c.reducedMotion.matches && c.reducedMotion.videos.every(v=>v.paused&&!v.autoplay)
    && c.reducedMotion.smil.every(s=>s.paused);
} catch (error) { report.passed=false; report.failure=error.message; }
finally {
  if (ws) ws.close();
  for (const item of pending.values()) { clearTimeout(item.timer); item.reject(new Error('Audit closing')); }
  if (chrome && chrome.exitCode===null) {
    chrome.kill('SIGTERM');
    for(let i=0;i<30&&chrome.exitCode===null;i++)await sleep(100);
    if(chrome.exitCode===null) { chrome.kill('SIGKILL'); await sleep(300); }
  }
  if(profile)await rm(profile,{recursive:true,force:true,maxRetries:3,retryDelay:200});
  await mkdir(output,{recursive:true});
  await writeFile(join(output,'audit.json'),JSON.stringify(clean(report),null,2)+'\n');
}
console.log(JSON.stringify({passed:report.passed,screenshots:report.screenshots,errors:report.errors.length,failure:report.failure}));
if(!report.passed)process.exitCode=1;
