// IM channel E2E: Feishu and WeCom digital employees against protocol-level
// mock platforms — no real credentials, but the real SDK/websocket paths.
//
//   IM1  unmapped Feishu DM → exactly one rate-limited rejection, no turn
//   IM2  mapped peer DM → resident reply through the platform; dept report
//       scoped by the mapped requester's identity (sibling dept DENIED)
//   IM3  mapped subordinate DM → ephemeral fork spawned, reply arrives via
//       the resident's platform connection; second DM reuses the fork
//   IM4  mapped WeCom DM → stream reply through the AI-bot websocket
//   IM5  deleting the WeCom mapping fails closed once the cache expires
//   IM6  Feishu group: non-@ messages ignored, @ triggers the resident
//
// Prerequisites: AEP gateway stack (compose gateway profile with the mock
// model), the picoclaw binary and mock binaries built. Env: AEP_BASE_URL,
// PICOCCLAW_BIN, IM_PORT (resident port, default 18820).
import {spawn} from 'node:child_process';
import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';

const base = process.env.AEP_BASE_URL ?? 'http://localhost:8080';
const bin = process.env.PICOCCLAW_BIN ?? path.resolve('build/picoclaw-im-e2e');
const mockFeishuBin = process.env.MOCK_FEISHU_BIN ?? path.resolve('build/mockfeishu');
const mockWecomBin = process.env.MOCK_WECOM_BIN ?? path.resolve('build/mockwecom');
const port = Number(process.env.IM_PORT ?? 18820);
const runId = Date.now().toString(36);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const processes = [];
const homes = [];
try {
  const admin = await login('admin', 'change-this-admin-password');
  const feishuMock = await launchMock(mockFeishuBin, ['-http', '127.0.0.1:0', '-ws', '127.0.0.1:0']);
  const wecomMock = await launchMock(mockWecomBin, ['-addr', '127.0.0.1:0']);
  const org = await provision(admin);
  const resident = await launchResident(org, feishuMock, wecomMock);
  await waitHealthy(resident);
  await waitPlatformConnected(feishuMock, 'feishu');
  await waitPlatformConnected(wecomMock, 'wecom');

  const feishuSent = () => platformSent(feishuMock, 'feishu');
  const wecomSent = () => platformSent(wecomMock, 'wecom');

  const forkCount = async () => (await adminGet('/aep/v1/admin/agents?includeEphemeral=true&limit=200'))
    .agents.filter(a => a.ephemeral && a.username.startsWith('eph-')).length;
  const baseline = await forkCount();

  // IM1 — unmapped Feishu DM: one rejection, rate-limited silence after.
  await pushFeishu(feishuMock, 'im1', 'ou_stranger', '你好');
  await pollFor('IM1 rejection', () => feishuSent(), sent =>
    sent.filter(s => s.msg_type === 'text' && s.content.includes('绑定')).length === 1, 60_000);
  await pushFeishu(feishuMock, 'im1', 'ou_stranger', '还在吗');
  await sleep(3000);
  const afterBurst = await feishuSent();
  if (afterBurst.filter(s => s.content.includes('绑定')).length !== 1) {
    throw new Error('IM1: rejection must be rate-limited to one per window');
  }
  console.log('PASS IM1 unmapped sender rejected once, rate-limited, no turn');

  // IM2 — mapped peer: resident answers through the platform connection and
  // the dept-data tool scopes by the mapped requester.
  await pushFeishu(feishuMock, 'im2', 'ou_peer', 'Say hello');
  await pollFor('IM2 peer reply', () => feishuSent(), sent =>
    sent.some(s => s.content.includes('Hello AEP')), 120_000);
  console.log('PASS IM2a mapped peer reaches the resident via Feishu');
  // The peer asks for a team inside the resident's subtree but outside the
  // peer's own scope: the dept-data tool must deny it (the identity mapping,
  // not the bot account, drives the scope).
  await pushFeishu(feishuMock, 'im2b', 'ou_peer', `AEP_DEPT_REPORT AEP_DEPT_REPORT_TEAM:${org.teams.child} please`);
  await pollFor('IM2b scoped denial', () => feishuSent(), sent =>
    sent.some(s => s.content.includes(`DENIED team=${org.teams.child}`)), 120_000);
  console.log('PASS IM2b tools scope by the mapped requester identity');

  // IM3 — mapped subordinate: ephemeral fork, reply via the resident.
  await pushFeishu(feishuMock, 'im3', 'ou_sub', 'Say hello');
  await pollFor('IM3 fork reply', () => feishuSent(), sent =>
    sent.filter(s => s.content.includes('Hello AEP')).length >= 2, 150_000);
  if ((await forkCount()) - baseline !== 1) throw new Error('IM3: exactly one fork expected');
  await pushFeishu(feishuMock, 'im3', 'ou_sub', 'Say hello again');
  await pollFor('IM3 reuse', () => feishuSent(), sent =>
    sent.filter(s => s.content.includes('Hello AEP')).length >= 3, 150_000);
  if ((await forkCount()) - baseline !== 1) throw new Error('IM3: fork must be reused');
  console.log('PASS IM3 subordinate served by a reused ephemeral fork, reply via the resident');

  // IM4 — WeCom peer DM: stream reply through the AI-bot websocket.
  await pushWecom(wecomMock, 'im4', 'wk_peer', 'Say hello');
  await pollFor('IM4 wecom reply', () => wecomSent(), sent =>
    sent.some(s => s.cmd === 'aibot_respond_msg' && JSON.stringify(s.body).includes('Hello AEP')), 120_000);
  console.log('PASS IM4 mapped WeCom peer answered through the bot websocket');

  // IM5 — deleting the WeCom mapping fails closed once the cache expires
  // (identity_cache_seconds is 4; wait past it, then observe fresh sends).
  await adminDelete(admin, `/aep/v1/admin/identity-sources/${org.wecomSource}/mappings/user/wk_peer`);
  await sleep(5000);
  await resetPlatform(wecomMock, 'wecom');
  await pushWecom(wecomMock, 'im5', 'wk_peer', 'Say hello');
  await pollFor('IM5 fail closed', () => wecomSent(), sent =>
    sent.some(s => s.cmd === 'aibot_respond_msg' && JSON.stringify(s.body).includes('绑定')), 90_000);
  console.log('PASS IM5 deleted mapping fails closed after cache expiry');

  // IM6 — Feishu group: no @, no reply; @ triggers the resident.
  await resetPlatform(feishuMock, 'feishu');
  await pushFeishu(feishuMock, 'im6group', 'ou_peer', 'group chatter without mention', 'group', false);
  await sleep(4000);
  if ((await feishuSent()).length !== 0) throw new Error('IM6: non-@ group message must be ignored');
  await pushFeishu(feishuMock, 'im6group', 'ou_peer', 'please help', 'group', true);
  await pollFor('IM6 @ reply', () => feishuSent(), sent =>
    sent.some(s => s.content.includes('Hello AEP')), 120_000);
  console.log('PASS IM6 group gating: non-@ ignored, @ answered');

  console.log('ALL IM E2E SCENARIOS PASSED');
} catch (error) {
  console.error('IM E2E FAILED:', error.message);
  for (const home of homes) {
    try {
      const {execSync} = await import('node:child_process');
      console.error(execSync(`tail -40 ${home}/logs/gateway.log 2>/dev/null || true`, {shell: '/bin/bash', encoding: 'utf8'}));
    } catch {}
  }
  process.exitCode = 1;
} finally {
  for (const proc of processes) proc.kill('SIGTERM');
  await sleep(500);
  for (const home of homes) rmSync(home, {recursive: true, force: true});
}

async function login(username, password) {
  const resp = await fetch(base + '/aep/v1/auth/password/login', {
    method: 'POST',
    headers: {'Content-Type': 'application/json', 'X-AEP-Protocol-Version': '1.0'},
    body: JSON.stringify({deploymentId: 'demo', sessionId: `im-${runId}`, username, password}),
  });
  if (!resp.ok) throw new Error(`login ${username}: ${resp.status} ${await resp.text()}`);
  return resp.json();
}

async function adminPost(admin, path_, body, method = 'POST') {
  const resp = await fetch(base + path_, {
    method,
    headers: {'Content-Type': 'application/json', 'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${admin.accessToken}`},
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (resp.status >= 300 && resp.status !== 409) throw new Error(`provision ${path_}: ${resp.status} ${text}`);
  return text ? JSON.parse(text) : null;
}

async function adminDelete(admin, path_) {
  const resp = await fetch(base + path_, {
    method: 'DELETE',
    headers: {'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${admin.accessToken}`},
  });
  if (resp.status >= 300) throw new Error(`delete ${path_}: ${resp.status} ${await resp.text()}`);
}

async function adminGet(path_) {
  const resp = await fetch(base + path_, {headers: {'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${(await login('admin', 'change-this-admin-password')).accessToken}`}});
  if (!resp.ok) throw new Error(`adminGet ${path_}: ${resp.status}`);
  return resp.json();
}

async function provision(admin) {
  const r = runId;
  const teams = {home: `im-home-${r}`, child: `im-child-${r}`, sibling: `im-sibling-${r}`};
  const passwords = {sub: 'sub-password-123', peer: 'peer-password-123'};
  await adminPost(admin, '/aep/v1/admin/teams', {id: teams.home, name: `IM Home ${r}`});
  await adminPost(admin, '/aep/v1/admin/teams', {id: teams.child, name: `IM Child ${r}`, parentId: teams.home});
  await adminPost(admin, '/aep/v1/admin/teams', {id: teams.sibling, name: `IM Sibling ${r}`});
  const runnerRole = `im-runner-${r}`;
  await adminPost(admin, '/aep/v1/admin/roles', {
    id: runnerRole, name: `IM Runner ${r}`,
    permissions: ['models.read', 'skills.read', 'data_scope.read', 'identity.read'],
  });
  const employeeRole = `im-employee-${r}`;
  await adminPost(admin, '/aep/v1/admin/roles', {
    id: employeeRole, name: `IM Employee ${r}`, permissions: ['models.read', 'skills.read'],
  });

  const sub = await adminPost(admin, '/aep/v1/admin/users', {
    deploymentId: 'demo', username: `im-sub-${r}`, displayName: 'Wen Xiaoxia (level-2)',
    temporaryPassword: passwords.sub, requirePasswordChange: false,
    teamIds: [teams.child], roleIds: [employeeRole],
  });
  const peer = await adminPost(admin, '/aep/v1/admin/users', {
    deploymentId: 'demo', username: `im-peer-${r}`, displayName: 'Qi Ping (sibling lead)',
    temporaryPassword: passwords.peer, requirePasswordChange: false,
    teamIds: [teams.sibling], roleIds: [employeeRole],
  });

  // Identity sources bind platform-native IDs to platform users.
  const feishuSource = `im-feishu-${r}`;
  const wecomSource = `im-wecom-${r}`;
  await adminPost(admin, '/aep/v1/admin/identity-sources', {
    id: feishuSource, kind: 'directory', displayName: `IM Feishu ${r}`,
    config: {vendor: 'feishu'},
  });
  await adminPost(admin, `/aep/v1/admin/identity-sources/${feishuSource}/mappings`, {
    externalSubjectType: 'user', externalId: 'ou_sub', localSubjectId: sub.id,
  }, 'PUT');
  await adminPost(admin, `/aep/v1/admin/identity-sources/${feishuSource}/mappings`, {
    externalSubjectType: 'user', externalId: 'ou_peer', localSubjectId: peer.id,
  }, 'PUT');
  await adminPost(admin, '/aep/v1/admin/identity-sources', {
    id: wecomSource, kind: 'directory', displayName: `IM WeCom ${r}`,
    config: {vendor: 'wecom'},
  });
  await adminPost(admin, `/aep/v1/admin/identity-sources/${wecomSource}/mappings`, {
    externalSubjectType: 'user', externalId: 'wk_peer', localSubjectId: peer.id,
  }, 'PUT');

  const residentAccount = await adminPost(admin, '/aep/v1/admin/agents', {
    username: `im-resident-${r}`, displayName: `IM Resident ${r}`, password: 'resident-password-123',
    roleIds: [runnerRole], teamIds: [], homeTeamId: teams.home, displayTitle: 'Department assistant',
  });

  const credential = await adminPost(admin, '/aep/v1/admin/credentials', {
    name: `IM provider ${r}`, service: 'mock-openai', type: 'api_key',
    deliveryMode: 'server_only', value: 'm1-e2e-provider-secret', enabled: true,
  });
  await adminPost(admin, '/aep/v1/admin/models', {
    id: 'enterprise-chat', displayName: `IM Chat ${r}`, sourceType: 'gateway',
    protocol: 'openai-compatible', endpoint: 'http://mock-openai.aep.internal:8080/v1',
    upstreamModel: 'mock-upstream-chat', credentialId: credential.id,
    capabilities: ['text'], contextWindow: 8192, isDefault: true, enabled: true,
  });
  await adminPost(admin, '/aep/v1/admin/model-assignments', {
    modelId: 'enterprise-chat', subject: {type: 'user', id: residentAccount.id},
  });

  return {
    teams, feishuSource, wecomSource, runnerRole,
    sub: {id: sub.id}, peer: {id: peer.id},
    resident: {username: `im-resident-${r}`, id: residentAccount.id},
  };
}

async function launchMock(binPath, args) {
  const proc = spawn(binPath, args, {stdio: ['ignore', 'pipe', 'pipe']});
  proc.stderr.on('data', () => {});
  processes.push(proc);
  const ports = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${binPath} did not report ports`)), 15_000);
    let buffer = '';
    proc.stdout.on('data', chunk => {
      buffer += chunk;
      try {
        const parsed = JSON.parse(buffer);
        clearTimeout(timer);
        resolve(parsed);
      } catch {}
    });
  });
  return {proc, ...ports};
}

async function launchResident(org, feishuMock, wecomMock) {
  const home = mkdtempSync(path.join(tmpdir(), 'im-resident-'));
  homes.push(home);
  writeFileSync(path.join(home, 'config.json'), JSON.stringify({
    version: 3,
    aep: {
      enabled: true, base_url: base, deployment_id: 'demo',
      username: org.resident.username, session_id: `im-resident-${runId}`,
      home_team_id: org.teams.home,
      supervisor_username: 'admin', supervisor_password: 'change-this-admin-password',
    },
    channel_list: {
      aepchat: {enabled: true, type: 'aepchat'},
      feishu: {enabled: true, type: 'feishu', group_trigger: {mention_only: true}, settings: {
        app_id: 'cli_mock', app_secret: 'mock-secret',
        domain: `http://127.0.0.1:${feishuMock.http}`,
        aep: {
          identity_source_id: org.feishuSource, identity_cache_seconds: 4,
          warden: {runtime_role_id: org.runnerRole, ttl_minutes: 20, port_range_start: 18920},
        },
      }},
      wecom: {enabled: true, type: 'wecom', settings: {
        bot_id: 'mockbot', secret: 'mock-secret',
        websocket_url: `ws://127.0.0.1:${wecomMock.ws}`,
        aep: {identity_source_id: org.wecomSource, identity_cache_seconds: 4},
      }},
    },
    model_list: [],
    agents: {defaults: {
      workspace: path.join(home, 'workspace'), restrict_to_workspace: true,
      max_tokens: 1024, max_tool_iterations: 6, context_window: 16384,
    }},
    gateway: {host: '127.0.0.1', port},
  }, null, 2));
  const proc = spawn(bin, ['gateway'], {
    env: {...process.env, PICOCLAW_HOME: home, PICOCLAW_AEP_PASSWORD: 'resident-password-123'},
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  proc.stdout.on('data', () => {});
  proc.stderr.on('data', () => {});
  processes.push(proc);
  return {port, url: `http://127.0.0.1:${port}`, home};
}

async function waitHealthy(agent) {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    try {
      const resp = await fetch(agent.url + '/aepchat/v1/health');
      if (resp.ok) return;
    } catch {}
    await sleep(1000);
  }
  throw new Error(`resident on :${agent.port} did not become healthy`);
}

async function waitPlatformConnected(mock, kind) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      if (kind === 'feishu') {
        const resp = await fetch(`http://127.0.0.1:${mock.http}/_test/sent`);
        if (resp.ok) return; // HTTP up implies the ws may not be; push will retry
      } else {
        const resp = await fetch(`http://127.0.0.1:${mock.ws}/_test/sent`);
        if (resp.ok) return;
      }
    } catch {}
    await sleep(500);
  }
  throw new Error(`${kind} mock did not come up`);
}

async function pushFeishu(mock, chatID, senderOpenID, text, chatType = 'p2p', mentionBot = false) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const resp = await fetch(`http://127.0.0.1:${mock.http}/_test/push`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({chatID, senderOpenID, text, chatType, mentionBot}),
    });
    if (resp.ok) return;
    if (resp.status !== 503) throw new Error(`feishu push: ${resp.status} ${await resp.text()}`);
    await sleep(1000); // ws client not connected yet
  }
  throw new Error(`feishu push never succeeded for ${chatID}`);
}

async function pushWecom(mock, chatID, userID, text, chatType = 'single') {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const resp = await fetch(`http://127.0.0.1:${mock.ws}/_test/push`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({chatID, userID, text, chatType}),
    });
    if (resp.ok) return;
    if (resp.status !== 503) throw new Error(`wecom push: ${resp.status} ${await resp.text()}`);
    await sleep(1000);
  }
  throw new Error(`wecom push never succeeded for ${chatID}`);
}

async function platformSent(mock, kind) {
  const url = kind === 'feishu' ? `http://127.0.0.1:${mock.http}/_test/sent` : `http://127.0.0.1:${mock.ws}/_test/sent`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`${kind} sent: ${resp.status}`);
  const page = await resp.json();
  return page.sent ?? [];
}

async function resetPlatform(mock, kind) {
  const url = kind === 'feishu' ? `http://127.0.0.1:${mock.http}/_test/reset` : `http://127.0.0.1:${mock.ws}/_test/reset`;
  await fetch(url, {method: 'POST'});
}

async function pollFor(label, probe, predicate, timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs;
  let last = '(none)';
  while (Date.now() < deadline) {
    const value = await probe();
    last = JSON.stringify(value).slice(0, 400);
    if (predicate(value)) return value;
    await sleep(1500);
  }
  throw new Error(`${label}: condition not met; last: ${last}`);
}
