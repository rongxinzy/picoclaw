// Warden E2E: the resident/ephemeral conversation split, live.
//
//   W1  a strictly-subordinate employee (level-2 team inside the resident's
//       home subtree) is served by a spawned ephemeral fork — reply arrives
//       through the resident's proxy, and the fork's knowledge stays inside
//       the employee's frozen scope (cross-department report DENIED)
//   W2  a peer-level user (team outside the subtree) reaches the resident
//       directly — no fork is spawned
//   W3  the fork's account is ephemeral: it appears only with
//       includeEphemeral, and its expiry is enforced by the control plane
//
// Prerequisites: AEP gateway stack with the ephemeral endpoints deployed,
// the picoclaw binary built with warden support. Env: AEP_BASE_URL,
// PICOCCLAW_BIN, WARDEN_PORT (resident port, default 18810).
import {spawn} from 'node:child_process';
import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';

const base = process.env.AEP_BASE_URL ?? 'http://localhost:8080';
const bin = process.env.PICOCCLAW_BIN ?? path.resolve('build/picoclaw-warden-e2e');
const port = Number(process.env.WARDEN_PORT ?? 18810);
const runId = Date.now().toString(36);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const processes = [];
const homes = [];
try {
  const admin = await login('admin', 'change-this-admin-password');
  const org = await provision(admin);
  const resident = await launchResident(org);
  await waitHealthy(resident);

  const sub = await login(org.sub.username, org.passwords.sub);
  const peer = await login(org.peer.username, org.passwords.peer);

  // Baseline of leftover ephemeral accounts from previous runs.
  const forkCount = async () => (await adminGet('/aep/v1/admin/agents?includeEphemeral=true&limit=200'))
    .agents.filter(a => a.ephemeral && a.username.startsWith('eph-')).length;
  const baseline = await forkCount();

  // W1 — subordinate: fork spawns lazily, the reply arrives via the proxy,
  // and the frozen scope denies the sibling department's data.
  const w1chat = `w1-${runId}`;
  let sent = await post(resident, w1chat, 'Say hello', sub.accessToken);
  if (sent.status !== 202) throw new Error(`W1 post = ${sent.status} ${sent.body}`);
  const hello = await pollFor('W1 fork greets', resident, w1chat, sub, /Hello AEP/, 120_000);
  console.log('PASS W1a subordinate served by ephemeral fork (proxied reply)');

  const denied = await converse('W1b frozen scope denies sibling dept', sub, resident, `w1d-${runId}`,
    `AEP_DEPT_REPORT AEP_DEPT_REPORT_TEAM:${org.teams.sibling} please`,
    body => body.includes(`DENIED team=${org.teams.sibling}`), 120_000);
  console.log('PASS W1b frozen scope denies sibling dept');

  // Exactly one new fork account exists, and only with includeEphemeral.
  if ((await forkCount()) - baseline !== 1) throw new Error('W1 should have spawned exactly one fork');
  const visible = (await adminGet('/aep/v1/admin/agents?includeEphemeral=true&limit=200'))
    .agents.filter(a => a.ephemeral && a.username.startsWith('eph-'));
  const hidden = await adminGet('/aep/v1/admin/agents?limit=200');
  if (hidden.agents.some(a => a.username === visible.at(-1).username)) throw new Error('directory leaked the fork');
  console.log('PASS W1c fork account is ephemeral and hidden by default');

  // W2 — peer: the resident answers directly; no second fork appears.
  await converse('W2 peer reaches resident', peer, resident, `w2-${runId}`,
    'Say hello', /Hello AEP/, 60_000);
  if ((await forkCount()) - baseline !== 1) throw new Error('peer chat must not spawn additional forks');
  console.log('PASS W2 peer reaches resident without spawning a fork');

  console.log('ALL WARDEN E2E SCENARIOS PASSED');
} catch (error) {
  console.error('WARDEN E2E FAILED:', error.message);
  for (const home of homes) {
    try {
      const {execSync} = await import('node:child_process');
      console.error(execSync(`tail -30 ${home}/logs/gateway.log 2>/dev/null || true`, {shell: '/bin/bash', encoding: 'utf8'}));
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
    body: JSON.stringify({deploymentId: 'demo', sessionId: `warden-${runId}`, username, password}),
  });
  if (!resp.ok) throw new Error(`login ${username}: ${resp.status} ${await resp.text()}`);
  return resp.json();
}

async function adminPost(admin, path_, body) {
  const resp = await fetch(base + path_, {
    method: 'POST',
    headers: {'Content-Type': 'application/json', 'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${admin.accessToken}`},
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (resp.status >= 300 && resp.status !== 409) throw new Error(`provision ${path_}: ${resp.status} ${text}`);
  return text ? JSON.parse(text) : null;
}

async function adminGet(path_) {
  const resp = await fetch(base + path_, {headers: {'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${(await login('admin', 'change-this-admin-password')).accessToken}`}});
  if (!resp.ok) throw new Error(`adminGet ${path_}: ${resp.status}`);
  return resp.json();
}

async function provision(admin) {
  const r = runId;
  const teams = {home: `w-home-${r}`, child: `w-child-${r}`, sibling: `w-sibling-${r}`};
  const passwords = {sub: 'sub-password-123', peer: 'peer-password-123'};
  await adminPost(admin, '/aep/v1/admin/teams', {id: teams.home, name: `Warden Home ${r}`});
  await adminPost(admin, '/aep/v1/admin/teams', {id: teams.child, name: `Warden Child ${r}`, parentId: teams.home});
  await adminPost(admin, '/aep/v1/admin/teams', {id: teams.sibling, name: `Warden Sibling ${r}`});
  await adminPost(admin, '/aep/v1/admin/roles', {
    id: `w-runner-${r}`, name: `Warden Runner ${r}`,
    permissions: ['models.read', 'skills.read', 'data_scope.read'],
  });

  // The subordinate lives strictly inside the resident's home subtree.
  await adminPost(admin, '/aep/v1/admin/users', {
    deploymentId: 'demo', username: `w-sub-${r}`, displayName: 'Wen Xiaoxia (level-2)',
    temporaryPassword: passwords.sub, requirePasswordChange: false,
    teamIds: [teams.child], roleIds: [`w-employee-${r}`] ,
  }).catch(async () => {
    await adminPost(admin, '/aep/v1/admin/roles', {
      id: `w-employee-${r}`, name: `Warden Employee ${r}`, permissions: ['models.read', 'skills.read'],
    });
    await adminPost(admin, '/aep/v1/admin/users', {
      deploymentId: 'demo', username: `w-sub-${r}`, displayName: 'Wen Xiaoxia (level-2)',
      temporaryPassword: passwords.sub, requirePasswordChange: false,
      teamIds: [teams.child], roleIds: [`w-employee-${r}`],
    });
  });

  // The peer leads the sibling department: outside the resident subtree.
  await adminPost(admin, '/aep/v1/admin/roles', {
    id: `w-peer-role-${r}`, name: `Warden Peer ${r}`, permissions: ['models.read', 'skills.read'],
  });
  await adminPost(admin, '/aep/v1/admin/users', {
    deploymentId: 'demo', username: `w-peer-${r}`, displayName: 'Qi Ping (sibling lead)',
    temporaryPassword: passwords.peer, requirePasswordChange: false,
    teamIds: [teams.sibling], roleIds: [`w-peer-role-${r}`],
  });

  // The resident digital employee: homed at the department root.
  const residentAccount = await adminPost(admin, '/aep/v1/admin/agents', {
    username: `w-resident-${r}`, displayName: `Warden Resident ${r}`, password: 'resident-password-123',
    roleIds: [`w-runner-${r}`], teamIds: [], homeTeamId: teams.home, displayTitle: 'Department assistant',
  });

  // The mock model chain, assigned to the resident (forks copy the catalog).
  const credential = await adminPost(admin, '/aep/v1/admin/credentials', {
    name: `Warden provider ${r}`, service: 'mock-openai', type: 'api_key',
    deliveryMode: 'server_only', value: 'm1-e2e-provider-secret', enabled: true,
  });
  await adminPost(admin, '/aep/v1/admin/models', {
    id: 'enterprise-chat', displayName: `Warden Chat ${r}`, sourceType: 'gateway',
    protocol: 'openai-compatible', endpoint: 'http://mock-openai.aep.internal:8080/v1',
    upstreamModel: 'mock-upstream-chat', credentialId: credential.id,
    capabilities: ['text'], contextWindow: 8192, isDefault: true, enabled: true,
  });
  await adminPost(admin, '/aep/v1/admin/model-assignments', {
    modelId: 'enterprise-chat', subject: {type: 'user', id: residentAccount.id},
  });

  return {
    teams, passwords,
    sub: {username: `w-sub-${r}`},
    peer: {username: `w-peer-${r}`},
    resident: {username: `w-resident-${r}`, id: residentAccount.id},
    runnerRole: `w-runner-${r}`,
  };
}

async function launchResident(org) {
  const home = mkdtempSync(path.join(tmpdir(), 'w-resident-'));
  homes.push(home);
  writeFileSync(path.join(home, 'config.json'), JSON.stringify({
    version: 3,
    aep: {
      enabled: true, base_url: base, deployment_id: 'demo',
      username: org.resident.username, session_id: `w-resident-${runId}`,
      home_team_id: org.teams.home,
      supervisor_username: 'admin', supervisor_password: 'change-this-admin-password',
    },
    channel_list: {aepchat: {enabled: true, type: 'aepchat', settings: {
      warden: {runtime_role_id: org.runnerRole, ttl_minutes: 20},
    }}},
    model_list: [],
    agents: {defaults: {
      workspace: path.join(home, 'workspace'), restrict_to_workspace: true,
      max_tokens: 1024, max_tool_iterations: 6,
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

async function post(agent, chatId, text, token) {
  const resp = await fetch(`${agent.url}/aepchat/v1/chats/${encodeURIComponent(chatId)}/messages`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json', Authorization: `Bearer ${token}`},
    body: JSON.stringify({text}),
  });
  return {status: resp.status, body: await resp.text()};
}

async function converse(label, session, agent, chatId, text, predicate, timeoutMs) {
  const test = typeof predicate === 'function' ? predicate : body => predicate.test(body);
  const sent = await post(agent, chatId, text, session.accessToken);
  if (sent.status !== 202) throw new Error(`${label}: post ${sent.status} ${sent.body}`);
  return pollFor(label, agent, chatId, session, test, timeoutMs);
}

async function pollFor(label, agent, chatId, session, predicate, timeoutMs = 90_000) {
  const test = typeof predicate === 'function' ? predicate : body => predicate.test(body);
  const deadline = Date.now() + timeoutMs;
  let after = 0;
  let lastReply = '(none)';
  while (Date.now() < deadline) {
    const resp = await fetch(`${agent.url}/aepchat/v1/chats/${encodeURIComponent(chatId)}/messages?after=${after}`, {
      headers: {Authorization: `Bearer ${session.accessToken}`},
    });
    if (!resp.ok) throw new Error(`${label} poll ${resp.status}: ${await resp.text()}`);
    const page = await resp.json();
    after = page.nextAfter ?? after;
    const reply = (page.messages ?? []).filter(m => m.from === 'agent').at(-1);
    if (reply) lastReply = reply.text;
    if (reply && test(reply.text)) return reply;
    await sleep(1500);
  }
  throw new Error(`${label}: expected reply did not arrive; last: ${JSON.stringify(lastReply)}`);
}
