// WeKnora knowledge E2E: the retrieval PEP against a live knowledge platform.
//
//   K1  an RD employee asks the digital employee for the release process →
//       the RD passage is cited, and HR material never appears
//   K2  the same RD employee probes salary knowledge → only the RD KB is
//       searched; no HR passage can leak
//   K3  a cross-department lead with management scope over both departments
//       → both KBs are searched, the HR passage is returned
//
// Prerequisites: AEP stack (gateway profile), the WeKnora deployment with
// kb-rd/kb-hr bootstrapped (WEKNORA_KB_RD / WEKNORA_KB_HR), and the scoped
// API key (WEKNORA_API_KEY).
import {spawn} from 'node:child_process';
import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';

const base = process.env.AEP_BASE_URL ?? 'http://localhost:8080';
const weknoraBase = process.env.WEKNORA_BASE_URL ?? 'http://localhost:8092';
const weknoraKey = process.env.WEKNORA_API_KEY;
const kbRD = process.env.WEKNORA_KB_RD;
const kbHR = process.env.WEKNORA_KB_HR;
const bin = process.env.PICOCCLAW_BIN ?? path.resolve('build/picoclaw-knowledge-e2e');
const port = Number(process.env.KNOWLEDGE_PORT ?? 18820);
const runId = Date.now().toString(36);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

if (!weknoraKey || !kbRD || !kbHR) {
  console.error('WEKNORA_API_KEY / WEKNORA_KB_RD / WEKNORA_KB_HR are required');
  process.exit(1);
}

const processes = [];
const homes = [];
try {
  const admin = await login('admin', 'change-this-admin-password');
  const org = await provision(admin);
  const agent = await launchAgent(org);
  await waitHealthy(agent);

  const rd = await login(org.rd.username, org.passwords.rd);
  const boss = await login(org.boss.username, org.passwords.boss);

  // K1 — own-department knowledge is retrieved and cited.
  await converse('K1 RD employee cites the release handbook', rd, agent, `k1-${runId}`,
    'AEP_KNOWLEDGE_SEARCH KNQ:灰度发布 请查发布流程',
    body => body.includes('AEP_KNOWLEDGE_OK') && body.includes('灰度') && !body.includes('薪酬'));

  // K2 — out-of-scope knowledge cannot leak: only kb-rd is searched.
  await converse('K2 RD employee cannot retrieve HR salary knowledge', rd, agent, `k2-${runId}`,
    'AEP_KNOWLEDGE_SEARCH KNQ:薪酬等级 请查薪酬等级',
    body => body.includes('AEP_KNOWLEDGE_OK') && !body.includes('P6月薪'));

  // K3 — the cross-department lead sees both KBs.
  await converse('K3 cross-dept lead retrieves the HR passage', boss, agent, `k3-${runId}`,
    'AEP_KNOWLEDGE_SEARCH KNQ:薪酬等级 请查薪酬等级',
    body => body.includes('AEP_KNOWLEDGE_OK') && body.includes('薪酬'));

  console.log('ALL KNOWLEDGE E2E SCENARIOS PASSED');
} catch (error) {
  console.error('KNOWLEDGE E2E FAILED:', error.message);
  for (const home of homes) {
    try {
      const {execSync} = await import('node:child_process');
      console.error(execSync(`tail -25 ${home}/logs/gateway.log 2>/dev/null || true`, {shell: '/bin/bash', encoding: 'utf8'}));
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
    body: JSON.stringify({deploymentId: 'demo', sessionId: `kn-${runId}-${username}`, username, password}),
  });
  if (!resp.ok) throw new Error(`login ${username}: ${resp.status} ${await resp.text()}`);
  return resp.json();
}

async function adminPost(path_, body) {
  const resp = await fetch(base + path_, {
    method: 'POST',
    headers: {'Content-Type': 'application/json', 'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${(await login('admin', 'change-this-admin-password')).accessToken}`},
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (resp.status >= 300 && resp.status !== 409) throw new Error(`provision ${path_}: ${resp.status} ${text}`);
  return text ? JSON.parse(text) : null;
}

async function provision(admin) {
  const r = runId;
  const teams = {rd: `kn-rd-${r}`, hr: `kn-hr-${r}`, boss: `kn-boss-${r}`};
  const passwords = {rd: 'rd-password-123', boss: 'boss-password-123'};
  for (const key of ['rd', 'hr', 'boss']) {
    await adminPost('/aep/v1/admin/teams', {id: teams[key], name: `KN Department ${key} ${r}`});
  }
  await adminPost('/aep/v1/admin/roles', {
    id: `kn-runner-${r}`, name: `KN Runner ${r}`,
    permissions: ['models.read', 'skills.read', 'data_scope.read'],
  });
  await adminPost('/aep/v1/admin/roles', {
    id: `kn-employee-${r}`, name: `KN Employee ${r}`, permissions: ['models.read', 'skills.read'],
  });
  await adminPost('/aep/v1/admin/users', {
    deploymentId: 'demo', username: `kn-rd-${r}`, displayName: 'Ren Fagong (RD)',
    temporaryPassword: passwords.rd, requirePasswordChange: false,
    teamIds: [teams.rd], roleIds: [`kn-employee-${r}`],
  });
  await adminPost('/aep/v1/admin/users', {
    deploymentId: 'demo', username: `kn-boss-${r}`, displayName: 'Bai Lingdao (cross-dept)',
    temporaryPassword: passwords.boss, requirePasswordChange: false,
    teamIds: [teams.boss], roleIds: [`kn-employee-${r}`],
  });
  // Cross-department management scope for the lead over both departments.
  const listUsers = await fetch(base + '/aep/v1/admin/users?limit=200', {
    headers: {'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${admin.accessToken}`},
  }).then(r => r.json());
  const bossRow = listUsers.items.find(item => item.username === `kn-boss-${r}`);
  for (const team of [teams.rd, teams.hr]) {
    await adminPost('/aep/v1/admin/data-scope-rules', {
      id: `kn-scope-${team}-${r}`, ruleKind: 'management_scope', subjectType: 'user',
      subjectId: bossRow.id, resourceKind: 'team', resourceId: team, reason: `kn e2e ${r}`,
    });
  }
  const agentAccount = await adminPost('/aep/v1/admin/agents', {
    username: `kn-agent-${r}`, displayName: `KN Digital Employee ${r}`, password: 'agent-password-123',
    roleIds: [`kn-runner-${r}`], teamIds: [], homeTeamId: teams.boss, displayTitle: 'Knowledge assistant',
  });
  await adminPost('/aep/v1/admin/model-assignments', {
    modelId: 'enterprise-chat', subject: {type: 'user', id: agentAccount.id},
  });
  return {teams, passwords, rd: {username: `kn-rd-${r}`}, boss: {username: `kn-boss-${r}`}};
}

async function launchAgent(org) {
  const home = mkdtempSync(path.join(tmpdir(), 'kn-agent-'));
  homes.push(home);
  writeFileSync(path.join(home, 'config.json'), JSON.stringify({
    version: 3,
    aep: {
      enabled: true, base_url: base, deployment_id: 'demo',
      username: `kn-agent-${runId}`, session_id: `kn-agent-${runId}`,
    },
    knowledge: {
      enabled: true, base_url: weknoraBase, max_passages: 3,
      team_kb_map: {[org.teams.rd]: [kbRD], [org.teams.hr]: [kbHR], [org.teams.boss]: []},
    },
    channel_list: {aepchat: {enabled: true, type: 'aepchat'}},
    model_list: [],
    agents: {defaults: {
      workspace: path.join(home, 'workspace'), restrict_to_workspace: true,
      max_tokens: 1024, max_tool_iterations: 6,
    }},
    gateway: {host: '127.0.0.1', port},
  }, null, 2));
  const proc = spawn(bin, ['gateway'], {
    env: {...process.env, PICOCLAW_HOME: home, PICOCLAW_AEP_PASSWORD: 'agent-password-123', PICOCLAW_KNOWLEDGE_API_KEY: weknoraKey},
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
  throw new Error(`agent on :${agent.port} did not become healthy`);
}

async function converse(label, session, agent, chatId, text, test) {
  const sent = await fetch(`${agent.url}/aepchat/v1/chats/${encodeURIComponent(chatId)}/messages`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json', Authorization: `Bearer ${session.accessToken}`},
    body: JSON.stringify({text}),
  });
  if (sent.status !== 202) throw new Error(`${label}: post ${sent.status} ${await sent.text()}`);
  const deadline = Date.now() + 120_000;
  let after = 0;
  let lastReply = '(none)';
  while (Date.now() < deadline) {
    const resp = await fetch(`${agent.url}/aepchat/v1/chats/${encodeURIComponent(chatId)}/messages?after=${after}`, {
      headers: {Authorization: `Bearer ${session.accessToken}`},
    });
    if (!resp.ok) throw new Error(`${label} poll ${resp.status}`);
    const page = await resp.json();
    after = page.nextAfter ?? after;
    const reply = (page.messages ?? []).filter(m => m.from === 'agent').at(-1);
    if (reply) lastReply = reply.text;
    if (reply && test(reply.text)) {
      console.log(`PASS ${label}`);
      return reply;
    }
    await sleep(1500);
  }
  throw new Error(`${label}: expected reply did not arrive; last: ${JSON.stringify(lastReply)}`);
}
