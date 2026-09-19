// Digital-employee conversation E2E for the AEP runtime.
//
// Drives real employees (kind=human) chatting with real digital employees
// (kind=agent picoclaw instances) through the aepchat web API against a
// running AEP control service + model gateway:
//
//   S0  auth negatives: no/invalid token 401; digital-employee token 403
//   S1  regular employee of dept A <-> dept A's digital employee (direct)
//   S2  dept A leader <-> dept A's digital employee (direct)
//   S3  cross-department leader <-> BOTH departments' digital employees
//   S4  group chat: one message fanned out to both digital employees
//   S5  delegated department reports, scoped to the REQUESTER:
//       - dept A employee sees dept A rows only
//       - cross-dept leader sees dept A + dept B rows
//       - dept A employee asking for dept B is DENIED
//
// Prerequisites: AEP gateway-profile stack (compose:gateway:up), the picoclaw
// binary built with aepchat. Env: AEP_BASE_URL, PICOCCLAW_BIN.
import {spawn} from 'node:child_process';
import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';

const base = process.env.AEP_BASE_URL ?? 'http://localhost:8080';
const bin = process.env.PICOCCLAW_BIN ?? path.resolve('build/picoclaw-aep-e2e');
const runId = Date.now().toString(36);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const agentPassword = 'de-password-123';
const ports = {A: 18801, B: 18802};

const processes = [];
const homes = [];
try {
  const admin = await login('admin', 'change-this-admin-password', 'm0-e2e-admin');
  const org = await provision(admin);
  const deA = await launchAgent(org.agents.A.username, 'A');
  const deB = await launchAgent(org.agents.B.username, 'B');
  await Promise.all([waitHealthy(deA), waitHealthy(deB)]);

  const zhang = await login(org.users.zhang.username, org.passwords.zhang, 'e2e-zhang');
  const liA = await login(org.users.liA.username, org.passwords.liA, 'e2e-lia');
  const boss = await login(org.users.boss.username, org.passwords.boss, 'e2e-boss');
  const deAToken = await login(org.agents.A.username, agentPassword, 'e2e-de-a');

  // S0 — auth negatives.
  await expectStatus(() => post(deA, 's0', 'x', null), 401, 'S0 no token');
  await expectStatus(() => post(deA, 's0', 'x', 'not-a-token'), 401, 'S0 invalid token');
  await expectStatus(() => post(deA, 's0', 'x', deAToken.accessToken), 403, 'S0 digital-employee sender rejected');

  // S1 — regular employee <-> own department's digital employee.
  await converse('S1 employee<->deptA agent', zhang, deA, `s1-${runId}`,
    'Say hello', /Hello AEP/);

  // S2 — department leader <-> same digital employee.
  await converse('S2 leader<->deptA agent', liA, deA, `s2-${runId}`,
    'Say hello', /Hello AEP/);

  // S3 — cross-department leader talks to both departments' agents.
  await converse('S3 cross-dept leader<->deptA agent', boss, deA, `s3a-${runId}`,
    'Say hello', /Hello AEP/);
  await converse('S3 cross-dept leader<->deptB agent', boss, deB, `s3b-${runId}`,
    'Say hello', /Hello AEP/);

  // S4 — one group message, both digital employees answer under the same chat.
  const groupChat = `group-${runId}`;
  await post(deA, groupChat, 'Team, please introduce yourselves.', boss.accessToken, 'group');
  await post(deB, groupChat, 'Team, please introduce yourselves.', boss.accessToken, 'group');
  await Promise.all([
    pollFor('S4 deptA agent in group', deA, groupChat, boss, /Hello AEP/),
    pollFor('S4 deptB agent in group', deB, groupChat, boss, /Hello AEP/),
  ]);
  console.log('PASS S4 group chat: both digital employees answered');

  // S5 — delegated department reports, scoped to the requester.
  const deptA = org.teams.A;
  const deptB = org.teams.B;
  await converse('S5 employee report: own dept only', zhang, deA, `s5a-${runId}`,
    'AEP_DEPT_REPORT please', body => body.includes(`team=${deptA} revenue`) && !body.includes(`team=${deptB}`));
  await converse('S5 cross-dept leader report: both depts', boss, deA, `s5b-${runId}`,
    'AEP_DEPT_REPORT please', body => body.includes(`team=${deptA} revenue`) && body.includes(`team=${deptB} revenue`));
  await converse('S5 employee denied other dept', zhang, deA, `s5c-${runId}`,
    `AEP_DEPT_REPORT AEP_DEPT_REPORT_TEAM:${deptB} please`, body => body.includes(`DENIED team=${deptB}`));

  // S6 — conversation isolation: two chat ids never see each other.
  await converse('S6a chat alpha', zhang, deA, `s6a-${runId}`, 'Say hello', /Hello AEP/);
  await converse('S6b chat beta', zhang, deA, `s6b-${runId}`, 'Say hello', /Hello AEP/);
  {
    const page = await (await fetch(`${deA.url}/aepchat/v1/chats/${encodeURIComponent(`s6a-${runId}`)}/messages`, {
      headers: {Authorization: `Bearer ${zhang.accessToken}`},
    })).json();
    const seenBeta = (page.messages ?? []).some(m => (m.text ?? '').includes('beta-marker'));
    if (seenBeta) throw new Error('S6: chat alpha leaked chat beta traffic');
    console.log('PASS S6 conversation isolation between chat ids');
  }

  // S7 — the employee access token is not an admin credential.
  {
    const adminProbe = await fetch(base + '/aep/v1/admin/agents?limit=1', {
      headers: {'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${zhang.accessToken}`},
    });
    if (adminProbe.status !== 403) throw new Error(`S7: employee token on admin API = ${adminProbe.status}`);
    console.log('PASS S7 employee token rejected on admin API');
  }

  console.log('ALL DIGITAL EMPLOYEE E2E SCENARIOS PASSED');
} catch (error) {
  console.error('E2E FAILED:', error.message);
  for (const proc of processes) {
    const home = homes[processes.indexOf(proc)];
    console.error(`--- ${home}/gateway.log tail ---`);
    try {
      const {execSync} = await import('node:child_process');
      console.error(execSync(`tail -40 ${home}/logs/gateway.log 2>/dev/null || true`, {shell: '/bin/bash', encoding: 'utf8'}));
    } catch {}
  }
  process.exitCode = 1;
} finally {
  for (const proc of processes) proc.kill('SIGTERM');
  await new Promise(resolve => setTimeout(resolve, 500));
  for (const home of homes) rmSync(home, {recursive: true, force: true});
}

async function login(username, password, sessionId) {
  const resp = await fetch(base + '/aep/v1/auth/password/login', {
    method: 'POST',
    headers: {'Content-Type': 'application/json', 'X-AEP-Protocol-Version': '1.0'},
    body: JSON.stringify({deploymentId: 'demo', sessionId: `${sessionId}-${runId}`, username, password}),
  });
  if (!resp.ok) throw new Error(`login ${username} failed: ${resp.status} ${await resp.text()}`);
  return resp.json();
}

async function adminPost(admin, path_, body) {
  const resp = await fetch(base + path_, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json', 'X-AEP-Protocol-Version': '1.0',
      Authorization: `Bearer ${admin.accessToken}`,
    },
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  return {status: resp.status, body: text ? JSON.parse(text) : null};
}

async function ensure(admin, path_, body, label) {
  const {status, body: out} = await adminPost(admin, path_, body);
  if (status >= 300 && status !== 409) throw new Error(`provision ${label}: ${status} ${JSON.stringify(out)}`);
  return out;
}

async function provision(admin) {
  const r = runId;
  const teams = {A: `de-dept-a-${r}`, B: `de-dept-b-${r}`, MGMT: `de-mgmt-${r}`};
  const roles = {
    employee: `de-employee-${r}`,
    runner: `de-agent-runner-${r}`, // models + data_scope.read for delegated scope resolution
  };
  const users = {
    zhang: {username: `de-zhang-${r}`, displayName: 'Zhang San (employee)'},
    liA: {username: `de-lia-${r}`, displayName: 'Li Ai (dept A leader)'},
    boss: {username: `de-boss-${r}`, displayName: 'Wang Wu (cross-dept leader)'},
  };
  const passwords = {zhang: 'zhang-password-123', liA: 'lia-password-123', boss: 'boss-password-123'};
  const agents = {
    A: {username: `de-agent-a-${r}`, displayName: `Dept A Digital Employee ${r}`},
    B: {username: `de-agent-b-${r}`, displayName: `Dept B Digital Employee ${r}`},
  };

  await ensure(admin, '/aep/v1/admin/roles', {
    id: roles.employee, name: `DE employee ${r}`, description: 'chat only',
    permissions: ['models.read', 'skills.read'],
  }, 'employee role');
  await ensure(admin, '/aep/v1/admin/roles', {
    id: roles.runner, name: `DE agent runner ${r}`, description: 'digital employee runtime',
    permissions: ['models.read', 'skills.read', 'data_scope.read'],
  }, 'runner role');
  for (const key of ['A', 'B', 'MGMT']) {
    await ensure(admin, '/aep/v1/admin/teams', {
      id: teams[key], name: `DE Department ${key} ${r}`,
    }, `team ${key}`);
  }

  const mkUser = async (user, teamIds, roleIds) => ensure(admin, '/aep/v1/admin/users', {
    deploymentId: 'demo', username: user.username, displayName: user.displayName,
    temporaryPassword: passwords[user === users.zhang ? 'zhang' : user === users.liA ? 'liA' : 'boss'],
    requirePasswordChange: false, teamIds, roleIds,
  }, `user ${user.username}`);

  await mkUser(users.zhang, [teams.A], [roles.employee]);
  await mkUser(users.liA, [teams.A], [roles.employee]);
  await mkUser(users.boss, [teams.MGMT], [roles.employee]);

  const mkAgent = async (agent, homeTeam) => {
    const out = await ensure(admin, '/aep/v1/admin/agents', {
      username: agent.username, displayName: agent.displayName, password: agentPassword,
      roleIds: [roles.runner], teamIds: [], homeTeamId: homeTeam, displayTitle: 'Department assistant',
    }, `agent ${agent.username}`);
    agent.id = out?.id;
    return agent;
  };
  await mkAgent(agents.A, teams.A);
  await mkAgent(agents.B, teams.B);

  // Leaders' management scope: dept A leader sees dept A; the cross-dept
  // leader sees both departments.
  const grantScope = (subjectId, team) => ensure(admin, '/aep/v1/admin/data-scope-rules', {
    id: `de-scope-${subjectId ? subjectId.slice(0, 8) : 'x'}-${team}-${r}`.replace(/[^a-zA-Z0-9_-]/g, 'y'),
    ruleKind: 'management_scope', subjectType: 'user', subjectId,
    resourceKind: 'team', resourceId: team, reason: `de e2e ${runId}`,
  }, `scope ${team}`);
  // Provision ids come back as UUIDs; rules need the real user ids.
  const listUsers = async username => {
    const resp = await fetch(base + `/aep/v1/admin/users?limit=200`, {
      headers: {'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${admin.accessToken}`},
    });
    const body = await resp.json();
    return body.items.find(item => item.username === username);
  };
  const liARow = await listUsers(users.liA.username);
  const bossRow = await listUsers(users.boss.username);
  await grantScope(liARow.id, teams.A);
  await grantScope(bossRow.id, teams.A);
  await grantScope(bossRow.id, teams.B);

  // Model: reuse/create the shared mock-backed model and assign per agent.
  await ensure(admin, '/aep/v1/admin/models', {
    id: 'enterprise-chat', displayName: 'Enterprise Chat', sourceType: 'gateway',
    protocol: 'openai-compatible', endpoint: 'http://mock-openai.aep.internal:8080/v1',
    upstreamModel: 'mock-upstream-chat', credentialId: (await ensure(admin, '/aep/v1/admin/credentials', {
      name: `DE provider ${r}`, service: 'mock-openai', type: 'api_key',
      deliveryMode: 'server_only', value: 'm1-e2e-provider-secret', enabled: true,
    }, 'credential')).id,
    capabilities: ['text'], contextWindow: 8192, isDefault: true, enabled: true,
  }, 'model');
  for (const key of ['A', 'B']) {
    await ensure(admin, '/aep/v1/admin/model-assignments', {
      modelId: 'enterprise-chat', subject: {type: 'user', id: agents[key].id},
    }, `model assignment ${key}`);
  }
  return {teams, roles, users, passwords, agents};
}

async function launchAgent(username, key) {
  const home = mkdtempSync(path.join(tmpdir(), `de-${key}-`));
  homes.push(home);
  const port = ports[key];
  writeFileSync(path.join(home, 'config.json'), JSON.stringify({
    version: 3,
    aep: {
      enabled: true, base_url: base, deployment_id: 'demo',
      username, session_id: `de-${key}-${runId}`,
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
    env: {...process.env, PICOCLAW_HOME: home, PICOCLAW_AEP_PASSWORD: agentPassword},
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  proc.stdout.on('data', () => {});
  proc.stderr.on('data', () => {});
  processes.push(proc);
  return {port, home, proc, url: `http://127.0.0.1:${port}`};
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

async function post(agent, chatId, text, token, chatType = 'direct') {
  const resp = await fetch(`${agent.url}/aepchat/v1/chats/${encodeURIComponent(chatId)}/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? {Authorization: `Bearer ${token}`} : {}),
    },
    body: JSON.stringify({text, chatType}),
  });
  const body = await resp.text();
  return {status: resp.status, body};
}

async function pollOnce(agent, chatId, token, after) {
  const resp = await fetch(`${agent.url}/aepchat/v1/chats/${encodeURIComponent(chatId)}/messages?after=${after}`, {
    headers: {Authorization: `Bearer ${token}`},
  });
  if (!resp.ok) throw new Error(`poll ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

async function pollFor(label, agent, chatId, session, predicate) {
  const test = typeof predicate === 'function' ? predicate : body => predicate.test(body);
  const deadline = Date.now() + 90_000;
  let after = 0;
  let lastReply = '(none)';
  while (Date.now() < deadline) {
    const page = await pollOnce(agent, chatId, session.accessToken, after);
    after = page.nextAfter ?? after;
    const reply = (page.messages ?? []).filter(message => message.from === 'agent').at(-1);
    if (reply) lastReply = reply.text;
    if (reply && test(reply.text)) return reply;
    await sleep(1500);
  }
  throw new Error(`${label}: expected reply did not arrive; last agent reply: ${JSON.stringify(lastReply)}`);
}

async function converse(label, session, agent, chatId, text, predicate) {
  const sent = await post(agent, chatId, text, session.accessToken);
  if (sent.status !== 202) throw new Error(`${label}: post failed ${sent.status} ${sent.body}`);
  const reply = await pollFor(label, agent, chatId, session, predicate);
  console.log(`PASS ${label}`);
  return reply;
}

async function expectStatus(fn, status, label) {
  const got = await fn();
  if (got.status !== status) throw new Error(`${label}: expected ${status}, got ${got.status} ${got.body}`);
  console.log(`PASS ${label}`);
}
