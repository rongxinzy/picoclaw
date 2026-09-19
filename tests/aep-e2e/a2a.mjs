// A2A E2E: agent-to-agent delegation anchored on AEP.
//
//   A1  agent A (dept-a) invokes agent B (dept-b) for the cross-department
//       leader: the leader talks to A, A delegates to B act-as the LEADER,
//       and B's department report covers the leader's scope (both depts)
//   A2  the same delegation for a dept-a employee: B's report stays inside
//       the EMPLOYEE's frozen scope — dept-b data is denied
//   A3  a call without the act-as principal is rejected by the protocol
//   A4  a chain deeper than the configured maximum is rejected
//   A5  a peer without agents.invoke is rejected with a permission error
//
// Prerequisites: AEP gateway stack; the picoclaw binary with a2a support.
import {spawn} from 'node:child_process';
import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';

const base = process.env.AEP_BASE_URL ?? 'http://localhost:8080';
const bin = process.env.PICOCCLAW_BIN ?? path.resolve('build/picoclaw-a2a-e2e');
const runId = Date.now().toString(36);
const ports = {A: 18830, B: 18831};
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const processes = [];
const homes = [];
try {
  const admin = await login('admin', 'change-this-admin-password');
  const org = await provision(admin);
  const agentA = await launchAgent(org.agents.A.username, 'A', {[org.peers.B]: `http://127.0.0.1:${ports.B}`});
  const agentB = await launchAgent(org.agents.B.username, 'B', {});
  await Promise.all([waitHealthy(agentA), waitHealthy(agentB)]);

  const leader = await login(org.users.leader.username, org.passwords.leader);
  const employee = await login(org.users.employee.username, org.passwords.employee);

  // A1 — the leader's scope crosses departments through the delegation.
  await converse('A1 leader delegation covers both departments', leader, agentA, `a1-${runId}`,
    `AEP_DEPT_REPORT AEP_A2A_INVOKE A2A_PEER:${org.peers.B} A2A_MSG:AEP_DEPT_REPORT A2A_END 请出跨部门报告`,
    body => body.includes('A2A_OK') && body.includes(`team=${org.teams.A}`) && body.includes(`team=${org.teams.B}`), 180_000);

  // A2 — the employee's scope does not widen across the call.
  await converse('A2 employee delegation stays inside dept-a', employee, agentA, `a2-${runId}`,
    `AEP_DEPT_REPORT AEP_A2A_INVOKE A2A_PEER:${org.peers.B} A2A_MSG:AEP_DEPT_REPORT A2A_END 请出报告`,
    body => body.includes('A2A_OK') && body.includes(`team=${org.teams.A}`) && !body.includes(`team=${org.teams.B}`), 180_000);

  // A3 — no act-as principal: the protocol itself refuses. A valid
  // agents.invoke bearer (agent A) isolates the act-as check.
  const agentASession = await login(org.agents.A.username, 'agent-password-123');
  await expectRPCError('A3 act-as is mandatory', agentB, agentASession,
    {actAs: null}, /aep_act_as_user is required|-32003/);

  // A4 — chain depth beyond the cap is refused.
  await expectRPCError('A4 chain depth capped', agentB, agentASession,
    {actAs: 'u1', chain: 'x>y>z'}, /maximum depth|-32004/);

  // A5 — a valid token without agents.invoke is refused on permission.
  await expectRPCError('A5 permission required', agentB, employee,
    {actAs: 'u1'}, /agents.invoke|-32002/);

  console.log('ALL A2A E2E SCENARIOS PASSED');
} catch (error) {
  console.error('A2A E2E FAILED:', error.message);
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
    body: JSON.stringify({deploymentId: 'demo', sessionId: `a2a-${runId}-${username}`, username, password}),
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
  const teams = {A: `a2a-dept-a-${r}`, B: `a2a-dept-b-${r}`};
  const peers = {A: 'agentA', B: 'agentB'};
  const passwords = {leader: 'leader-password-123', employee: 'employee-password-123'};
  for (const key of ['A', 'B']) {
    await adminPost('/aep/v1/admin/teams', {id: teams[key], name: `A2A Department ${key} ${r}`});
  }
  await adminPost('/aep/v1/admin/roles', {
    id: `a2a-runner-${r}`, name: `A2A Runner ${r}`,
    permissions: ['models.read', 'skills.read', 'data_scope.read', 'agents.invoke'],
  });
  await adminPost('/aep/v1/admin/roles', {
    id: `a2a-employee-${r}`, name: `A2A Employee ${r}`, permissions: ['models.read', 'skills.read'],
  });

  const mkUser = (username, displayName, password, team, role) => adminPost('/aep/v1/admin/users', {
    deploymentId: 'demo', username, displayName, temporaryPassword: password,
    requirePasswordChange: false, teamIds: [team], roleIds: [role],
  });
  await mkUser(`a2a-leader-${r}`, 'Ling Dao (cross-dept)', passwords.leader, teams.A, `a2a-employee-${r}`);
  await mkUser(`a2a-employee-${r}`, 'Yuan Gong (dept-a)', passwords.employee, teams.A, `a2a-employee-${r}`);

  // The leader may see both departments; the employee only dept-a.
  const listUsers = await fetch(base + '/aep/v1/admin/users?limit=200', {
    headers: {'X-AEP-Protocol-Version': '1.0', Authorization: `Bearer ${admin.accessToken}`},
  }).then(rsp => rsp.json());
  const leaderRow = listUsers.items.find(item => item.username === `a2a-leader-${r}`);
  await adminPost('/aep/v1/admin/data-scope-rules', {
    id: `a2a-scope-${r}`, ruleKind: 'management_scope', subjectType: 'user',
    subjectId: leaderRow.id, resourceKind: 'team', resourceId: teams.B, reason: `a2a e2e ${r}`,
  });

  const mkAgent = async (username, home) => {
    const account = await adminPost('/aep/v1/admin/agents', {
      username, displayName: `A2A Agent ${username}`, password: 'agent-password-123',
      roleIds: [`a2a-runner-${r}`], teamIds: [], homeTeamId: home, displayTitle: 'Peer agent',
    });
    await adminPost('/aep/v1/admin/model-assignments', {
      modelId: 'enterprise-chat', subject: {type: 'user', id: account.id},
    });
    return {username, id: account.id};
  };
  const agents = {
    A: await mkAgent(`a2a-agent-a-${r}`, teams.A),
    B: await mkAgent(`a2a-agent-b-${r}`, teams.B),
  };
  return {teams, peers, passwords, users: {leader: {username: `a2a-leader-${r}`}, employee: {username: `a2a-employee-${r}`}}, agents};
}

async function launchAgent(username, key, peers) {
  const home = mkdtempSync(path.join(tmpdir(), `a2a-${key}-`));
  homes.push(home);
  writeFileSync(path.join(home, 'config.json'), JSON.stringify({
    version: 3,
    aep: {
      enabled: true, base_url: base, deployment_id: 'demo',
      username, session_id: `a2a-${key}-${runId}`,
    },
    a2a: {peers},
    channel_list: {
      aepchat: {enabled: true, type: 'aepchat'},
      a2a: {enabled: true, type: 'a2a', settings: {public_url: `http://127.0.0.1:${ports[key]}`, max_chain_depth: 3}},
    },
    model_list: [],
    agents: {defaults: {
      workspace: path.join(home, 'workspace'), restrict_to_workspace: true,
      max_tokens: 1024, max_tool_iterations: 8,
    }},
    gateway: {host: '127.0.0.1', port: ports[key]},
  }, null, 2));
  const proc = spawn(bin, ['gateway'], {
    env: {...process.env, PICOCLAW_HOME: home, PICOCLAW_AEP_PASSWORD: 'agent-password-123'},
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  proc.stdout.on('data', () => {});
  proc.stderr.on('data', () => {});
  processes.push(proc);
  return {port: ports[key], url: `http://127.0.0.1:${ports[key]}`, home};
}

async function waitHealthy(agent) {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    try {
      const resp = await fetch(agent.url + '/a2a/card.json');
      if (resp.ok) return;
    } catch {}
    await sleep(1000);
  }
  throw new Error(`agent on :${agent.port} did not become healthy`);
}

async function converse(label, session, agent, chatId, text, test, timeoutMs = 120_000) {
  const sent = await fetch(`${agent.url}/aepchat/v1/chats/${encodeURIComponent(chatId)}/messages`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json', Authorization: `Bearer ${session.accessToken}`},
    body: JSON.stringify({text}),
  });
  if (sent.status !== 202) throw new Error(`${label}: post ${sent.status} ${await sent.text()}`);
  const deadline = Date.now() + timeoutMs;
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

async function expectRPCError(label, agent, session, opts, pattern) {
  // Raw JSON-RPC probes with a real bearer so each negative isolates one rule.
  const body = JSON.stringify({
    jsonrpc: '2.0', id: 1, method: 'message/send',
    params: {message: {role: 'user', parts: [{kind: 'text', text: 'probe'}], metadata: {
      ...(opts?.actAs ? {aep_act_as_user: opts.actAs} : {}),
      ...(opts?.chain ? {aep_chain: opts.chain} : {}),
    }}},
  });
  const resp = await fetch(agent.url + '/a2a/', {
    method: 'POST',
    headers: {'Content-Type': 'application/json', Authorization: `Bearer ${session.accessToken}`},
    body,
  });
  const text = await resp.text();
  if (!pattern.test(text)) throw new Error(`${label}: ${text}`);
  console.log(`PASS ${label}`);
}
