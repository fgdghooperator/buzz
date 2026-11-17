export const inject = ["agents", "host"];

const PILOT = Object.freeze({
  objectiveId: "REM-165-TREY-PILOT-001",
  channelId: "2dcedaf8-3cb6-4ab3-ad6e-389248616972",
  threadRootId:
    "513d637a6ed0e4782b091ecd47cd6476db42df379f6e81f82bcadbd650b0f5c1",
});

// Replaceable role registry: the state machine consumes capability roles only.
// Fixed identities are a harmless-pilot fixture and can change without changing
// the planner or dispatch state machine.
const ROLE_REGISTRY = Object.freeze({
  revision: 7,
  roles: Object.freeze({
    IMPLEMENTER: Object.freeze({
      pubkey:
        "801987f5ed7ddd36bf855042b297ea7fa1882bf320580521bd7afa69e2aff5f9",
      displayName: "Kenny | REMH - RE Management Holdings",
    }),
    INFRASTRUCTURE: Object.freeze({
      pubkey:
        "03f0cc2c1b1c292fec9d1813954b2b4986ff5d0f7a6d1fa2cd4e3f3188a05ab6",
      displayName: "Jimmy | Wiring",
    }),
    TECHNICAL_REVIEWER: Object.freeze({
      pubkey:
        "149a9f86ef67dad94202432aa889fd8075f82d6f2282083d1455e659c9f29fd5",
      displayName: "REMH Grok Build Specialist",
    }),
    INDEPENDENT_AUDITOR: Object.freeze({
      pubkey:
        "116cfc840c6ee3a95cdb4bc91d525ec45dc6a37bc2987b0f6c3e18d04d0bce86",
      displayName: "Claude | REMH Audit",
    }),
    OWNER: Object.freeze({
      pubkey:
        "f22e6b13c6e235caa2162ebc40ead515ba096ab1eeccc2aea87a55edefad4541",
      displayName: "Matt",
    }),
  }),
});

const now = () => Math.floor(Date.now() / 1000);
const MIN_RECHECK_MS = 60_000;

function reply(value) {
  return JSON.stringify(value, null, 2);
}

function assertEnvelope(dispatch) {
  if (
    !dispatch ||
    dispatch.objective_id !== PILOT.objectiveId ||
    dispatch.channel_id !== PILOT.channelId ||
    dispatch.thread_root_id !== PILOT.threadRootId ||
    typeof dispatch.route_identity !== "string" ||
    !dispatch.route_identity.startsWith("remh-route:")
  ) {
    throw new Error(
      "The continuation dispatch left the fixed harmless pilot envelope",
    );
  }
}

export const apply = (ctx) => {
  let timer;
  let stopped = false;

  const run = async (request) => {
    const output = await ctx.host.runCommand("pilot", JSON.stringify(request));
    if (!output)
      throw new Error(
        "The deterministic continuation host did not return output",
      );
    return JSON.parse(output);
  };

  const roleBinding = (role) => {
    const binding = ROLE_REGISTRY.roles[role];
    if (!binding) throw new Error(`No enabled role binding for ${role}`);
    return binding;
  };

  const runningTarget = (role) => {
    const binding = roleBinding(role);
    const snapshot = ctx.agents.snapshot();
    if (snapshot.status === "loading")
      throw new Error("The agent inventory is still loading");
    const target = snapshot.running.find(
      (candidate) => candidate.pubkey === binding.pubkey,
    );
    if (!target) throw new Error(`${role} is not running on this Buzz host`);
    return { ...binding, key: target.key };
  };

  const schedule = (nextReviewAt) => {
    if (timer) clearTimeout(timer);
    timer = undefined;
    if (stopped || !Number.isSafeInteger(nextReviewAt)) return;
    const delay = Math.max(MIN_RECHECK_MS, nextReviewAt * 1000 - Date.now());
    timer = setTimeout(
      () => {
        timer = undefined;
        void evaluate("pump").catch(() => {});
      },
      Math.min(delay, 2_147_000_000),
    );
  };

  const route = async (response) => {
    schedule(response.next_review_at);
    const dispatch = response.dispatch;
    if (!dispatch) return response;
    assertEnvelope(dispatch);
    if (dispatch.state !== "PENDING") return response;

    if (dispatch.target_role === "OWNER") {
      const owner = roleBinding("OWNER");
      return {
        ...response,
        owner_event: {
          route_identity: dispatch.route_identity,
          mention_pubkey: owner.pubkey,
          mention_text: `@${owner.displayName}`,
          message: dispatch.wake_payload,
        },
      };
    }

    const target = runningTarget(dispatch.target_role);
    await run({
      action: "claim",
      now: now(),
      route_identity: dispatch.route_identity,
    });
    ctx.agents.wake(target.key, {
      channel: PILOT.channelId,
      root: PILOT.threadRootId,
      text: dispatch.wake_payload,
    });
    const sent = await run({
      action: "sent",
      now: now(),
      route_identity: dispatch.route_identity,
    });
    schedule(sent.next_review_at);
    return sent;
  };

  const evaluate = async (action, extra = {}) =>
    route(await run({ action, now: now(), ...extra }));

  ctx.agents.addTool({
    name: "remh_continuation_ack",
    commits: true,
    description:
      "Acknowledge one exact harmless REMH deterministic-continuation wake. It performs no REMH business work and rejects unknown markers.",
    inputSchema: {
      type: "object",
      properties: {
        marker: { type: "string" },
        event_id: { type: "string" },
        objective_id: { type: "string" },
        generation: { type: "integer", minimum: 1 },
        route_identity: { type: "string" },
      },
      required: [
        "marker",
        "event_id",
        "objective_id",
        "generation",
        "route_identity",
      ],
      additionalProperties: false,
    },
    async call(client, context, args) {
      if (
        context.channel !== PILOT.channelId ||
        context.root !== PILOT.threadRootId
      )
        throw new Error("ACK is outside the fixed harmless pilot conversation");
      const acked = await run({
        action: "ack",
        now: now(),
        marker: args.marker,
        event_id: args.event_id,
        objective_id: args.objective_id,
        generation: args.generation,
        route_identity: args.route_identity,
        caller_pubkey: client.pubkey,
      });
      if (
        acked.status === "ACKED" &&
        acked.dispatch?.target_role === "INFRASTRUCTURE"
      ) {
        return reply(await evaluate("infrastructure_resolved"));
      }
      schedule(acked.next_review_at);
      return reply(acked);
    },
  });

  ctx.agents.addTool({
    name: "remh_continuation_pilot",
    commits: (args) =>
      !["status", "counters", "condition_report_dependency_status"].includes(
        args.action,
      ),
    description:
      "Operate the fixed harmless REM-165 continuation pilot. The local database is a dispatch/ACK/dedupe projection only; REMH Operations Control events remain authoritative. Use owner_event_sent only after the exact returned @Matt event is actually posted.",
    inputSchema: {
      type: "object",
      properties: {
        action: {
          type: "string",
          enum: [
            "arm",
            "pump",
            "progress",
            "infrastructure_blocked",
            "review_ready",
            "owner_gate",
            "owner_event_sent",
            "owner_action",
            "complete",
            "reconcile_condition_report_dependency",
            "condition_report_dependency_status",
            "status",
            "counters",
          ],
        },
        route_identity: { type: "string" },
      },
      required: ["action"],
      additionalProperties: false,
    },
    async call(_client, _context, args) {
      if (args.action === "owner_event_sent") {
        if (typeof args.route_identity !== "string")
          throw new Error("owner_event_sent requires the exact route_identity");
        await run({
          action: "claim",
          now: now(),
          route_identity: args.route_identity,
        });
        return reply(
          await run({
            action: "sent",
            now: now(),
            route_identity: args.route_identity,
          }),
        );
      }
      if (args.action === "status" || args.action === "counters")
        return reply(await run({ action: args.action }));
      if (args.action === "condition_report_dependency_status")
        return reply(await run({ action: args.action }));
      if (args.action === "reconcile_condition_report_dependency")
        return reply(
          await run({
            action: args.action,
            now: now(),
            dependency_event_id:
              "504370148108d2758ce40c47c5a1f58ea97e16831ad4aa5b151dc6076f1fff30",
            objective_event_id:
              "3655046804e51d1d7a8b5823bc32be8e12b91c5345b46c7b1f2fb91aba4067f2",
            idempotency_key:
              "remh-oi-condition-report-owner-test-ready→operational-findings-v1",
            upstream_state: "OWNER_TEST_READY",
            downstream_state: "IMPLEMENTING",
            downstream_agent_key: "kenny-remh",
            terminal_event_id:
              "ead170f787ca0929a04f9686f46c55cc2499a6e45f404626b6e3d2561aac6339",
            predecessor_event_id:
              "ead170f787ca0929a04f9686f46c55cc2499a6e45f404626b6e3d2561aac6339",
            start_receipt_event_id:
              "ead170f787ca0929a04f9686f46c55cc2499a6e45f404626b6e3d2561aac6339",
            implementer_pubkey:
              "801987f5ed7ddd36bf855042b297ea7fa1882bf320580521bd7afa69e2aff5f9",
            activation_generation: 1,
          }),
        );
      return reply(await evaluate(args.action));
    },
  });

  void run({ action: "status" })
    .then((status) => {
      schedule(status.next_review_at);
      if (status.dispatch?.state === "PENDING") return route(status);
      return status;
    })
    .catch(() => {});

  ctx.effect(() => () => {
    stopped = true;
    if (timer) clearTimeout(timer);
  });
};
