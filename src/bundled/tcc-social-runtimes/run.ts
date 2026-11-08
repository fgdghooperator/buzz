import type { AgentType } from "../../features/agents/service";
import { type Config, config } from "./config";
import type { TccRuntime } from "./runtime";

const HANDOVER_MS = 2 * 60_000;

export const tccRun = (
  runtime: TccRuntime,
): Pick<
  AgentType<Config>,
  "sessionControls" | "run" | "runOnce" | "timeoutMs" | "resumes"
> => ({
  sessionControls: {
    scope: (agent) => config(agent.config).scope,
    state: (agent, place) => runtime.sessionState(agent, place),
    run: (agent, place, action) => runtime.session(agent, place, action),
    subscribe: (listener) => runtime.subscribe(listener),
  },
  run: (delivery) => runtime.run(delivery),
  runOnce: (turn) => runtime.runOnce(turn),
  timeoutMs: HANDOVER_MS,
  resumes: true,
});
