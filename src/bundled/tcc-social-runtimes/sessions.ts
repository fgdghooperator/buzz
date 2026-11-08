import {
  AgentSessions as AcpAgentSessions,
  type SavedConversation,
  type SessionStore,
  type SessionsOptions as AcpSessionsOptions,
} from "../../features/agent-harness/acp/agent-sessions";

export type SessionsOptions = Omit<AcpSessionsOptions, "label">;

export class AgentSessions extends AcpAgentSessions {
  constructor(label: string, options: SessionsOptions) {
    super({ ...options, label });
  }
}

const SAVED_LIMIT = 200;

export function localSessions(
  storage: Storage,
  agent: string,
): SessionStore & {
  list(): [string, SavedConversation][];
} {
  const name = "tcc.social-runtimes.sessions.v1";

  const readAll = (): Record<string, Record<string, SavedConversation>> => {
    try {
      const value = JSON.parse(storage.getItem(name) ?? "{}");
      return value && typeof value === "object" ? value : {};
    } catch {
      return {};
    }
  };

  const list = (saved: Record<string, SavedConversation> = {}) =>
    Object.entries(saved)
      .filter(([, conversation]) => typeof conversation?.at === "number")
      .sort(([, a], [, b]) => b.at - a.at);

  return {
    list: () => list(readAll()[agent]),
    get: (key) => readAll()[agent]?.[key],
    set: (key, saved) => {
      const all = readAll();
      all[agent] = Object.fromEntries(
        list({ ...all[agent], [key]: saved }).slice(0, SAVED_LIMIT),
      );

      storage.setItem(name, JSON.stringify(all));
    },
  };
}
