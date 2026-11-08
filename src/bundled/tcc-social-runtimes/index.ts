import { isTauri } from "@tauri-apps/api/core";
import type { PluginModule } from "../../plugins/api";
import { repositoriesContext } from "../../features/communities/repositories";
import { GrokLogoIcon, RobotIcon } from "../../shared/design-system/icons";
import { type Config, DEFAULT_CONFIG } from "./config";
import { GROK_HOOKS, TIMMY_HOOKS } from "./acp";
import { TccRuntime } from "./runtime";
import { tccRun } from "./run";

export const inject = ["agents", "host"];

export const apply: PluginModule["apply"] = (ctx) => {
  if (!isTauri()) return;

  const { agents, host } = ctx;

  const timmyTypeKey = `${ctx.pluginOwner?.id ?? "tcc.social-runtimes"}/timmy`;
  const grokTypeKey = `${ctx.pluginOwner?.id ?? "tcc.social-runtimes"}/grok`;

  const makeRuntime = (
    label: string,
    runtimeKey: string,
    hooks: typeof TIMMY_HOOKS,
  ) =>
    new TccRuntime(
      label,
      runtimeKey,
      hooks,
      host,
      agents.relay,
      (key, signal) => agents.core(key, signal),
      localStorage,
      () => agents.tools(),
      repositoriesContext,
      agents.storage,
    );

  const timmy = makeRuntime("Timmy", "tcc-timmy", TIMMY_HOOKS);
  const grok = makeRuntime("Grok", "tcc-grok", GROK_HOOKS);

  const sync = () => {
    const snapshot = agents.snapshot();
    if (snapshot.status === "loading") return;

    timmy.sync(
      snapshot.running.filter((agent) => agent.type === timmyTypeKey),
    );

    grok.sync(
      snapshot.running.filter((agent) => agent.type === grokTypeKey),
    );
  };

  const unsubscribe = agents.subscribe(sync);
  sync();

  ctx.effect(() => () => {
    unsubscribe();
    timmy.dispose();
    grok.dispose();
  });

  agents.register<Config>({
    id: "timmy",
    title: "TCC Timmy",
    icon: RobotIcon,
    description:
      "TCC execution lead using the dedicated Timmy Hermes ACP profile.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Timmy · Hermes ACP",
    ...tccRun(timmy),
  });

  agents.register<Config>({
    id: "grok",
    title: "TCC Grok",
    icon: GrokLogoIcon,
    description:
      "TCC X intelligence scout using native Grok ACP and X search.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Grok 4.7 · X intelligence",
    ...tccRun(grok),
  });
};
