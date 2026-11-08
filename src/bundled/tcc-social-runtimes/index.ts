import { isTauri } from "@tauri-apps/api/core";
import type { PluginModule } from "../../plugins/api";
import { repositoriesContext } from "../../features/communities/repositories";
import { GrokLogoIcon, RobotIcon } from "../../shared/design-system/icons";
import { type Config, DEFAULT_CONFIG } from "./config";
import {
  CRAIG_HOOKS,
  GROK_HOOKS,
  JIMMY_HOOKS,
  KENNY_HOOKS,
  PIP_HOOKS,
  TIMMY_HOOKS,
  TWEAK_HOOKS,
  WENDY_HOOKS,
} from "./acp";
import { TccRuntime } from "./runtime";
import { tccRun } from "./run";

export const inject = ["agents", "host"];

export const apply: PluginModule["apply"] = (ctx) => {
  if (!isTauri()) return;

  const { agents, host } = ctx;
  const pluginId = ctx.pluginOwner?.id ?? "tcc.social-runtimes";

  const typeKey = (id: string) => `${pluginId}/${id}`;

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

  /*
   * Proven TCC runtimes remain completely intact.
   */
  const timmy = makeRuntime("Timmy", "tcc-timmy", TIMMY_HOOKS);
  const grok = makeRuntime("Grok", "tcc-grok", GROK_HOOKS);

  /*
   * FGD Hermes profiles are deliberately static.
   * Each Buzz agent type maps to exactly one pre-existing Hermes profile
   * declared in manifest.json.
   */
  const jimmy = makeRuntime("Jimmy", "fgd-jimmy", JIMMY_HOOKS);
  const kenny = makeRuntime("Kenny", "fgd-kenny", KENNY_HOOKS);
  const wendy = makeRuntime("Wendy", "fgd-wendy", WENDY_HOOKS);
  const tweak = makeRuntime("Tweak", "fgd-tweak", TWEAK_HOOKS);
  const pip = makeRuntime("Pip", "fgd-pip", PIP_HOOKS);
  const craig = makeRuntime("Craig", "fgd-craig", CRAIG_HOOKS);

  const runtimes = [
    ["timmy", timmy],
    ["grok", grok],
    ["jimmy", jimmy],
    ["kenny", kenny],
    ["wendy", wendy],
    ["tweak", tweak],
    ["pip", pip],
    ["craig", craig],
  ] as const;

  const sync = () => {
    const snapshot = agents.snapshot();
    if (snapshot.status === "loading") return;

    for (const [id, runtime] of runtimes) {
      runtime.sync(
        snapshot.running.filter(
          (agent) => agent.type === typeKey(id),
        ),
      );
    }
  };

  const unsubscribe = agents.subscribe(sync);
  sync();

  ctx.effect(() => () => {
    unsubscribe();
    for (const [, runtime] of runtimes) runtime.dispose();
  });

  /*
   * Existing proven TCC types.
   */
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
    title: "FGD Grok",
    icon: GrokLogoIcon,
    description:
      "FGD Grok agents using native Grok ACP and X search.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Grok 4.7 · FGD ACP",
    ...tccRun(grok),
  });

  /*
   * FGD first-class Hermes profiles.
   */
  agents.register<Config>({
    id: "jimmy",
    title: "FGD Jimmy",
    icon: RobotIcon,
    description:
      "FGD Wiring and Mission Control using the dedicated Jimmy Hermes profile.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Jimmy · Hermes ACP",
    ...tccRun(jimmy),
  });

  agents.register<Config>({
    id: "kenny",
    title: "FGD Kenny",
    icon: RobotIcon,
    description:
      "FGD REMH execution using the dedicated Kenny Hermes profile.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Kenny · Hermes ACP",
    ...tccRun(kenny),
  });

  agents.register<Config>({
    id: "wendy",
    title: "FGD Wendy",
    icon: RobotIcon,
    description:
      "FGD Blockchain Launch Pad execution using the dedicated Wendy Hermes profile.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Wendy · Hermes ACP",
    ...tccRun(wendy),
  });

  agents.register<Config>({
    id: "tweak",
    title: "FGD Tweak",
    icon: RobotIcon,
    description:
      "FGD Credited Work execution using the dedicated Tweak Hermes profile.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Tweak · Hermes ACP",
    ...tccRun(tweak),
  });

  agents.register<Config>({
    id: "pip",
    title: "FGD Pip",
    icon: RobotIcon,
    description:
      "FGD OSRE execution using the dedicated Pip Hermes profile.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Pip · Hermes ACP",
    ...tccRun(pip),
  });

  agents.register<Config>({
    id: "craig",
    title: "FGD Craig",
    icon: RobotIcon,
    description:
      "FGD Craig execution using the dedicated Craig Hermes profile.",
    defaults: () => ({ config: DEFAULT_CONFIG }),
    summary: () => "Craig · Hermes ACP",
    ...tccRun(craig),
  });
};
