import { AcpProcess } from "../../features/agent-harness/acp/process";
import type {
  AcpHooks,
  ServeMcp,
  Spawn,
} from "../../features/agent-harness/acp/types";

export type AcpHost = Readonly<{
  spawn: Spawn;
  resolveDirectory(path: string): Promise<string>;
}>;

const hermesProfileHooks = (
  label: string,
  program: string,
): AcpHooks => ({
  label,
  program,
  missing: `The ${label} Hermes ACP runtime is unavailable.`,
  requiresHttpMcp: false,
  sharedMcpSession: true,
  env: {
    HERMES_ACP_SKIP_CONFIGURED_MCP: "1",
  },
});

export const TIMMY_HOOKS: AcpHooks = {
  label: "Timmy",
  program: "tcc-timmy-acp",
  missing: "The TCC Timmy Hermes ACP runtime is unavailable.",
  requiresHttpMcp: false,
  sharedMcpSession: true,
  env: {
    HERMES_ACP_SKIP_CONFIGURED_MCP: "1",
  },
};

export const GROK_HOOKS: AcpHooks = {
  label: "Grok",
  program: "tcc-grok-acp",
  missing: "The FGD Grok ACP runtime is unavailable.",
  env: {},
};

export const JIMMY_HOOKS = hermesProfileHooks(
  "FGD Jimmy",
  "fgd-jimmy-acp",
);

export const KENNY_HOOKS = hermesProfileHooks(
  "FGD Kenny",
  "fgd-kenny-acp",
);

export const WENDY_HOOKS = hermesProfileHooks(
  "FGD Wendy",
  "fgd-wendy-acp",
);

export const TWEAK_HOOKS = hermesProfileHooks(
  "FGD Tweak",
  "fgd-tweak-acp",
);

export const PIP_HOOKS = hermesProfileHooks(
  "FGD Pip",
  "fgd-pip-acp",
);

export const CRAIG_HOOKS = hermesProfileHooks(
  "FGD Craig",
  "fgd-craig-acp",
);

export class SharedAcpProcess {
  private ready: Promise<AcpProcess> | undefined;

  constructor(
    private readonly host: AcpHost,
    private readonly hooks: AcpHooks,
    private readonly serve: ServeMcp,
  ) {}

  get(): Promise<AcpProcess> {
    if (this.ready) return this.ready;

    const ready = (async () =>
      AcpProcess.start(this.host.spawn, this.hooks, {
        cwd: await this.host.resolveDirectory("~"),
        serve: this.serve,
      }))();

    const forget = () => {
      if (this.ready === ready) this.ready = undefined;
    };

    void ready.then((process) => process.exited.then(forget), forget);
    this.ready = ready;

    return ready;
  }

  stop() {
    const ready = this.ready;
    this.ready = undefined;
    void ready?.then((process) => process.kill()).catch(() => undefined);
  }
}
