import { describe, expect, it, vi } from "vitest";
import type { AcpProcess } from "./process";
import { AcpSession } from "./session";

const sessionWith = (
  prompt: (session: AcpSession) => Promise<{ stopReason?: string }>,
) => {
  let session!: AcpSession;
  const acp = {
    hooks: { label: "Harness" },
    request: vi.fn(() => prompt(session)),
  } as unknown as AcpProcess;
  session = new AcpSession(acp, "session-1");
  return { acp, session };
};

const write = (session: AcpSession, text: string) =>
  session.update({
    sessionUpdate: "agent_message_chunk",
    content: { text },
  });

describe("ACP turn finalisation", () => {
  it("reports a genuine failure before any durable mutation", async () => {
    const { session } = sessionWith(async () => {
      throw new Error("provider failed");
    });

    await expect(session.send("do it")).resolves.toEqual({
      ok: false,
      error: "provider failed",
    });
  });

  it("returns the normal response after a durable mutation and successful finalisation", async () => {
    const { session } = sessionWith(async (active) => {
      active.committed();
      write(active, "Complete");
      return { stopReason: "end_turn" };
    });

    await expect(session.send("do it")).resolves.toEqual({
      ok: true,
      text: "Complete",
    });
  });

  it("keeps durable success authoritative when response finalisation fails", async () => {
    const { session } = sessionWith(async (active) => {
      active.committed();
      throw new Error("Internal error");
    });

    await expect(session.send("do it")).resolves.toEqual({
      ok: true,
      text: "⚠️ The action completed, but I couldn't finish presenting the response. Durable state is authoritative; I'll resume from it on the next turn.",
    });
  });

  it("does not replace an already published BLOCKED, COMPLETE, or handoff result", async () => {
    for (const result of ["BLOCKED", "COMPLETE", "HANDOFF"]) {
      const { session } = sessionWith(async (active) => {
        active.answered();
        active.finished();
        throw new Error("Internal error");
      });

      await expect(session.send(result)).resolves.toEqual({ ok: true });
    }
  });

  it("does not add a failure result after a public result already exists", async () => {
    const { session } = sessionWith(async (active) => {
      active.answered();
      throw new Error("Internal error");
    });

    await expect(session.send("publish once")).resolves.toEqual({ ok: true });
  });

  it("fails closed when a provider error leaves the mutation outcome unknown", async () => {
    const { session } = sessionWith(async (active) => {
      write(active, "partial response");
      throw new Error("outcome unknown");
    });

    await expect(session.send("unknown mutation")).resolves.toEqual({
      ok: false,
      error: "outcome unknown",
      text: "partial response",
    });
  });

  it("does not retry a committed action on the next turn", async () => {
    let calls = 0;
    const { session } = sessionWith(async (active) => {
      calls++;
      if (calls === 1) {
        active.committed();
        throw new Error("Internal error");
      }
      write(active, "Resumed from durable state");
      return { stopReason: "end_turn" };
    });

    await session.send("mutate once");
    await expect(session.send("resume")).resolves.toEqual({
      ok: true,
      text: "Resumed from durable state",
    });
    expect(calls).toBe(2);
  });
});
