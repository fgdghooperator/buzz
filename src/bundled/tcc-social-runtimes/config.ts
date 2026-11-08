export type Scope = "thread" | "channel";

export type Config = Readonly<{
  instructions: string;
  workspace: string;
  scope: Scope;
}>;

export const DEFAULT_CONFIG: Config = Object.freeze({
  instructions: "",
  workspace: "~/.buzz",
  scope: "thread",
});

export const config = (value: unknown): Config => ({
  ...DEFAULT_CONFIG,
  ...(value && typeof value === "object" ? (value as Partial<Config>) : {}),
});
