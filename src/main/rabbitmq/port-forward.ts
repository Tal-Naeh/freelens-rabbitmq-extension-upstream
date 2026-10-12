import { RabbitmqError } from "../../common/errors";
import type { ChildProcess, SpawnOptions } from "node:child_process";

export interface PodPort {
  namespace: string;
  pod: string;
  port: number;
}

export interface Tunnel {
  readonly localHost: string;
  readonly localPort: number;
  readonly target: PodPort;
  close(): void;
}

/** Opens a local tunnel to a pod port. */
export type TunnelOpener = (target: PodPort) => Promise<Tunnel>;

export interface KubectlTunnelDependencies {
  /** The kubectl Freelens runs for the cluster (`Main.Shell.getKubectlPath`). */
  getKubectlPath(): Promise<string>;
  /** An environment whose kubeconfig reaches the cluster through Freelens (`Main.Shell.getEnvironment`). */
  getEnvironment(): Promise<Record<string, string>>;
  spawn(command: string, args: string[], options: SpawnOptions): ChildProcess;
  readyTimeoutMs?: number;
}

const LOCAL_HOST = "127.0.0.1";
const DEFAULT_READY_TIMEOUT_MS = 15_000;
const FORWARDING_LINE = /^Forwarding from 127\.0\.0\.1:(\d+) ->/m;

// kubectl children still running; killed when the Main process exits so a quit never leaves one behind.
const running = new Set<ChildProcess>();
let exitHookInstalled = false;

function installExitHook(): void {
  if (exitHookInstalled) return;
  exitHookInstalled = true;
  process.once("exit", () => {
    for (const child of running) child.kill();
  });
}

/**
 * {@link TunnelOpener} that runs `kubectl port-forward` with the kubectl and the kubeconfig Freelens itself uses
 * for the cluster, so the tunnel goes through the Freelens proxy like the app's own port-forwards. kubectl picks a
 * free local port; the tunnel is ready once it prints the `Forwarding from` line.
 */
export function createKubectlTunnelOpener(deps: KubectlTunnelDependencies): TunnelOpener {
  const readyTimeoutMs = deps.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS;

  return async (target) => {
    const [kubectl, env] = await Promise.all([deps.getKubectlPath(), deps.getEnvironment()]);
    const args = [
      "port-forward",
      "--address",
      LOCAL_HOST,
      "--namespace",
      target.namespace,
      `pod/${target.pod}`,
      `:${target.port}`,
    ];
    const child = deps.spawn(kubectl, args, { env, stdio: ["ignore", "pipe", "pipe"] });
    running.add(child);
    installExitHook();
    child.once("exit", () => running.delete(child));

    return new Promise<Tunnel>((resolve, reject) => {
      let stdout = "";
      let stderr = "";
      let settled = false;

      const fail = (message: string) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        child.kill();
        reject(new RabbitmqError("unreachable", message));
      };

      const timer = setTimeout(
        () => fail(`kubectl port-forward to ${describe(target)} was not ready after ${readyTimeoutMs} ms`),
        readyTimeoutMs,
      );

      child.stdout?.on("data", (chunk: Buffer) => {
        if (settled) return;
        stdout += chunk.toString();
        const match = FORWARDING_LINE.exec(stdout);
        if (!match) return;
        settled = true;
        clearTimeout(timer);
        resolve({
          localHost: LOCAL_HOST,
          localPort: Number(match[1]),
          target,
          close: () => child.kill(),
        });
      });
      child.stderr?.on("data", (chunk: Buffer) => {
        stderr += chunk.toString();
      });
      child.once("error", (err) => fail(`could not run kubectl port-forward: ${err.message}`));
      child.once("exit", (code, signal) => {
        const reason = stderr.trim() || `exited with ${signal ?? `code ${code}`}`;
        fail(`kubectl port-forward to ${describe(target)} failed: ${reason}`);
      });
    });
  };
}

function describe(target: PodPort): string {
  return `${target.namespace}/${target.pod}:${target.port}`;
}
