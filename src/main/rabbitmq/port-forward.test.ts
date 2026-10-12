import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import { createKubectlTunnelOpener } from "./port-forward";
import type { ChildProcess, SpawnOptions } from "node:child_process";

import type { KubectlTunnelDependencies } from "./port-forward";

class FakeChild extends EventEmitter {
  readonly stdout = new PassThrough();
  readonly stderr = new PassThrough();
  readonly kill = vi.fn(() => {
    this.emit("exit", null, "SIGTERM");
    return true;
  });
}

function setup(overrides: Partial<KubectlTunnelDependencies> = {}) {
  const child = new FakeChild();
  const calls: { command: string; args: string[]; options: SpawnOptions }[] = [];
  const open = createKubectlTunnelOpener({
    getKubectlPath: async () => "/opt/freelens/kubectl",
    getEnvironment: async () => ({ KUBECONFIG: "/tmp/proxy-kubeconfig" }),
    spawn: (command, args, options) => {
      calls.push({ command, args, options });
      return child as unknown as ChildProcess;
    },
    ...overrides,
  });
  return { child, calls, open };
}

const target = { namespace: "shop", pod: "orders-server-0", port: 15672 };

describe("createKubectlTunnelOpener", () => {
  it("runs the host kubectl with the Freelens environment and resolves on the Forwarding line", async () => {
    const { child, calls, open } = setup();
    const opening = open(target);
    await vi.waitFor(() => expect(calls).toHaveLength(1));

    expect(calls[0]?.command).toBe("/opt/freelens/kubectl");
    expect(calls[0]?.args).toEqual([
      "port-forward",
      "--address",
      "127.0.0.1",
      "--namespace",
      "shop",
      "pod/orders-server-0",
      ":15672",
    ]);
    expect(calls[0]?.options.env).toEqual({ KUBECONFIG: "/tmp/proxy-kubeconfig" });

    child.stdout.write("Forwarding from 127.0.0.1:");
    child.stdout.write("54321 -> 15672\n");
    const tunnel = await opening;
    expect(tunnel).toMatchObject({ localHost: "127.0.0.1", localPort: 54321, target });

    tunnel.close();
    expect(child.kill).toHaveBeenCalledOnce();
  });

  it("rejects as unreachable with kubectl's stderr when it exits before forwarding", async () => {
    const { child, calls, open } = setup();
    const opening = open(target);
    await vi.waitFor(() => expect(calls).toHaveLength(1));

    child.stderr.write('error: pods "orders-server-0" is forbidden: cannot create resource "pods/portforward"\n');
    await new Promise((r) => setImmediate(r));
    child.emit("exit", 1, null);

    await expect(opening).rejects.toMatchObject({
      code: "unreachable",
      message: expect.stringContaining("pods/portforward"),
    });
  });

  it("kills kubectl and rejects when it is not ready in time", async () => {
    const { child, calls, open } = setup({ readyTimeoutMs: 20 });

    await expect(open(target)).rejects.toMatchObject({
      code: "unreachable",
      message: expect.stringContaining("not ready"),
    });
    expect(calls).toHaveLength(1);
    expect(child.kill).toHaveBeenCalled();
  });

  it("rejects when kubectl cannot be started", async () => {
    const { child, calls, open } = setup();
    const opening = open(target);
    await vi.waitFor(() => expect(calls).toHaveLength(1));

    child.emit("error", new Error("spawn ENOENT"));
    await expect(opening).rejects.toMatchObject({ code: "unreachable", message: expect.stringContaining("ENOENT") });
  });

  it("rejects when the host has no kubectl for the cluster", async () => {
    const { calls, open } = setup({
      getKubectlPath: async () => {
        throw new Error("cluster is not connected");
      },
    });
    await expect(open(target)).rejects.toThrow("cluster is not connected");
    expect(calls).toHaveLength(0);
  });
});
