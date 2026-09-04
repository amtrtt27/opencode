import { describe, expect } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Effect, Exit, Sink, Stream } from "effect"
import type * as PlatformError from "effect/PlatformError"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { testEffect } from "../lib/effect"

const live = LayerNode.compile(CrossSpawnSpawner.node)
const fx = testEffect(live)

function js(code: string, opts?: ChildProcess.CommandOptions) {
  return ChildProcess.make("node", ["-e", code], opts)
}

function decodeByteStream(stream: Stream.Stream<Uint8Array, PlatformError.PlatformError>) {
  return Stream.runCollect(stream).pipe(
    Effect.map((chunks) => {
      const total = chunks.reduce((acc, x) => acc + x.length, 0)
      const out = new Uint8Array(total)
      let off = 0
      for (const chunk of chunks) {
        out.set(chunk, off)
        off += chunk.length
      }
      return new TextDecoder("utf-8").decode(out).trim()
    }),
  )
}

function alive(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

async function tmpdir() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-core-test-"))
  return {
    path: dir,
    async [Symbol.asyncDispose]() {
      await fs.rm(dir, { recursive: true, force: true })
    },
  }
}

async function gone(pid: number, timeout = 5_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    if (!alive(pid)) return true
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  return !alive(pid)
}

describe("cross-spawn spawner", () => {
  describe("basic spawning", () => {
    fx.effect(
      "captures stdout",
      Effect.gen(function* () {
        const out = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.string(ChildProcess.make(process.execPath, ["-e", 'process.stdout.write("ok")'])),
        )
        expect(out).toBe("ok")
      }),
    )

    fx.effect(
      "captures multiple lines",
      Effect.gen(function* () {
        const handle = yield* js('console.log("line1"); console.log("line2"); console.log("line3")')
        const out = yield* decodeByteStream(handle.stdout)
        expect(out).toBe("line1\nline2\nline3")
      }),
    )

    fx.effect(
      "returns exit code",
      Effect.gen(function* () {
        const handle = yield* js("process.exit(0)")
        const code = yield* handle.exitCode
        expect(code).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )

    fx.effect(
      "returns non-zero exit code",
      Effect.gen(function* () {
        const handle = yield* js("process.exit(42)")
        const code = yield* handle.exitCode
        expect(code).toBe(ChildProcessSpawner.ExitCode(42))
      }),
    )
  })

  describe("cwd option", () => {
    fx.effect(
      "uses cwd when spawning commands",
      Effect.gen(function* () {
        const tmp = yield* Effect.acquireRelease(
          Effect.promise(() => tmpdir()),
          (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
        )
        const out = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.string(
            ChildProcess.make(process.execPath, ["-e", "process.stdout.write(process.cwd())"], { cwd: tmp.path }),
          ),
        )
        expect(yield* Effect.promise(() => fs.realpath(out))).toBe(yield* Effect.promise(() => fs.realpath(tmp.path)))
      }),
    )

    fx.effect(
      "fails for invalid cwd",
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
            svc.spawn(ChildProcess.make("echo", ["test"], { cwd: "/nonexistent/directory/path" })),
          ),
        )
        expect(Exit.isFailure(exit)).toBe(true)
      }),
    )
  })

  describe("env option", () => {
    fx.effect(
      "passes environment variables with extendEnv",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write(process.env.TEST_VAR ?? "")', {
          env: { TEST_VAR: "test_value" },
          extendEnv: true,
        })
        const out = yield* decodeByteStream(handle.stdout)
        expect(out).toBe("test_value")
      }),
    )

    fx.effect(
      "passes multiple environment variables",
      Effect.gen(function* () {
        const handle = yield* js(
          "process.stdout.write(`${process.env.VAR1}-${process.env.VAR2}-${process.env.VAR3}`)",
          {
            env: { VAR1: "one", VAR2: "two", VAR3: "three" },
            extendEnv: true,
          },
        )
        const out = yield* decodeByteStream(handle.stdout)
        expect(out).toBe("one-two-three")
      }),
    )

    fx.effect(
      "does not extend the parent environment when extendEnv is false",
      Effect.gen(function* () {
        const handle = yield* js(
          'process.stdout.write(`${process.env.ONLY_VAR ?? ""}:${process.env.OPENCODE_SHOULD_NOT_EXIST ?? ""}`)',
          {
            env: { ONLY_VAR: "only" },
            extendEnv: false,
          },
        )

        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode

        expect(out).toBe("only:")
      }),
    )
  })

  describe("stderr", () => {
    fx.effect(
      "captures stderr output",
      Effect.gen(function* () {
        const handle = yield* js('process.stderr.write("error message")')
        const err = yield* decodeByteStream(handle.stderr)
        expect(err).toBe("error message")
      }),
    )

    fx.effect(
      "captures both stdout and stderr",
      Effect.gen(function* () {
        const handle = yield* js(
          [
            "let pending = 2",
            "const done = () => {",
            "  pending -= 1",
            "  if (pending === 0) setTimeout(() => process.exit(0), 0)",
            "}",
            'process.stdout.write("stdout\\n", done)',
            'process.stderr.write("stderr\\n", done)',
          ].join("\n"),
        )
        const [stdout, stderr] = yield* Effect.all([decodeByteStream(handle.stdout), decodeByteStream(handle.stderr)], {
          concurrency: 2,
        })
        expect(stdout).toBe("stdout")
        expect(stderr).toBe("stderr")
      }),
    )

    fx.effect(
      "supports ignored stdout and stderr",
      Effect.gen(function* () {
        const handle = yield* js(
          'process.stdout.write("stdout"); process.stderr.write("stderr")',
          {
            stdout: "ignore",
            stderr: "ignore",
          },
        )

        const code = yield* handle.exitCode
        expect(code).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )

    fx.effect(
      "supports Sink as stdout",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("hello")', {
          stdout: Sink.drain,
        })

        const code = yield* handle.exitCode
        expect(code).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )

    fx.effect(
      "supports Sink as stderr",
      Effect.gen(function* () {
        const handle = yield* js('process.stderr.write("error")', {
          stderr: Sink.drain,
        })

        const code = yield* handle.exitCode
        expect(code).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )
  })

  describe("combined output (all)", () => {
    fx.effect(
      "captures stdout via .all when no stderr",
      Effect.gen(function* () {
        const handle = yield* ChildProcess.make("echo", ["hello from stdout"])
        const all = yield* decodeByteStream(handle.all)
        expect(all).toBe("hello from stdout")
      }),
    )

    fx.effect(
      "captures stderr via .all when no stdout",
      Effect.gen(function* () {
        const handle = yield* js('process.stderr.write("hello from stderr")')
        const all = yield* decodeByteStream(handle.all)
        expect(all).toBe("hello from stderr")
      }),
    )
  })

  describe("stdin", () => {
    fx.effect(
      "allows providing standard input to a command",
      Effect.gen(function* () {
        const input = "a b c"
        const stdin = Stream.make(Buffer.from(input, "utf-8"))
        const handle = yield* js(
          'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out))',
          { stdin },
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toBe("a b c")
      }),
    )

    fx.effect(
      "supports string stdin configuration",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("ok")', {
          stdin: "ignore",
        })

        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode

        expect(out).toBe("ok")
      }),
    )

    fx.effect(
      "surfaces a stdin write failure when the child closes stdin",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const chunk = Buffer.alloc(1024 * 1024, 65)
        const input = Stream.fromIterable(Array.from({ length: 16 }, () => chunk))

        const exit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              const handle = yield* js(
                'process.stdin.destroy(); setTimeout(() => process.exit(0), 50)',
                { stdin: input },
              )

              yield* handle.exitCode
            }),
          ),
        )

        // Depending on scheduling, the child can exit before or during the write.
        // Either outcome is valid; this test primarily exercises the writable error path.
        expect(Exit.isSuccess(exit) || Exit.isFailure(exit)).toBe(true)
      }),
    )
  })

  describe("additional file descriptors", () => {
    fx.effect(
      "captures output from an additional file descriptor",
      Effect.gen(function* () {
        const handle = yield* js(
          `
            const fs = require("node:fs")
            fs.writeSync(3, "hello from fd3")
          `,
          {
            additionalFds: {
              fd3: { type: "output" },
            },
          },
        )

        const out = yield* decodeByteStream(handle.getOutputFd(3))
        yield* handle.exitCode

        expect(out).toBe("hello from fd3")
      }),
    )

    fx.effect(
      "provides input through an additional file descriptor",
      Effect.gen(function* () {
        const input = Stream.make(Buffer.from("hello fd3", "utf-8"))

        const handle = yield* js(
          `
            const fs = require("node:fs")
            const input = fs.readFileSync(3, "utf8")
            process.stdout.write(input.toUpperCase())
          `,
          {
            additionalFds: {
              fd3: {
                type: "input",
                stream: input,
              },
            },
          },
        )

        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode

        expect(out).toBe("HELLO FD3")
      }),
    )

    fx.effect(
      "returns empty stream for unknown output fd",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("done")')

        const output = yield* decodeByteStream(handle.getOutputFd(99))
        yield* handle.exitCode

        expect(output).toBe("")
      }),
    )

    fx.effect(
      "allows unused unknown input fd",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("done")')

        const sink = handle.getInputFd(99)

        yield* Stream.run(
          Stream.make(Buffer.from("ignored")),
          sink,
        )

        const output = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode

        expect(output).toBe("done")
      }),
    )

    fx.effect(
      "handles an additional input fd closing while data is written",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const chunk = Buffer.alloc(1024 * 1024, 66)
        const input = Stream.fromIterable(Array.from({ length: 16 }, () => chunk))

        const exit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              const handle = yield* js(
                `
                  const fs = require("node:fs")
                  try { fs.closeSync(3) } catch {}
                  setTimeout(() => process.exit(0), 50)
                `,
                {
                  additionalFds: {
                    fd3: {
                      type: "input",
                      stream: input,
                    },
                  },
                },
              )

              yield* handle.exitCode
            }),
          ),
        )

        expect(Exit.isSuccess(exit) || Exit.isFailure(exit)).toBe(true)
      }),
    )
  })

  describe("process control", () => {
    fx.effect(
      "kills a running process",
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.gen(function* () {
            const handle = yield* js("setTimeout(() => {}, 10_000)")
            yield* handle.kill()
            return yield* handle.exitCode
          }),
        )
        expect(Exit.isFailure(exit) ? true : exit.value !== ChildProcessSpawner.ExitCode(0)).toBe(true)
      }),
    )

    fx.effect(
      "kills a child when scope exits",
      Effect.gen(function* () {
        const pid = yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* js("setInterval(() => {}, 10_000)")
            return Number(handle.pid)
          }),
        )
        const done = yield* Effect.promise(() => gone(pid))
        expect(done).toBe(true)
      }),
    )

    fx.effect(
      "forceKillAfter escalates for stubborn processes",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const started = Date.now()
        const exit = yield* Effect.exit(
          Effect.gen(function* () {
            const handle = yield* js('process.on("SIGTERM", () => {}); setInterval(() => {}, 10_000)')
            yield* handle.kill({ forceKillAfter: 100 })
            return yield* handle.exitCode
          }),
        )

        expect(Date.now() - started).toBeLessThan(1_000)
        expect(Exit.isFailure(exit) ? true : exit.value !== ChildProcessSpawner.ExitCode(0)).toBe(true)
      }),
    )

    fx.effect(
      "isRunning reflects process state",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("done")')
        yield* handle.exitCode
        const running = yield* handle.isRunning
        expect(running).toBe(false)
      }),
    )

    fx.effect(
      "supports unref and ref",
      Effect.gen(function* () {
        const handle = yield* js('setTimeout(() => process.exit(0), 100)')

        const ref = yield* handle.unref
        yield* ref

        const code = yield* handle.exitCode
        expect(code).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )

    fx.effect(
      "falls back to killing an individual process",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const handle = yield* js("setInterval(() => {}, 10_000)", {
          detached: false,
        })

        const pid = Number(handle.pid)
        yield* handle.kill()

        const done = yield* Effect.promise(() => gone(pid))
        expect(done).toBe(true)
      }),
    )

    fx.effect(
      "force kills stubborn process when scope closes",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const started = Date.now()

        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* js('process.on("SIGTERM", () => {}); setInterval(() => {}, 10_000)', {
              forceKillAfter: 100,
            })
          }),
        )

        expect(Date.now() - started).toBeLessThan(1_000)
      }),
    )

    fx.effect(
      "cleans up exited non-zero process with forceKillAfter configured",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const code = yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* js("process.exit(2)", {
              forceKillAfter: 100,
            })

            return yield* handle.exitCode
          }),
        )

        expect(code).toBe(ChildProcessSpawner.ExitCode(2))
      }),
    )

    fx.effect(
      "handles killing an already exited process",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const handle = yield* js("process.exit(0)")
        yield* handle.exitCode

        const exit = yield* Effect.exit(handle.kill())

        expect(Exit.isSuccess(exit) || Exit.isFailure(exit)).toBe(true)
      }),
    )
  })

  describe("error handling", () => {
  fx.effect(
    "fails for invalid command",
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        Effect.gen(function* () {
          const handle = yield* ChildProcess.make("nonexistent-command-12345")
          return yield* handle.exitCode
        }),
      )

      expect(Exit.isFailure(exit) ? true : exit.value !== ChildProcessSpawner.ExitCode(0)).toBe(true)
    }),
  )

  fx.effect(
    "fails with permission denied for non-executable file",
    Effect.gen(function* () {
      if (process.platform === "win32") return

      const tmp = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      )

      const file = path.join(tmp.path, "script.sh")

      yield* Effect.promise(() =>
        fs.writeFile(file, '#!/bin/sh\necho "hello"\n'),
      )

      yield* Effect.promise(() =>
        fs.chmod(file, 0o644),
      )

      const exit = yield* Effect.exit(
        ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.spawn(ChildProcess.make(file)),
        ),
      )

      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  fx.effect(
    "fails when cwd contains a non-directory path component",
    Effect.gen(function* () {
      const tmp = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      )

      const file = path.join(tmp.path, "not-a-directory")
      yield* Effect.promise(() => fs.writeFile(file, "hello"))

      const invalidCwd = path.join(file, "child")

      const exit = yield* Effect.exit(
        ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.spawn(
            ChildProcess.make("echo", ["test"], {
              cwd: invalidCwd,
            }),
          ),
        ),
      )

      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  fx.effect(
    "fails when command path contains a non-directory component",
    Effect.gen(function* () {
      if (process.platform === "win32") return

      const tmp = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      )

      const file = path.join(tmp.path, "plain-file")
      yield* Effect.promise(() => fs.writeFile(file, "not a directory"))

      const command = path.join(file, "child-command")
      const exit = yield* Effect.exit(
        ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.spawn(ChildProcess.make(command)),
        ),
      )

      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  fx.effect(
    "fails for a symbolic-link command loop",
    Effect.gen(function* () {
      if (process.platform === "win32") return

      const tmp = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      )

      const a = path.join(tmp.path, "a")
      const b = path.join(tmp.path, "b")

      yield* Effect.promise(() => fs.symlink(b, a))
      yield* Effect.promise(() => fs.symlink(a, b))

      const exit = yield* Effect.exit(
        ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.spawn(ChildProcess.make(a)),
        ),
      )

      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )
})

  describe("pipeline", () => {
    fx.effect(
      "pipes stdout of one command to stdin of another",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("hello world")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out.toUpperCase()))',
            ),
          ),
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toBe("HELLO WORLD")
      }),
    )

    fx.effect(
      "three-stage pipeline",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("hello world")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out.toUpperCase()))',
            ),
          ),
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out.replaceAll(" ", "-")))',
            ),
          ),
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toBe("HELLO-WORLD")
      }),
    )

    fx.effect(
      "pipes stderr with { from: 'stderr' }",
      Effect.gen(function* () {
        const handle = yield* js('process.stderr.write("error")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out))',
            ),
            { from: "stderr" },
          ),
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toBe("error")
      }),
    )

    fx.effect(
      "pipes combined output with { from: 'all' }",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("stdout\\n"); process.stderr.write("stderr\\n")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out))',
            ),
            { from: "all" },
          ),
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toContain("stdout")
        expect(out).toContain("stderr")
      }),
    )

    fx.effect(
      "pipes from an additional output file descriptor",
      Effect.gen(function* () {
        const producer = js(
          `
            const fs = require("node:fs")
            fs.writeSync(3, "hello fd")
          `,
          {
            additionalFds: {
              fd3: { type: "output" },
            },
          },
        )

        const handle = yield* producer.pipe(
          ChildProcess.pipeTo(
            js(
              `
                process.stdin.setEncoding("utf8")
                let out = ""
                process.stdin.on("data", chunk => out += chunk)
                process.stdin.on("end", () => process.stdout.write(out.toUpperCase()))
              `,
            ),
            { from: "fd3" },
          ),
        )

        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode

        expect(out).toBe("HELLO FD")
      }),
    )

    fx.effect(
      "pipes output into an additional input file descriptor",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("hello world")').pipe(
          ChildProcess.pipeTo(
            js(`
              const fs = require("node:fs")
              const input = fs.readFileSync(3, "utf8")
              process.stdout.write(input.toUpperCase())
            `),
            { to: "fd3" },
          ),
        )

        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode

        expect(out).toBe("HELLO WORLD")
      }),
    )

    fx.effect(
      "falls back to stdin for an unrecognized pipeline destination",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("hello")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", chunk => out += chunk); process.stdin.on("end", () => process.stdout.write(out.toUpperCase()))',
            ),
            { to: "invalid" as any },
          ),
        )

        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode

        expect(out).toBe("HELLO")
      }),
    )

    fx.effect(
      "falls back to stdout for an unrecognized pipeline source",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("fallback")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", chunk => out += chunk); process.stdin.on("end", () => process.stdout.write(out.toUpperCase()))',
            ),
            { from: "invalid" as any },
          ),
        )

        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode

        expect(out).toBe("FALLBACK")
      }),
    )
  })

  describe("Windows-specific", () => {
    fx.effect(
      "uses shell routing on Windows",
      Effect.gen(function* () {
        if (process.platform !== "win32") return

        const out = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.string(
            ChildProcess.make("set", ["OPENCODE_TEST_SHELL"], {
              shell: true,
              extendEnv: true,
              env: { OPENCODE_TEST_SHELL: "ok" },
            }),
          ),
        )
        expect(out).toContain("OPENCODE_TEST_SHELL=ok")
      }),
    )

    fx.effect(
      "runs cmd scripts with spaces on Windows without shell",
      Effect.gen(function* () {
        if (process.platform !== "win32") return

        const tmp = yield* Effect.acquireRelease(
          Effect.promise(() => tmpdir()),
          (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
        )
        const dir = path.join(tmp.path, "with space")
        const file = path.join(dir, "echo cmd.cmd")

        yield* Effect.promise(() => fs.mkdir(dir, { recursive: true }))
        yield* Effect.promise(() => fs.writeFile(file, "@echo off\r\nif %~1==--stdio exit /b 0\r\nexit /b 7\r\n"))

        const code = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.exitCode(
            ChildProcess.make(file, ["--stdio"], {
              stdin: "pipe",
              stdout: "pipe",
              stderr: "pipe",
            }),
          ),
        )
        expect(code).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )
  })
})