# std.Io.RwLock cancelation hang reproducer

Not part of the xsync library. This exists because ziglang/zig CI hit

```
error: 'Io.RwLock.test.canceled writer does not leak a semaphore permit'
       timed out after 2m1.047ms
```

on `std-aarch64-maccatalyst-none-apple_m1-debug`, on branch `rwlock-fixes`
(candidate fixes for ziglang#36178 and ziglang#36217, stacked on the
ziglang#35564 Condition fix). The test takes ~0.3s on x86_64-linux, so 2
minutes is a hang, not slowness. This repo has a macOS runner we can hammer
without burning zig's.

`std_condition.zig`, `std_semaphore.zig` and `std_rwlock.zig` are copies of the
std sources from that branch, rewired to import each other. They deliberately
do **not** use xsync's primitives — xsync's Condition is parker-based, so it
would not exercise the code under suspicion. What they do use from std is
`Io.Mutex` and `Threaded`'s `futexWait`/`futexWaitUncancelable`, which is where
the platform-specific behavior lives.

## What is suspected

The #36178 fix makes a canceled writer perform a *blocking uncancelable wait
inside a cancelation handler* (`semaphore.waitUncancelable` while the task
status is already `canceled`). Nothing else in std does that for more than a
mutex hand-off. In `Threaded.futexWaitInner`, uncancelable means the wait is
never registered with the cancelation machinery (`.{ .thread = null }` instead
of `try Syscall.start()`), and on Darwin it then parks in `__ulock_wait2`.

## Running

```
zig build -Drepro-rounds=2000 -Drepro-iterations=10000
./zig-out/bin/repro
```

Options:

- `-Dfix-36178=false` — control arm: drop the cancel-path fix. The permit leak
  is then expected and merely counted, so this arm runs as long as the fixed
  one and tells us whether a hang predates the fix or is caused by it.
- `-Drepro-rounds`, `-Drepro-iterations`.

A watchdog task dumps `rl.state`, both mutexes, `semaphore.permits` and the
Condition's `waiters`/`signals`/`epoch` after a 20s stall and exits 1. That
dump is the point of the exercise: it says which invariant broke, which a bare
CI timeout does not.

Self-checks performed locally on x86_64-linux:

- `-Dfix-36178=false` reports leaked permits within a few hundred iterations.
- The watchdog was validated by injecting a deliberate hang.
- The fixed build passes 2000x10000 without stalling.
