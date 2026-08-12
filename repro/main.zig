//! Reproducer for the aarch64-macos hang seen in ziglang/zig CI on the
//! `rwlock-fixes` branch:
//!
//!   error: 'Io.RwLock.test.canceled writer does not leak a semaphore permit'
//!          timed out after 2m1.047ms
//!
//! It runs against verbatim copies of `std.Io.Condition`, `std.Io.Semaphore`
//! and `std.Io.RwLock` (see the sibling files), so it exercises std's code on
//! std's `Threaded` futex, not xsync's parker-based primitives. It lives here
//! only because this repo has a macOS runner we can hammer without burning
//! zig's CI.
//!
//! On a stall the watchdog dumps every relevant word instead of just hanging,
//! which is the whole point: a CI timeout tells us nothing, but knowing
//! whether `permits` is 0 or 1, and whether the Condition has a waiter with no
//! signal, tells us which invariant broke.

const std = @import("std");
const Io = std.Io;

const RwLock = @import("std_rwlock.zig");
const build_options = @import("build_options");

const rounds = build_options.rounds;
const iterations = build_options.iterations;

/// Bumped once per iteration; the watchdog watches this for liveness.
var progress: std.atomic.Value(u64) = .init(0);
/// Names the scenario currently running, for the stall dump.
var phase: std.atomic.Value(u8) = .init(0);
/// Permits leaked in the control arm (`-Dfix-36178=false`), where the leak is expected.
var leaks: u64 = 0;

const stall_seconds = 20;

fn dumpAndDie(rl: *RwLock) noreturn {
    const state = @atomicLoad(usize, &rl.state, .seq_cst);
    const cond_state = rl.semaphore.cond.state.load(.seq_cst);

    std.debug.print(
        \\
        \\=== STALLED after {d}s ===
        \\phase:                {d}
        \\iteration:            {d}
        \\fix_36178:            {}
        \\
        \\rl.state:             0x{x}
        \\  is_writing:         {d}
        \\  writers:            {d}
        \\  readers:            {d}
        \\rl.mutex.state:       {t}
        \\
        \\semaphore.permits:    {d}
        \\semaphore.mutex:      {t}
        \\condition.waiters:    {d}
        \\condition.signals:    {d}
        \\condition.epoch:      {d}
        \\
        \\in uncancelable wait: {}
        \\
    , .{
        stall_seconds,
        phase.load(.monotonic),
        progress.load(.monotonic),
        RwLock.fix_36178,
        state,
        state & 1,
        (state & RwLock.writer_mask) >> 1,
        state / RwLock.reader,
        rl.mutex.state.load(.seq_cst),
        rl.semaphore.permits,
        rl.semaphore.mutex.state.load(.seq_cst),
        cond_state.waiters,
        cond_state.signals,
        rl.semaphore.cond.epoch.load(.seq_cst),
        RwLock.waiting_uncancelable.load(.monotonic),
    });
    std.process.exit(1);
}

fn watchdog(rl: *RwLock, io: Io) void {
    var last: u64 = 0;
    var stalled: u64 = 0;
    while (true) {
        io.sleep(.fromSeconds(1), .awake) catch return;
        const now = progress.load(.monotonic);
        if (now != last) {
            last = now;
            stalled = 0;
            continue;
        }
        stalled += 1;
        if (stalled >= stall_seconds) dumpAndDie(rl);
    }
}

fn lockUnlock(rl: *RwLock, io: Io) Io.Cancelable!void {
    try rl.lock(io);
    rl.unlock(io);
}

/// ziglang#36178: race the last reader's `post` against cancelation of a
/// writer that is waiting for readers to drain.
fn canceledWriterPermit(rl: *RwLock, io: Io, n: u64) !void {
    phase.store(1, .monotonic);

    for (0..n) |i| {
        progress.store(i, .monotonic);

        rl.lockSharedUncancelable(io);

        var wfuture = try io.concurrent(lockUnlock, .{ rl, io });
        var rfuture = try io.concurrent(RwLock.unlockShared, .{ rl, io });

        wfuture.cancel(io) catch {};
        rfuture.await(io);

        const state = @atomicLoad(usize, &rl.state, .seq_cst);
        if (state != 0) {
            std.debug.print("iteration {d}: state 0x{x}, expected 0\n", .{ i, state });
            return error.StateNotZero;
        }
        if (rl.semaphore.permits != 0) {
            if (RwLock.fix_36178) {
                std.debug.print("iteration {d}: permits {d}, expected 0\n", .{ i, rl.semaphore.permits });
                return error.PermitLeaked;
            }
            // Control arm: without the fix the leak is the *expected* outcome, and we
            // care whether this build also hangs. Drain and keep going so it runs as
            // long as the fixed build does.
            leaks += 1;
            rl.semaphore.permits = 0;
        }
    }
}

const Readers = struct {
    rl: *RwLock,
    stop: std.atomic.Value(bool) = .init(false),

    fn run(r: *Readers, io: Io) void {
        while (!r.stop.load(.monotonic)) {
            if (r.rl.tryLockShared(io)) r.rl.unlockShared(io);
        }
    }
};

/// ziglang#36217: `tryLock` must not admit a writer while a reader is inside.
fn tryLockRace(rl: *RwLock, io: Io, n: u64) !void {
    phase.store(2, .monotonic);

    var readers: Readers = .{ .rl = rl };
    var future = try io.concurrent(Readers.run, .{ &readers, io });
    defer {
        readers.stop.store(true, .monotonic);
        future.await(io);
    }

    for (0..n) |i| {
        progress.store(i, .monotonic);

        if (!rl.tryLock(io)) continue;
        defer rl.unlock(io);

        const state = @atomicLoad(usize, &rl.state, .seq_cst);
        if (state & RwLock.reader_mask != 0) {
            std.debug.print("iteration {d}: reader inside exclusive lock, state 0x{x}\n", .{ i, state });
            return error.ReaderRacedIn;
        }
    }

    if (rl.semaphore.permits != 0) {
        std.debug.print("permits {d} after tryLock race, expected 0\n", .{rl.semaphore.permits});
        return error.PermitLeaked;
    }
}

pub fn main() !void {
    const gpa = std.heap.smp_allocator;

    var threaded: Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    std.debug.print("fix_36178={}, rounds={d}, iterations={d}\n", .{ RwLock.fix_36178, rounds, iterations });

    var rl: RwLock = .init;

    var wd = try io.concurrent(watchdog, .{ &rl, io });
    defer wd.cancel(io);

    for (0..rounds) |round| {
        try canceledWriterPermit(&rl, io, iterations);
        try tryLockRace(&rl, io, iterations);
        std.debug.print("round {d}/{d} ok\n", .{ round + 1, rounds });
    }

    if (!RwLock.fix_36178) std.debug.print("control arm: {d} leaked permits (expected non-zero)\n", .{leaks});
    std.debug.print("all rounds passed\n", .{});
}
