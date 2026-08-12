//! The same two scenarios as `main.zig`, but shaped exactly like the tests in
//! ziglang/zig's `lib/std/Io/RwLock.zig`: run by the std test runner, on
//! `std.testing.io` rather than a `Threaded` we build ourselves.
//!
//! `main.zig` failed to reproduce the aarch64-macos hang over 20M iterations,
//! so the harness itself is the next suspect: the test runner's io is shared
//! across the whole binary and set up with its own options, and zig's failure
//! was a *test runner* timeout, not a process hang.
//!
//! There is deliberately no watchdog here — this arm is about whether the
//! harness matters, and a watchdog would change the thread population.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const testing = std.testing;

const RwLock = @import("std_rwlock.zig");
const build_options = @import("build_options");

const iterations = build_options.iterations;

test "canceled writer does not leak a semaphore permit" {
    if (builtin.single_threaded) return error.SkipZigTest;

    const io = testing.io;

    const Writer = struct {
        fn lockUnlock(rl: *RwLock, writer_io: Io) Io.Cancelable!void {
            try rl.lock(writer_io);
            rl.unlock(writer_io);
        }
    };

    var rl: RwLock = .init;

    for (0..iterations) |_| {
        rl.lockSharedUncancelable(io);

        var wfuture = io.concurrent(Writer.lockUnlock, .{ &rl, io }) catch |err| switch (err) {
            error.ConcurrencyUnavailable => {
                rl.unlockShared(io);
                return error.SkipZigTest;
            },
        };
        var rfuture = io.concurrent(RwLock.unlockShared, .{ &rl, io }) catch |err| switch (err) {
            error.ConcurrencyUnavailable => {
                rl.unlockShared(io);
                wfuture.await(io) catch {};
                return error.SkipZigTest;
            },
        };

        // Races the last reader's `post` against the writer's cancelation.
        wfuture.cancel(io) catch {};
        rfuture.await(io);

        try testing.expectEqual(0, rl.state);
        if (RwLock.fix_36178) try testing.expectEqual(0, rl.semaphore.permits);
        rl.semaphore.permits = 0;
    }
}

test "tryLock does not race with readers" {
    if (builtin.single_threaded) return error.SkipZigTest;

    const Context = struct {
        rl: RwLock,

        fn reader(ctx: *@This(), io: Io) !void {
            while (true) {
                if (ctx.rl.tryLockShared(io)) ctx.rl.unlockShared(io);
                try io.checkCancel();
            }
        }
    };

    var ctx: Context = .{ .rl = .init };
    const io = testing.io;

    var future = io.concurrent(Context.reader, .{ &ctx, io }) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };
    defer future.cancel(io) catch {};

    for (0..iterations) |_| {
        if (!ctx.rl.tryLock(io)) continue;
        defer ctx.rl.unlock(io);
        const state = @atomicLoad(usize, &ctx.rl.state, .seq_cst);
        try testing.expectEqual(0, state & RwLock.reader_mask);
    }
}
