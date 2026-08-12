//! The two tests from ziglang/zig's `lib/std/Io/RwLock.zig` on branch
//! `rwlock-fixes`, copied verbatim. The only edits are the ones that are
//! unavoidable outside the std tree: `Io.RwLock` becomes the local `RwLock`
//! copy, and `reader_mask` becomes `RwLock.reader_mask`. Iteration counts,
//! assertions and control flow are untouched, so that timing measured here is
//! directly comparable to zig's CI, which runs these under `--test-timeout 2m`.
//!
//! Build with `-Dfix-36178=false` and the first test is *expected* to fail on a
//! leaked permit; that arm is for `main.zig`, not for this one.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const testing = std.testing;

const RwLock = @import("std_rwlock.zig");

test "canceled writer does not leak a semaphore permit" {
    if (builtin.single_threaded) return error.SkipZigTest;

    const Writer = struct {
        fn lockUnlock(rl: *RwLock, io: Io) Io.Cancelable!void {
            try rl.lock(io);
            rl.unlock(io);
        }
    };

    const io = testing.io;

    var rl: RwLock = .init;

    for (0..10_000) |_| {
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
        try testing.expectEqual(0, rl.semaphore.permits);
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

    for (0..10_000) |_| {
        if (!ctx.rl.tryLock(io)) continue;
        defer ctx.rl.unlock(io);
        const state = @atomicLoad(usize, &ctx.rl.state, .seq_cst);
        try testing.expectEqual(0, state & RwLock.reader_mask);
    }

    try testing.expectEqual(0, ctx.rl.semaphore.permits);
}
