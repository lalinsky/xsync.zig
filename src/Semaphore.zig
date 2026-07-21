// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! An unsigned integer that blocks the kernel thread if the number would
//! become negative.
//!
//! This API supports static initialization and does not require deinitialization.
const Semaphore = @This();

const builtin = @import("builtin");

const std = @import("std");
const Io = std.Io;
const Mutex = @import("Mutex.zig");
const Condition = @import("Condition.zig");
const testing = std.testing;

mutex: Mutex = .init,
cond: Condition = .init,
/// It is OK to initialize this field to any value.
permits: usize = 0,

/// Blocks until a `permit` is available and consumes a single one.
/// Unblocks without consuming a `permit` when canceled.
///
/// See also:
/// * `waitTimeout`
/// * `waitUncancelable`
pub fn wait(s: *Semaphore, io: Io) Io.Cancelable!void {
    s.waitTimeout(io, .none) catch |err| switch (err) {
        error.Timeout => unreachable,
        error.Canceled => |e| return e,
    };
}

pub const WaitTimeoutError = Io.Cancelable || Io.Timeout.Error;

/// Blocks until a `permit` is available and consumes a single one.
/// Unblocks without consuming a `permit` when canceled or when the provided
/// timeout expires before a `permit` is available.
///
/// See also:
/// * `wait`
/// * `waitUncancelable`
pub fn waitTimeout(s: *Semaphore, io: Io, timeout: Io.Timeout) WaitTimeoutError!void {
    const deadline = timeout.toDeadline(io);
    try s.mutex.lock(io);
    defer s.mutex.unlock(io);
    while (s.permits == 0) try s.cond.waitTimeout(io, &s.mutex, deadline);
    s.permits -= 1;
    if (s.permits > 0) s.cond.signal(io);
}

/// Blocks until a `permit` is available and consumes a single one.
///
/// See also:
/// * `wait`
/// * `waitTimeout`
pub fn waitUncancelable(s: *Semaphore, io: Io) void {
    s.mutex.lockUncancelable(io);
    defer s.mutex.unlock(io);
    while (s.permits == 0) s.cond.waitUncancelable(io, &s.mutex);
    s.permits -= 1;
    if (s.permits > 0) s.cond.signal(io);
}

/// Makes an additional `permit` available.
pub fn post(s: *Semaphore, io: Io) void {
    s.mutex.lockUncancelable(io);
    defer s.mutex.unlock(io);

    s.permits += 1;
    s.cond.signal(io);
}

test wait {
    const io = testing.io;

    const Context = struct {
        sem: Semaphore = .{ .permits = 1 },
        n: u32 = 0,

        fn worker(ctx: *@This()) !void {
            try ctx.sem.wait(io);
            ctx.n += 1;
            ctx.sem.post(io);
        }
    };

    var ctx: Context = .{};

    var group: Io.Group = .init;
    defer group.cancel(io);

    const num_workers = 3;
    for (0..num_workers) |_| group.async(io, Context.worker, .{&ctx});

    try group.await(io);
    try testing.expectEqual(num_workers, ctx.n);
}

test waitTimeout {
    const io = testing.io;

    const Context = struct {
        ready: Io.Event = .unset,
        sem: Semaphore = .{ .permits = 0 },
        value: u32 = 0,

        fn worker(ctx: *@This()) !void {
            defer ctx.ready.set(io);

            try testing.expectError(error.Timeout, ctx.sem.waitTimeout(io, .{ .duration = .{
                .raw = .fromMilliseconds(1),
                .clock = .awake,
            } }));
            try testing.expectEqual(0, ctx.value);

            ctx.ready.set(io);

            while (ctx.value == 0) try ctx.sem.wait(io);
            try testing.expectEqual(1, ctx.value);
        }
    };

    var ctx: Context = .{};

    var future = io.concurrent(Context.worker, .{&ctx}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };
    defer future.cancel(io) catch {};

    try ctx.ready.wait(io);

    ctx.value = 1;
    ctx.sem.post(io);

    try future.await(io);
}

test "post racing timeout never loses a permit" {
    const io = testing.io;

    const T = struct {
        fn waiter(w_io: Io, sem: *Semaphore, got: *std.atomic.Value(bool)) Io.Cancelable!void {
            sem.waitTimeout(w_io, .{ .duration = .{ .raw = .fromMicroseconds(100), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => return,
                error.Canceled => |e| return e,
            };
            got.store(true, .seq_cst);
        }
    };

    for (0..100) |_| {
        var sem: Semaphore = .{};
        var got: std.atomic.Value(bool) = .init(false);

        var fut = io.concurrent(T.waiter, .{ io, &sem, &got }) catch |err| switch (err) {
            error.ConcurrencyUnavailable => return error.SkipZigTest,
        };
        sem.post(io);
        fut.await(io) catch {};

        // The permit was either consumed by the waiter or is still available.
        sem.mutex.lockUncancelable(io);
        const permits = sem.permits;
        sem.mutex.unlock(io);
        if (got.load(.seq_cst)) {
            try testing.expectEqual(0, permits);
        } else {
            try testing.expectEqual(1, permits);
        }
    }
}

test "two ios: capacity invariant under stress" {
    const gpa = testing.allocator;

    var t1: Io.Threaded = .init(gpa, .{});
    defer t1.deinit();
    var t2: Io.Threaded = .init(gpa, .{});
    defer t2.deinit();
    const ios: [2]Io = .{ t1.io(), t2.io() };

    var sem: Semaphore = .{ .permits = 2 };
    var inside: std.atomic.Value(u32) = .init(0);
    var over: std.atomic.Value(u32) = .init(0);

    const T = struct {
        fn worker(w_io: Io, s: *Semaphore, in_count: *std.atomic.Value(u32), ov: *std.atomic.Value(u32)) Io.Cancelable!void {
            for (0..500) |_| {
                try s.wait(w_io);
                if (in_count.fetchAdd(1, .seq_cst) >= 2) _ = ov.fetchAdd(1, .monotonic);
                std.atomic.spinLoopHint();
                _ = in_count.fetchSub(1, .seq_cst);
                s.post(w_io);
            }
        }
    };

    var groups: [2]Io.Group = .{ .init, .init };
    defer for (&groups, ios) |*g, io| g.cancel(io);
    for (&groups, ios) |*g, io| {
        for (0..3) |_| {
            g.concurrent(io, T.worker, .{ io, &sem, &inside, &over }) catch return error.SkipZigTest;
        }
    }
    for (&groups, ios) |*g, io| try g.await(io);

    try testing.expectEqual(0, over.load(.monotonic));
    sem.mutex.lockUncancelable(ios[0]);
    const permits = sem.permits;
    sem.mutex.unlock(ios[0]);
    try testing.expectEqual(2, permits);
}

test waitUncancelable {
    const io = testing.io;

    const Context = struct {
        sem: Semaphore = .{ .permits = 1 },
        n: u32 = 0,

        fn worker(ctx: *@This()) !void {
            ctx.sem.waitUncancelable(io);
            ctx.n += 1;
            ctx.sem.post(io);
        }
    };

    var ctx: Context = .{};

    var group: Io.Group = .init;
    defer group.cancel(io);

    const num_workers = 3;
    for (0..num_workers) |_| group.async(io, Context.worker, .{&ctx});

    try group.await(io);
    try testing.expectEqual(num_workers, ctx.n);
}
