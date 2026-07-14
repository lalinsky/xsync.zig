// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! A condition variable shareable between tasks on different `std.Io`
//! instances. Used with a `Mutex` guarding the predicate.

const std = @import("std");

const Io = std.Io;
const Cancelable = Io.Cancelable;
const WaitQueue = @import("WaitQueue.zig");
const Node = WaitQueue.Node;
const Mutex = @import("Mutex.zig");

const Condition = @This();

queue: WaitQueue = .empty,

pub const init: Condition = .{};

/// Atomically releases `mutex` and waits for a signal, reacquiring `mutex`
/// before returning. The mutex is held again even on `error.Canceled`.
pub fn wait(c: *Condition, io: Io, mutex: *Mutex) Cancelable!void {
    var w: Node = .{ .io = io };
    _ = c.queue.push(&w, .queue);
    mutex.unlock(io);

    w.wait() catch |err| {
        // If a signal already claimed us, consume it and forward it on.
        if (!c.queue.remove(&w)) {
            w.waitUncancelable();
            if (c.queue.pop(.keep_flag)) |n| n.wake();
        }
        mutex.lockUncancelable(io);
        return err;
    };

    mutex.lockUncancelable(io);
}

/// Like `wait`, but ignores cancellation.
pub fn waitUncancelable(c: *Condition, io: Io, mutex: *Mutex) void {
    var w: Node = .{ .io = io };
    _ = c.queue.push(&w, .queue);
    mutex.unlock(io);
    w.waitUncancelable();
    mutex.lockUncancelable(io);
}

/// Like `wait`, but returns `error.Timeout` if no signal arrives before
/// `timeout` elapses. The mutex is held on every return path.
pub fn timedWait(c: *Condition, io: Io, mutex: *Mutex, timeout: Io.Timeout) (error{Timeout} || Cancelable)!void {
    if (timeout == .none) return c.wait(io, mutex);

    var w: Node = .{ .io = io };
    _ = c.queue.push(&w, .queue);
    mutex.unlock(io);

    const deadline = timeout.toDeadline(io);
    w.timedWait(deadline) catch |err| {
        if (!c.queue.remove(&w)) {
            w.waitUncancelable();
            if (c.queue.pop(.keep_flag)) |n| n.wake();
        }
        mutex.lockUncancelable(io);
        return err;
    };

    // If we can still remove ourselves the deadline won; else a signal did.
    const timed_out = c.queue.remove(&w);
    if (!timed_out) w.waitUncancelable();

    mutex.lockUncancelable(io);
    if (timed_out) return error.Timeout;
}

/// Wakes one waiter, if any.
pub fn signal(c: *Condition, io: Io) void {
    _ = io;
    if (c.queue.pop(.keep_flag)) |w| w.wake();
}

/// Wakes every current waiter.
pub fn broadcast(c: *Condition, io: Io) void {
    _ = io;
    while (c.queue.pop(.keep_flag)) |w| w.wake();
}

test "wait/signal" {
    const io = std.testing.io;

    var m: Mutex = .init;
    var cond: Condition = .init;
    var ready = false;

    const T = struct {
        fn waiter(w_io: Io, mtx: *Mutex, cnd: *Condition, flag: *bool) Cancelable!void {
            try mtx.lock(w_io);
            defer mtx.unlock(w_io);
            while (!flag.*) try cnd.wait(w_io, mtx);
        }
        fn signaler(w_io: Io, mtx: *Mutex, cnd: *Condition, flag: *bool) Cancelable!void {
            try mtx.lock(w_io);
            flag.* = true;
            mtx.unlock(w_io);
            cnd.signal(w_io);
        }
    };

    var group: Io.Group = .init;
    group.concurrent(io, T.waiter, .{ io, &m, &cond, &ready }) catch return error.SkipZigTest;
    group.concurrent(io, T.signaler, .{ io, &m, &cond, &ready }) catch return error.SkipZigTest;
    try group.await(io);

    try std.testing.expect(ready);
}

test "broadcast wakes all" {
    const io = std.testing.io;

    var m: Mutex = .init;
    var cond: Condition = .init;
    var ready = false;
    var arrived: std.atomic.Value(u32) = .init(0);
    var woken: std.atomic.Value(u32) = .init(0);

    const T = struct {
        fn waiter(w_io: Io, mtx: *Mutex, cnd: *Condition, flag: *bool, arr: *std.atomic.Value(u32), count: *std.atomic.Value(u32)) Cancelable!void {
            try mtx.lock(w_io);
            defer mtx.unlock(w_io);
            _ = arr.fetchAdd(1, .monotonic);
            while (!flag.*) try cnd.wait(w_io, mtx);
            _ = count.fetchAdd(1, .monotonic);
        }
    };

    var group: Io.Group = .init;
    for (0..3) |_| {
        group.concurrent(io, T.waiter, .{ io, &m, &cond, &ready, &arrived, &woken }) catch return error.SkipZigTest;
    }

    // Once all three have arrived under the mutex, taking the lock guarantees
    // they have released into cond.wait (and thus queued).
    while (arrived.load(.monotonic) < 3) std.atomic.spinLoopHint();
    m.lockUncancelable(io);
    ready = true;
    m.unlock(io);
    cond.broadcast(io);

    try group.await(io);
    try std.testing.expectEqual(@as(u32, 3), woken.load(.monotonic));
}

test "timedWait times out" {
    const io = std.testing.io;

    var m: Mutex = .init;
    var cond: Condition = .init;

    try m.lock(io);
    defer m.unlock(io);

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } };
    try std.testing.expectError(error.Timeout, cond.timedWait(io, &m, timeout));
}
