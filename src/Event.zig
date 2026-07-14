// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! A settable boolean flag with a "wait until set" operation, shareable between
//! tasks on different `std.Io` instances. Mirrors `std.Io.Event`.
//!
//! `set` is sticky: stays true until `reset`, so a task that waits after `set`
//! returns immediately. `set` wakes every pending waiter.

const std = @import("std");

const Io = std.Io;
const Cancelable = Io.Cancelable;
const WaitQueue = @import("WaitQueue.zig");
const Node = WaitQueue.Node;

const Event = @This();

/// Flag set means the event is set.
queue: WaitQueue = .empty,

pub const init: Event = .{};

/// Returns whether the event is currently set.
pub fn isSet(e: *const Event) bool {
    return e.queue.isFlagSet();
}

/// Blocks until the event is set.
pub fn wait(e: *Event, io: Io) Cancelable!void {
    if (e.queue.isFlagSet()) return;

    var w: Node = .{ .io = io };
    if (!e.queue.pushUnlessFlag(&w)) return; // set() won the race

    w.wait() catch |err| {
        // If set() already took us, take delivery of the wake before leaving.
        if (!e.queue.remove(&w)) w.waitUncancelable();
        return err;
    };
}

/// Like `wait`, but ignores cancellation.
pub fn waitUncancelable(e: *Event, io: Io) void {
    if (e.queue.isFlagSet()) return;

    var w: Node = .{ .io = io };
    if (!e.queue.pushUnlessFlag(&w)) return;

    w.waitUncancelable();
}

/// Blocks until the event is set or `timeout` elapses, returning
/// `error.Timeout` on expiry.
pub fn waitTimeout(e: *Event, io: Io, timeout: Io.Timeout) (error{Timeout} || Cancelable)!void {
    if (e.queue.isFlagSet()) return;
    if (timeout == .none) return e.wait(io);

    var w: Node = .{ .io = io };
    if (!e.queue.pushUnlessFlag(&w)) return;

    const deadline = timeout.toDeadline(io);
    w.timedWait(deadline) catch |err| {
        if (!e.queue.remove(&w)) w.waitUncancelable();
        return err;
    };

    const timed_out = e.queue.remove(&w);
    if (!timed_out) w.waitUncancelable();
    if (timed_out) return error.Timeout;
}

/// Sets the event and wakes every pending waiter. Idempotent until `reset`.
pub fn set(e: *Event, io: Io) void {
    _ = io;
    // First pop publishes the flag, so mid-loop arrivals take the set fast path.
    while (e.queue.popAndSetFlag()) |r| {
        r.node.wake();
        if (r.is_last) break;
    }
}

/// Clears the event. Assumes there are no pending waiters.
pub fn reset(e: *Event) void {
    e.queue.clearFlag();
}

test "set/reset and immediate wait" {
    const io = std.testing.io;

    var e: Event = .init;
    try std.testing.expect(!e.isSet());

    e.set(io);
    try std.testing.expect(e.isSet());
    try e.wait(io); // already set: returns immediately

    e.reset();
    try std.testing.expect(!e.isSet());
}

test "set wakes all waiters" {
    const io = std.testing.io;

    var e: Event = .init;
    var woken: std.atomic.Value(u32) = .init(0);

    const T = struct {
        fn waiter(w_io: Io, ev: *Event, count: *std.atomic.Value(u32)) Cancelable!void {
            try ev.wait(w_io);
            _ = count.fetchAdd(1, .monotonic);
        }
    };

    var group: Io.Group = .init;
    for (0..3) |_| {
        group.concurrent(io, T.waiter, .{ io, &e, &woken }) catch return error.SkipZigTest;
    }

    // Ensure at least one waiter is parked so the wake path runs; stragglers
    // that arrive after set() just take the already-set fast path.
    while (!e.queue.hasWaiters()) std.atomic.spinLoopHint();
    e.set(io);

    try group.await(io);
    try std.testing.expectEqual(@as(u32, 3), woken.load(.monotonic));
}

test "waitTimeout times out" {
    const io = std.testing.io;

    var e: Event = .init;

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } };
    try std.testing.expectError(error.Timeout, e.waitTimeout(io, timeout));
}
