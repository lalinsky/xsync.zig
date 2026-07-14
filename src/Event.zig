// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! A settable boolean flag with a "wait until set" operation, shareable
//! between tasks on different `std.Io` instances. Same three-state design as
//! `std.Io.Event`, parked on a `Parker` so wakes reach the right io.
//!
//! `set` is sticky: stays true until `reset`, so a task that waits after `set`
//! returns immediately. `set` wakes every pending waiter.

const std = @import("std");

const Io = std.Io;
const Cancelable = Io.Cancelable;
const Parker = @import("Parker.zig");

const Event = @This();

parker: Parker = .{},

const State = enum(u32) { unset, waiting, is_set };

pub const init: Event = .{};
pub const unset: Event = .{};

pub fn isSet(e: *const Event) bool {
    return e.parker.word.load(.acquire) == @intFromEnum(State.is_set);
}

/// Blocks until the event is set.
pub fn wait(e: *Event, io: Io) Cancelable!void {
    if (!e.beginWait()) return;

    var node: Parker.Node = .{ .io = io };
    const ref = e.parker.enter(&node);
    defer e.parker.leave(ref);

    while (true) {
        try io.futexWait(u32, &e.parker.word.raw, @intFromEnum(State.waiting));
        switch (e.load()) {
            .unset => unreachable, // reset called before pending wait returned
            .waiting => continue,
            .is_set => return,
        }
    }
}

/// Like `wait`, but ignores cancellation.
pub fn waitUncancelable(e: *Event, io: Io) void {
    if (!e.beginWait()) return;

    var node: Parker.Node = .{ .io = io };
    const ref = e.parker.enter(&node);
    defer e.parker.leave(ref);

    while (true) {
        io.futexWaitUncancelable(u32, &e.parker.word.raw, @intFromEnum(State.waiting));
        switch (e.load()) {
            .unset => unreachable, // reset called before pending wait returned
            .waiting => continue,
            .is_set => return,
        }
    }
}

pub const WaitTimeoutError = error{Timeout} || Cancelable;

/// Blocks until the event is set, the timeout expires, or a spurious wakeup
/// occurs. Returns `error.Timeout` for the latter two.
pub fn waitTimeout(e: *Event, io: Io, timeout: Io.Timeout) WaitTimeoutError!void {
    if (!e.beginWait()) return;

    var node: Parker.Node = .{ .io = io };
    const ref = e.parker.enter(&node);
    defer e.parker.leave(ref);

    try io.futexWaitTimeout(u32, &e.parker.word.raw, @intFromEnum(State.waiting), timeout);
    switch (e.load()) {
        .unset => unreachable, // reset called before pending wait returned
        .waiting => return error.Timeout,
        .is_set => return,
    }
}

/// Sets the event and wakes every pending waiter. Idempotent until `reset`.
///
/// A canceled or timed-out wait leaves the state at `waiting` (there may be
/// other waiters), so at worst this does one redundant wake.
pub fn set(e: *Event, io: Io) void {
    _ = io;
    const prev = e.parker.word.swap(@intFromEnum(State.is_set), .seq_cst);
    if (prev == @intFromEnum(State.waiting)) {
        e.parker.wake(std.math.maxInt(u32));
    }
}

/// Clears the event. Assumes there are no pending waiters.
pub fn reset(e: *Event) void {
    e.parker.word.store(@intFromEnum(State.unset), .monotonic);
}

/// Moves unset -> waiting. Returns false if the event is already set.
fn beginWait(e: *Event) bool {
    const prev = e.parker.word.cmpxchgStrong(
        @intFromEnum(State.unset),
        @intFromEnum(State.waiting),
        .acquire,
        .acquire,
    ) orelse return true;
    return switch (@as(State, @enumFromInt(prev))) {
        .unset => unreachable,
        .waiting => true,
        .is_set => false,
    };
}

fn load(e: *const Event) State {
    return @enumFromInt(e.parker.word.load(.acquire));
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

    // Wait for at least one waiter to park so the wake path runs; stragglers
    // that arrive after set() just take the already-set fast path.
    while (e.parker.word.load(.acquire) != @intFromEnum(State.waiting)) std.atomic.spinLoopHint();
    e.set(io);

    try group.await(io);
    try std.testing.expectEqual(3, woken.load(.monotonic));
}

test "waitTimeout times out" {
    const io = std.testing.io;

    var e: Event = .init;

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } };
    try std.testing.expectError(error.Timeout, e.waitTimeout(io, timeout));
}
