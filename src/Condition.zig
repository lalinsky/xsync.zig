// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! A condition variable shareable between tasks on different `std.Io`
//! instances. Used with a `Mutex` guarding the predicate. Same design as
//! `std.Io.Condition` (with the cancelation fix from ziglang/zig#35564),
//! parked on a `Parker` so wakes reach the right io.

const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

const Io = std.Io;
const Cancelable = Io.Cancelable;
const Parker = @import("Parker.zig");
const Mutex = @import("Mutex.zig");

const Condition = @This();

state: std.atomic.Value(State) = .init(.{ .waiters = 0, .signals = 0 }),
/// The epoch lives in parker.word; incremented whenever the condition is signaled.
parker: Parker = .{},

const State = packed struct(u32) {
    waiters: u16,
    signals: u16,
};

pub const init: Condition = .{};

pub fn wait(c: *Condition, io: Io, mutex: *Mutex) Cancelable!void {
    c.waitTimeout(io, mutex, .none) catch |err| switch (err) {
        error.Timeout => unreachable,
        error.Canceled => |e| return e,
    };
}

pub const WaitTimeoutError = Cancelable || Io.Timeout.Error;

pub fn waitTimeout(c: *Condition, io: Io, mutex: *Mutex, timeout: Io.Timeout) WaitTimeoutError!void {
    const deadline = timeout.toDeadline(io);
    const epoch_word = &c.parker.word;

    var epoch = epoch_word.load(.acquire); // `.acquire` to ensure ordered before state load

    {
        const prev_state = c.state.fetchAdd(.{ .waiters = 1, .signals = 0 }, .monotonic);
        assert(prev_state.waiters < math.maxInt(u16)); // overflow caused by too many waiters
    }

    var node: Parker.Node = .{ .io = io };
    const ref = c.parker.enter(&node);
    defer c.parker.leave(ref);

    mutex.unlock(io);
    defer mutex.lockUncancelable(io);

    while (true) {
        const result = io.futexWaitTimeout(u32, &epoch_word.raw, epoch, deadline);

        epoch = epoch_word.load(.acquire); // `.acquire` to ensure ordered before `state` load

        // We were woken normally, so try to consume a pending signal. A signal takes
        // priority over an expired deadline, so this is checked before the deadline
        // below. On error we safely remove ourselves as a waiter and propagate the error.
        if (result) |_| {
            var prev_state = c.state.load(.monotonic);
            while (prev_state.signals > 0) {
                prev_state = c.state.cmpxchgWeak(prev_state, .{
                    .waiters = prev_state.waiters - 1,
                    .signals = prev_state.signals - 1,
                }, .acquire, .monotonic) orelse {
                    // We successfully consumed a signal.
                    return;
                };
            }
        } else |err| {
            c.deregister();
            return err;
        }

        // There are no signals available and no error; if a timeout was specified and
        // the deadline has passed, remove ourselves as a waiter and return
        // `error.Timeout`. Otherwise, this was a spurious wakeup: loop back to the
        // futex wait.
        switch (deadline) {
            .none => {},
            .deadline => |d| if (d.untilNow(io).raw.nanoseconds >= 0) {
                c.deregister();
                return error.Timeout;
            },
            .duration => unreachable,
        }
    }
}

/// Same as `wait`, except does not introduce a cancelation point.
pub fn waitUncancelable(c: *Condition, io: Io, mutex: *Mutex) void {
    const epoch_word = &c.parker.word;

    var epoch = epoch_word.load(.acquire);

    {
        const prev_state = c.state.fetchAdd(.{ .waiters = 1, .signals = 0 }, .monotonic);
        assert(prev_state.waiters < math.maxInt(u16)); // overflow caused by too many waiters
    }

    var node: Parker.Node = .{ .io = io };
    const ref = c.parker.enter(&node);
    defer c.parker.leave(ref);

    mutex.unlock(io);
    defer mutex.lockUncancelable(io);

    while (true) {
        io.futexWaitUncancelable(u32, &epoch_word.raw, epoch);

        epoch = epoch_word.load(.acquire);

        var prev_state = c.state.load(.monotonic);
        while (prev_state.signals > 0) {
            prev_state = c.state.cmpxchgWeak(prev_state, .{
                .waiters = prev_state.waiters - 1,
                .signals = prev_state.signals - 1,
            }, .acquire, .monotonic) orelse {
                // We successfully consumed a signal.
                return;
            };
        }

        // No signals available; spurious wakeup, loop back to the futex wait.
    }
}

fn deregister(c: *Condition) void {
    var prev_state = c.state.load(.monotonic);
    while (true) {
        assert(prev_state.waiters > 0); // underflow caused by illegal state
        const new_signals = @min(prev_state.signals, prev_state.waiters - 1);
        prev_state = c.state.cmpxchgWeak(prev_state, .{
            .waiters = prev_state.waiters - 1,
            .signals = new_signals,
        }, .acquire, .monotonic) orelse {
            if (prev_state.signals > 0 and prev_state.signals < prev_state.waiters) {
                // We kept a signal we are not consuming; wake a remaining waiter for it.
                _ = c.parker.word.fetchAdd(1, .seq_cst);
                c.parker.wake(1);
            }
            return;
        };
    }
}

pub fn signal(c: *Condition, io: Io) void {
    _ = io;
    var prev_state = c.state.load(.monotonic);
    while (prev_state.waiters > prev_state.signals) {
        @branchHint(.unlikely);
        prev_state = c.state.cmpxchgWeak(prev_state, .{
            .waiters = prev_state.waiters,
            .signals = prev_state.signals + 1,
        }, .release, .monotonic) orelse {
            // Update the epoch to tell the waiting threads that there are new signals for them.
            // Note that a waiting thread could miss a take if *exactly* (1<<32)-1 wakes happen
            // between it observing the epoch and sleeping on it, but this is extraordinarily
            // unlikely due to the precise number of calls required.
            _ = c.parker.word.fetchAdd(1, .seq_cst); // ordered after `state`, and against wake()'s directory scan
            c.parker.wake(1);
            return;
        };
    }
}

pub fn broadcast(c: *Condition, io: Io) void {
    _ = io;
    var prev_state = c.state.load(.monotonic);
    while (prev_state.waiters > prev_state.signals) {
        @branchHint(.unlikely);
        prev_state = c.state.cmpxchgWeak(prev_state, .{
            .waiters = prev_state.waiters,
            .signals = prev_state.waiters,
        }, .release, .monotonic) orelse {
            _ = c.parker.word.fetchAdd(1, .seq_cst); // ordered after `state`, and against wake()'s directory scan
            c.parker.wake(prev_state.waiters - prev_state.signals);
            return;
        };
    }
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
    try std.testing.expectEqual(3, woken.load(.monotonic));
}

test "three ios: broadcast reaches every io" {
    const gpa = std.testing.allocator;

    var t1: Io.Threaded = .init(gpa, .{});
    defer t1.deinit();
    var t2: Io.Threaded = .init(gpa, .{});
    defer t2.deinit();
    var t3: Io.Threaded = .init(gpa, .{});
    defer t3.deinit();
    const ios: [3]Io = .{ t1.io(), t2.io(), t3.io() };

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

    var groups: [3]Io.Group = .{ .init, .init, .init };
    defer for (&groups, ios) |*g, io| g.cancel(io);
    for (&groups, ios) |*g, io| {
        g.concurrent(io, T.waiter, .{ io, &m, &cond, &ready, &arrived, &woken }) catch return error.SkipZigTest;
    }

    // Once all three have arrived under the mutex, taking the lock guarantees
    // they have released into cond.wait (and thus queued).
    while (arrived.load(.monotonic) < 3) std.atomic.spinLoopHint();
    m.lockUncancelable(ios[0]);
    ready = true;
    m.unlock(ios[0]);
    cond.broadcast(ios[0]);

    for (&groups, ios) |*g, io| try g.await(io);
    try std.testing.expectEqual(3, woken.load(.monotonic));
}

test "waitTimeout times out" {
    const io = std.testing.io;

    var m: Mutex = .init;
    var cond: Condition = .init;

    try m.lock(io);
    defer m.unlock(io);

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } };
    try std.testing.expectError(error.Timeout, cond.waitTimeout(io, &m, timeout));
}
