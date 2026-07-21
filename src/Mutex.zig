// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! A mutual exclusion lock shareable between tasks on different `std.Io`
//! instances. Same three-state futex design as `std.Io.Mutex`, parked on a
//! `Parker` so the wake reaches the right io.

const std = @import("std");

const Io = std.Io;
const Cancelable = Io.Cancelable;
const Parker = @import("Parker.zig");

const Mutex = @This();

parker: Parker = .{},

pub const init: Mutex = .{};

const unlocked: u32 = 0;
const locked_once: u32 = 1;
const contended: u32 = 2;

pub fn tryLock(m: *Mutex) bool {
    return m.parker.word.cmpxchgStrong(unlocked, locked_once, .acquire, .monotonic) == null;
}

pub fn lock(m: *Mutex, io: Io) Cancelable!void {
    if (m.tryLock()) {
        @branchHint(.likely);
        return;
    }

    var node: Parker.Node = .{ .io = io };
    const ref = m.parker.enter(&node);
    defer m.parker.leave(ref);

    while (m.parker.word.swap(contended, .acq_rel) != unlocked) {
        try io.futexWait(u32, &m.parker.word.raw, contended);
    }
}

pub fn lockUncancelable(m: *Mutex, io: Io) void {
    if (m.tryLock()) {
        @branchHint(.likely);
        return;
    }

    var node: Parker.Node = .{ .io = io };
    const ref = m.parker.enter(&node);
    defer m.parker.leave(ref);

    while (m.parker.word.swap(contended, .acq_rel) != unlocked) {
        io.futexWaitUncancelable(u32, &m.parker.word.raw, contended);
    }
}

pub fn unlock(m: *Mutex, io: Io) void {
    _ = io;
    switch (m.parker.word.swap(unlocked, .seq_cst)) {
        locked_once => {},
        contended => {
            @branchHint(.unlikely);
            m.parker.wake(1);
        },
        else => unreachable,
    }
}

test "uncontended lock/unlock" {
    const io = std.testing.io;

    var m: Mutex = .init;

    try std.testing.expect(m.tryLock());
    try std.testing.expect(!m.tryLock());
    m.unlock(io);

    try m.lock(io);
    m.unlock(io);
}

test "concurrent counter" {
    const io = std.testing.io;

    var m: Mutex = .init;
    var counter: u64 = 0;

    const Worker = struct {
        fn run(w_io: Io, mtx: *Mutex, ctr: *u64) Cancelable!void {
            for (0..1000) |_| {
                try mtx.lock(w_io);
                ctr.* += 1;
                mtx.unlock(w_io);
            }
        }
    };

    var group: Io.Group = .init;
    for (0..4) |_| {
        group.concurrent(io, Worker.run, .{ io, &m, &counter }) catch return error.SkipZigTest;
    }
    try group.await(io);

    try std.testing.expectEqual(4000, counter);
}

test "canceled waiter does not consume the wake" {
    const io = std.testing.io;

    var m: Mutex = .init;

    const T = struct {
        fn locker(w_io: Io, mtx: *Mutex) Cancelable!void {
            try mtx.lock(w_io);
            mtx.unlock(w_io);
        }
    };

    for (0..50) |_| {
        try m.lock(io);

        var a = io.concurrent(T.locker, .{ io, &m }) catch |err| switch (err) {
            error.ConcurrencyUnavailable => {
                m.unlock(io);
                return error.SkipZigTest;
            },
        };
        var b = io.concurrent(T.locker, .{ io, &m }) catch |err| switch (err) {
            error.ConcurrencyUnavailable => {
                m.unlock(io);
                _ = a.cancel(io) catch {};
                return error.SkipZigTest;
            },
        };

        // At least one waiter has parked; the main task holds the lock, so
        // neither can acquire and the cancel outcome is deterministic.
        while (m.parker.word.load(.acquire) != contended) std.atomic.spinLoopHint();

        try std.testing.expectEqual(error.Canceled, a.cancel(io));
        m.unlock(io);
        try b.await(io);
    }
}

test "three ios: concurrent counter" {
    const gpa = std.testing.allocator;

    var t1: Io.Threaded = .init(gpa, .{});
    defer t1.deinit();
    var t2: Io.Threaded = .init(gpa, .{});
    defer t2.deinit();
    var t3: Io.Threaded = .init(gpa, .{});
    defer t3.deinit();
    const ios: [3]Io = .{ t1.io(), t2.io(), t3.io() };

    var m: Mutex = .init;
    var counter: u64 = 0;

    const Worker = struct {
        fn run(w_io: Io, mtx: *Mutex, ctr: *u64) Cancelable!void {
            for (0..1000) |_| {
                try mtx.lock(w_io);
                ctr.* += 1;
                mtx.unlock(w_io);
            }
        }
    };

    var groups: [3]Io.Group = .{ .init, .init, .init };
    defer for (&groups, ios) |*g, io| g.cancel(io);
    for (&groups, ios) |*g, io| {
        for (0..2) |_| {
            g.concurrent(io, Worker.run, .{ io, &m, &counter }) catch return error.SkipZigTest;
        }
    }
    for (&groups, ios) |*g, io| try g.await(io);

    try std.testing.expectEqual(6000, counter);
}
