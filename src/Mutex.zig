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

    while (m.parker.word.swap(contended, .acquire) != unlocked) {
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

    while (m.parker.word.swap(contended, .acquire) != unlocked) {
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
