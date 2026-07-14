// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! A mutual exclusion lock shareable between tasks on different `std.Io`
//! instances. The lock is handed directly to the next FIFO waiter.

const std = @import("std");

const Io = std.Io;
const Cancelable = Io.Cancelable;
const WaitQueue = @import("WaitQueue.zig");
const Node = WaitQueue.Node;

const Mutex = @This();

/// Flag set means unlocked; flag clear means locked, with or without waiters.
queue: WaitQueue = .flagged,

pub const init: Mutex = .{};

/// Acquires the lock without blocking, returning whether it succeeded.
pub fn tryLock(m: *Mutex) bool {
    return m.queue.tryClearFlag();
}

/// Acquires the lock, parking on `io` while another task holds it. On
/// `error.Canceled` the lock is not held.
pub fn lock(m: *Mutex, io: Io) Cancelable!void {
    if (m.queue.tryClearFlag()) return;

    var w: Node = .{ .io = io };
    if (m.queue.push(&w, .acquire) == .acquired) return;

    w.wait() catch |err| {
        // If we can't remove ourselves, unlock() already handed us the lock:
        // take the wake and pass it on.
        if (!m.queue.remove(&w)) {
            w.waitUncancelable();
            m.unlock(io);
        }
        return err;
    };
}

/// Like `lock`, but ignores cancellation and always ends up holding the lock.
pub fn lockUncancelable(m: *Mutex, io: Io) void {
    if (m.queue.tryClearFlag()) return;

    var w: Node = .{ .io = io };
    if (m.queue.push(&w, .acquire) == .acquired) return;

    w.waitUncancelable();
}

/// Releases the lock, handing it to the next waiter if there is one.
pub fn unlock(m: *Mutex, io: Io) void {
    _ = io;
    if (m.queue.pop(.set_flag)) |w| w.wake();
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
