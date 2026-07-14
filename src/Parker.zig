// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! A futex word that tasks from multiple `Io` instances can park on. Tracks
//! which ios currently have sleepers so a waker can reach each one through
//! the right instance. Two inline slots cover the realistic case; further
//! ios fall back to an intrusive list behind an OS mutex.

const std = @import("std");
const Io = std.Io;

const Parker = @This();

word: std.atomic.Value(u32) = .init(0),
slots: [2]Slot = .{ .{}, .{} },
dir_lock: Io.Mutex = .init,
overflow: std.atomic.Value(?*Node) = .init(null),

/// Transient count marking a slot whose io is being written or cleared.
const claiming = std.math.maxInt(u32);

const Slot = struct {
    count: std.atomic.Value(u32) = .init(0),
    /// Stable while count is a pin count; written only by the holder that
    /// moved count to `claiming`.
    io: ?Io = null,

    /// Pin the slot so its io cannot be rewritten. Fails if it has no users.
    fn pin(s: *Slot) bool {
        var c = s.count.load(.seq_cst);
        while (c != 0 and c != claiming) {
            std.debug.assert(c != claiming - 1); // pin count saturated
            c = s.count.cmpxchgWeak(c, c + 1, .seq_cst, .seq_cst) orelse return true;
        }
        return false;
    }

    /// Drop a pin. The last one out clears the slot, returning the parker to
    /// its init state (users may compare a quiesced primitive against .init).
    fn unpin(s: *Slot) void {
        var c = s.count.load(.monotonic);
        while (true) {
            if (c == 1) {
                c = s.count.cmpxchgWeak(1, claiming, .acquire, .monotonic) orelse {
                    s.io = null;
                    s.count.store(0, .release);
                    return;
                };
            } else {
                c = s.count.cmpxchgWeak(c, c - 1, .release, .monotonic) orelse return;
            }
        }
    }

    /// Pin only if the slot belongs to `io`. The identity check runs while
    /// pinned, so a slot recycled for another io between load and CAS is
    /// caught here and released.
    fn tryPin(s: *Slot, io: Io) bool {
        if (!s.pin()) return false;
        if (ioEql(s.io.?, io)) return true;
        s.unpin();
        return false;
    }

    /// Take a free slot for `io`. Fails if the slot is not exactly free.
    fn tryClaim(s: *Slot, io: Io) bool {
        if (s.count.cmpxchgStrong(0, claiming, .acquire, .monotonic) != null) return false;
        s.io = io;
        // Seq_cst pairs with wake()'s directory scan: a waker that misses this
        // store must have written the word late enough for our futex check to
        // see it.
        s.count.store(1, .seq_cst);
        return true;
    }
};

pub const Node = struct {
    io: Io,
    next: ?*Node = null,
};

pub const Ref = union(enum) {
    slot: *Slot,
    node: *Node,
};

/// Registers `node.io` as having a sleeper on this word. Must be paired with
/// `leave` after the wait returns.
pub fn enter(p: *Parker, node: *Node) Ref {
    while (true) {
        var settled = true;
        for (&p.slots) |*s| {
            switch (s.count.load(.acquire)) {
                0 => if (s.tryClaim(node.io)) return .{ .slot = s } else {
                    settled = false;
                },
                claiming => settled = false,
                else => if (s.tryPin(node.io)) return .{ .slot = s },
            }
        }
        // Both slots stably pinned by other ios: fall back to the overflow
        // list. A transient state may free a slot, so retry those.
        if (settled) break;
        std.atomic.spinLoopHint();
    }

    Io.Threaded.mutexLock(&p.dir_lock);
    defer Io.Threaded.mutexUnlock(&p.dir_lock);
    node.next = p.overflow.load(.monotonic);
    p.overflow.store(node, .release);
    return .{ .node = node };
}

pub fn leave(p: *Parker, ref: Ref) void {
    switch (ref) {
        .slot => |s| s.unpin(),
        .node => |n| {
            Io.Threaded.mutexLock(&p.dir_lock);
            defer Io.Threaded.mutexUnlock(&p.dir_lock);
            var cur = p.overflow.load(.monotonic);
            if (cur == n) {
                p.overflow.store(n.next, .monotonic);
            } else while (cur) |c| : (cur = c.next) {
                if (c.next == n) {
                    c.next = n.next;
                    break;
                }
            }
        },
    }
}

/// Wakes up to `max_waiters` sleepers through every io that has some.
pub fn wake(p: *Parker, max_waiters: u32) void {
    for (&p.slots) |*s| {
        if (s.pin()) {
            const io = s.io.?;
            s.unpin();
            io.futexWake(u32, &p.word.raw, max_waiters);
        }
    }
    if (p.overflow.load(.seq_cst) != null) {
        Io.Threaded.mutexLock(&p.dir_lock);
        defer Io.Threaded.mutexUnlock(&p.dir_lock);
        var cur = p.overflow.load(.monotonic);
        while (cur) |n| : (cur = n.next) {
            n.io.futexWake(u32, &p.word.raw, max_waiters);
        }
    }
}

fn ioEql(a: Io, b: Io) bool {
    return a.userdata == b.userdata and a.vtable == b.vtable;
}
