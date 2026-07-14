// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! A futex word that tasks from multiple `Io` instances can park on. Tracks
//! which ios currently have sleepers so a waker can reach each one through
//! the right instance. Two inline slots cover the realistic case; further
//! ios fall back to an intrusive list behind a spinlock.

const std = @import("std");
const Io = std.Io;

const Parker = @This();

word: std.atomic.Value(u32) = .init(0),
slots: [2]Slot = .{ .{}, .{} },
dir_lock: std.atomic.Value(bool) = .init(false),
overflow: std.atomic.Value(?*Node) = .init(null),

const Slot = struct {
    count: std.atomic.Value(u32) = .init(0),
    /// Stable while count > 0; written only under dir_lock while count == 0.
    io: ?Io = null,

    /// Pin the slot so its io cannot be rewritten. Fails if it has no users.
    fn pin(s: *Slot) bool {
        var c = s.count.load(.acquire);
        while (c != 0) {
            c = s.count.cmpxchgWeak(c, c + 1, .seq_cst, .acquire) orelse return true;
        }
        return false;
    }

    fn unpin(s: *Slot) void {
        _ = s.count.fetchSub(1, .release);
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
    for (&p.slots) |*s| {
        if (s.tryPin(node.io)) return .{ .slot = s };
    }

    p.lockDir();
    defer p.unlockDir();

    for (&p.slots) |*s| {
        if (s.tryPin(node.io)) return .{ .slot = s };
    }
    for (&p.slots) |*s| {
        if (s.count.load(.monotonic) == 0) {
            s.io = node.io;
            s.count.store(1, .seq_cst);
            return .{ .slot = s };
        }
    }
    node.next = p.overflow.load(.monotonic);
    p.overflow.store(node, .release);
    return .{ .node = node };
}

pub fn leave(p: *Parker, ref: Ref) void {
    switch (ref) {
        .slot => |s| {
            // Last one out clears the slot, returning the parker to its init
            // state (users may compare a quiesced primitive against .init).
            if (s.count.fetchSub(1, .release) == 1) {
                p.lockDir();
                defer p.unlockDir();
                if (s.count.load(.monotonic) == 0) s.io = null;
            }
        },
        .node => |n| {
            p.lockDir();
            defer p.unlockDir();
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
    if (p.overflow.load(.acquire) != null) {
        p.lockDir();
        defer p.unlockDir();
        var cur = p.overflow.load(.monotonic);
        while (cur) |n| : (cur = n.next) {
            n.io.futexWake(u32, &p.word.raw, max_waiters);
        }
    }
}

fn lockDir(p: *Parker) void {
    while (p.dir_lock.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
        std.atomic.spinLoopHint();
    }
}

fn unlockDir(p: *Parker) void {
    p.dir_lock.store(false, .release);
}

fn ioEql(a: Io, b: Io) bool {
    return a.userdata == b.userdata and a.vtable == b.vtable;
}
