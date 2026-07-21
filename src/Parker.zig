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
        // Seq_cst pairs with wake()'s directory scan: a waker that misses
        // this store has already written the word, so wait()'s pre-check
        // sees it.
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
    // Seq_cst for the same reason as tryClaim's count store: a waker whose
    // null check misses this node has already written the word, so wait()'s
    // pre-check sees it.
    p.overflow.store(node, .seq_cst);
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

/// Parks on the word through `io` while it still holds `expect`. The seq_cst
/// pre-check pairs with `wake`'s directory scan: a waker whose scan missed
/// this waiter's registration has already written the word, so the load sees
/// it and we return instead of sleeping. This keeps the whole ordering
/// argument inside the parker; the io's futexWait only needs the plain
/// same-io wait/wake contract.
pub fn wait(p: *Parker, io: Io, expect: u32) Io.Cancelable!void {
    if (p.word.load(.seq_cst) != expect) return;
    return io.futexWait(u32, &p.word.raw, expect);
}

/// Same as `wait` with a deadline.
pub fn waitTimeout(p: *Parker, io: Io, expect: u32, deadline: Io.Timeout) Io.Cancelable!void {
    if (p.word.load(.seq_cst) != expect) return;
    return io.futexWaitTimeout(u32, &p.word.raw, expect, deadline);
}

/// Same as `wait`, but ignores cancellation.
pub fn waitUncancelable(p: *Parker, io: Io, expect: u32) void {
    if (p.word.load(.seq_cst) != expect) return;
    io.futexWaitUncancelable(u32, &p.word.raw, expect);
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
    if (p.overflow.load(.seq_cst) == null) return;

    // Copy the distinct ios out so futexWake runs outside the lock. One wake
    // per io reaches every sleeper. More distinct ios than fit is not a real
    // case; wake the rest under the lock if it ever happens.
    var ios: [8]Io = undefined;
    var len: usize = 0;
    var rest = false;
    {
        Io.Threaded.mutexLock(&p.dir_lock);
        defer Io.Threaded.mutexUnlock(&p.dir_lock);
        var cur = p.overflow.load(.monotonic);
        outer: while (cur) |n| : (cur = n.next) {
            for (ios[0..len]) |io| {
                if (ioEql(io, n.io)) continue :outer;
            }
            if (len == ios.len) {
                rest = true;
                break;
            }
            ios[len] = n.io;
            len += 1;
        }
    }
    for (ios[0..len]) |io| io.futexWake(u32, &p.word.raw, max_waiters);
    if (rest) {
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
