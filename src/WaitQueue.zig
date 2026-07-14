// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! Intrusive FIFO wait queue keyed by a single word: a head pointer plus a
//! sticky flag and a mutation spinlock in the low bits. The tail lives in the
//! head node's `tail` field, so appends are O(1) with no second atomic.

const std = @import("std");

const Io = std.Io;
const Cancelable = Io.Cancelable;

const WaitQueue = @This();

const safety = std.debug.runtime_safety;

head: std.atomic.Value(usize),

const flag_bit: usize = 0b01;
const lock_bit: usize = 0b10;
const ptr_mask: usize = ~@as(usize, 0b11);

pub const empty: WaitQueue = .{ .head = .init(0) };
pub const flagged: WaitQueue = .{ .head = .init(flag_bit) };

/// `push` behavior when the flag is set and the queue is empty.
pub const Vacancy = enum { queue, acquire };
/// `pop` behavior when the queue is empty.
pub const OnEmpty = enum { keep_flag, set_flag };
pub const PushResult = enum { queued, acquired };
pub const PopResult = struct { node: *Node, is_last: bool };

/// A parked task. Lives on the waiting task's stack; the `Io` it parked on
/// rides along so the waker reaches it through the right instance.
pub const Node = struct {
    prev: ?*Node = null,
    next: ?*Node = null,
    tail: *Node = undefined, // valid only while this node is the head
    in_list: if (safety) bool else void = if (safety) false else {},

    io: Io,
    futex: std.atomic.Value(u32) = .init(0),

    pub fn wake(node: *Node) void {
        // Read `io` before the store: once it lands the node may be freed. The
        // futex address is only a wake key, never dereferenced.
        const io = node.io;
        const futex = &node.futex.raw;
        @atomicStore(u32, futex, 1, .release);
        io.futexWake(u32, futex, 1);
    }

    pub fn wait(node: *Node) Cancelable!void {
        while (node.futex.load(.acquire) == 0) {
            try node.io.futexWait(u32, &node.futex.raw, 0);
        }
    }

    pub fn waitUncancelable(node: *Node) void {
        while (node.futex.load(.acquire) == 0) {
            node.io.futexWaitUncancelable(u32, &node.futex.raw, 0);
        }
    }

    /// Caller distinguishes wake from timeout by trying to `remove` itself.
    pub fn timedWait(node: *Node, deadline: Io.Timeout) Cancelable!void {
        while (node.futex.load(.acquire) == 0) {
            try node.io.futexWaitTimeout(u32, &node.futex.raw, 0, deadline);
            if (node.futex.load(.acquire) != 0) return;
            const left = deadline.toDurationFromNow(node.io) orelse return;
            if (left.raw.nanoseconds <= 0) return;
        }
    }
};

pub fn hasWaiters(q: *const WaitQueue) bool {
    return q.head.load(.acquire) & ptr_mask != 0;
}

pub fn isFlagSet(q: *const WaitQueue) bool {
    return q.head.load(.acquire) & flag_bit != 0;
}

/// Clears the flag iff set with no waiters (uncontended lock fast path).
pub fn tryClearFlag(q: *WaitQueue) bool {
    return q.head.cmpxchgStrong(flag_bit, 0, .acq_rel, .acquire) == null;
}

/// Enqueues `node`, or with `.acquire` takes the flag if it is free.
pub fn push(q: *WaitQueue, node: *Node, comptime vacancy: Vacancy) PushResult {
    const old = q.acquire();
    if (vacancy == .acquire and old == flag_bit) {
        q.head.store(0, .release);
        return .acquired;
    }
    q.pushLocked(old, node);
    return .queued;
}

/// Pops the head node, handling the empty case per `on_empty`.
pub fn pop(q: *WaitQueue, comptime on_empty: OnEmpty) ?*Node {
    const old = q.acquire();
    const head = fromState(old) orelse {
        switch (on_empty) {
            .keep_flag => q.release(),
            .set_flag => q.head.store(flag_bit, .release),
        }
        return null;
    };
    return q.popLocked(old, head, false);
}

/// Like `pop`, but forces the flag set and reports whether this was the last
/// node, letting a wake-all loop stop without a final empty pop.
pub fn popAndSetFlag(q: *WaitQueue) ?PopResult {
    const old = q.acquire();
    const head = fromState(old) orelse {
        q.head.store(flag_bit, .release);
        return null;
    };
    const is_last = head.next == null;
    return .{ .node = q.popLocked(old, head, true), .is_last = is_last };
}

/// Removes a specific node. Preserves the flag, keeping cancel races safe.
pub fn remove(q: *WaitQueue, node: *Node) bool {
    const old = q.acquire();
    const head = fromState(old) orelse {
        q.release();
        return false;
    };
    const tail = head.tail;

    // Not queued if it is neither an end nor an interior node.
    if (node.prev == null and head != node) {
        q.release();
        return false;
    }
    if (node.next == null and tail != node) {
        q.release();
        return false;
    }

    if (safety) {
        std.debug.assert(node.in_list);
        node.in_list = false;
    }

    const prev = node.prev;
    const next = node.next;
    node.prev = null;
    node.next = null;
    if (prev) |p| p.next = next;
    if (next) |n| n.prev = prev;

    if (head == node) {
        if (next) |n| {
            n.tail = tail;
            q.head.store(makeState(n, old), .release);
        } else {
            q.head.store(old & flag_bit, .release);
        }
    } else {
        if (tail == node) head.tail = prev.?;
        q.release();
    }
    return true;
}

/// Enqueues `node` unless the flag is set. Returns whether it was enqueued.
pub fn pushUnlessFlag(q: *WaitQueue, node: *Node) bool {
    const old = q.acquire();
    if (old & flag_bit != 0) {
        q.release();
        return false;
    }
    q.pushLocked(old, node);
    return true;
}

/// Clears the flag. Assumes no waiters are queued.
pub fn clearFlag(q: *WaitQueue) void {
    _ = q.head.fetchAnd(~flag_bit, .release);
}

// --- internals ---------------------------------------------------------------

/// Spins until the mutation lock is held, returning the pre-lock state.
fn acquire(q: *WaitQueue) usize {
    while (true) {
        const old = q.head.fetchOr(lock_bit, .acquire);
        if (old & lock_bit == 0) return old;
        std.atomic.spinLoopHint();
    }
}

fn release(q: *WaitQueue) void {
    _ = q.head.fetchAnd(~lock_bit, .release);
}

fn fromState(state: usize) ?*Node {
    const ptr = state & ptr_mask;
    return if (ptr == 0) null else @ptrFromInt(ptr);
}

fn makeState(node: *Node, keep_flag: usize) usize {
    const addr = @intFromPtr(node);
    std.debug.assert(addr & 0b11 == 0);
    return addr | (keep_flag & flag_bit);
}

fn pushLocked(q: *WaitQueue, old: usize, node: *Node) void {
    if (safety) {
        std.debug.assert(!node.in_list);
        node.in_list = true;
    }
    node.next = null;
    node.prev = null;

    if (fromState(old)) |head| {
        const tail = head.tail;
        tail.next = node;
        node.prev = tail;
        head.tail = node;
        q.release();
    } else {
        node.tail = node;
        q.head.store(makeState(node, old), .release);
    }
}

fn popLocked(q: *WaitQueue, old: usize, head: *Node, comptime set_flag: bool) *Node {
    const next = head.next;
    if (safety) {
        std.debug.assert(head.in_list);
        head.in_list = false;
    }
    head.next = null;
    head.prev = null;

    const flag = if (set_flag) flag_bit else (old & flag_bit);
    if (next) |new_head| {
        new_head.tail = head.tail;
        new_head.prev = null;
        q.head.store(makeState(new_head, flag), .release);
    } else {
        q.head.store(flag, .release);
    }
    return head;
}

test "FIFO push/pop" {
    var q: WaitQueue = .empty;

    var a: Node = .{ .io = undefined };
    var b: Node = .{ .io = undefined };
    var c: Node = .{ .io = undefined };

    _ = q.push(&a, .queue);
    _ = q.push(&b, .queue);
    _ = q.push(&c, .queue);

    try std.testing.expectEqual(&a, q.pop(.keep_flag));
    try std.testing.expectEqual(&b, q.pop(.keep_flag));
    try std.testing.expectEqual(&c, q.pop(.keep_flag));
    try std.testing.expectEqual(@as(?*Node, null), q.pop(.keep_flag));
}

test "remove is position independent and idempotent" {
    var q: WaitQueue = .empty;

    var a: Node = .{ .io = undefined };
    var b: Node = .{ .io = undefined };
    var c: Node = .{ .io = undefined };
    _ = q.push(&a, .queue);
    _ = q.push(&b, .queue);
    _ = q.push(&c, .queue);

    try std.testing.expect(q.remove(&b)); // middle
    try std.testing.expect(!q.remove(&b)); // already gone
    try std.testing.expect(q.remove(&c)); // tail
    try std.testing.expect(q.remove(&a)); // last one
    try std.testing.expect(!q.hasWaiters());
}

test "flag acquire/release fast paths" {
    var q: WaitQueue = .flagged;

    var a: Node = .{ .io = undefined };
    try std.testing.expectEqual(.acquired, q.push(&a, .acquire));
    try std.testing.expect(!q.isFlagSet());

    var b: Node = .{ .io = undefined };
    try std.testing.expectEqual(.queued, q.push(&b, .acquire));
    try std.testing.expect(!q.tryClearFlag());

    try std.testing.expectEqual(&b, q.pop(.set_flag));
    try std.testing.expectEqual(@as(?*Node, null), q.pop(.set_flag));
    try std.testing.expect(q.isFlagSet());
    try std.testing.expect(q.tryClearFlag());
}
