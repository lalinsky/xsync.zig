// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! Test-only `Io` wrapper around `std.Io.Threaded` with a private futex
//! namespace. Every kernel-futex-backed io shares one wait/wake namespace, so
//! a wake routed through the wrong io still reaches the sleeper and hides
//! routing bugs. Here waiters park on a per-instance proxy word instead, so a
//! wake through another instance provably wakes nothing.

const std = @import("std");
const Io = std.Io;

const TestIo = @This();

threaded: Io.Threaded,
vtable: Io.VTable,
table_lock: Io.Mutex = .init,
proxies: [32]Proxy = @splat(.{}),

const Proxy = struct {
    addr: usize = 0,
    seq: std.atomic.Value(u32) = .init(0),
};

pub fn init(gpa: std.mem.Allocator) TestIo {
    var t: TestIo = .{
        .threaded = .init(gpa, .{}),
        .vtable = undefined,
    };
    t.vtable = t.threaded.io().vtable.*;
    t.vtable.futexWait = futexWait;
    t.vtable.futexWaitUncancelable = futexWaitUncancelable;
    t.vtable.futexWake = futexWake;
    return t;
}

pub fn deinit(t: *TestIo) void {
    t.threaded.deinit();
}

pub fn io(t: *TestIo) Io {
    return .{ .userdata = &t.threaded, .vtable = &t.vtable };
}

fn fromUserdata(userdata: ?*anyopaque) *TestIo {
    const threaded: *Io.Threaded = @ptrCast(@alignCast(userdata.?));
    return @fieldParentPtr("threaded", threaded);
}

/// Both the word check here and the seq bump in `wake` run under the table
/// lock, which gives the check-and-sleep vs. wake atomicity the futex
/// contract requires; the kernel futex on the proxy covers the gap between
/// unlocking and actually sleeping.
fn prepare(t: *TestIo, ptr: *const u32, expected: u32) ?struct { proxy: *Proxy, seq: u32 } {
    Io.Threaded.mutexLock(&t.table_lock);
    defer Io.Threaded.mutexUnlock(&t.table_lock);
    if (@atomicLoad(u32, ptr, .seq_cst) != expected) return null;
    const addr = @intFromPtr(ptr);
    for (&t.proxies) |*p| {
        if (p.addr == addr or p.addr == 0) {
            p.addr = addr;
            return .{ .proxy = p, .seq = p.seq.load(.monotonic) };
        }
    }
    @panic("TestIo proxy pool exhausted");
}

fn futexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: Io.Timeout) Io.Cancelable!void {
    const t = fromUserdata(userdata);
    const w = t.prepare(ptr, expected) orelse return;
    return t.threaded.io().futexWaitTimeout(u32, &w.proxy.seq.raw, w.seq, timeout);
}

fn futexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
    const t = fromUserdata(userdata);
    const w = t.prepare(ptr, expected) orelse return;
    t.threaded.io().futexWaitUncancelable(u32, &w.proxy.seq.raw, w.seq);
}

fn futexWake(userdata: ?*anyopaque, ptr: *const u32, max_waiters: u32) void {
    const t = fromUserdata(userdata);
    const addr = @intFromPtr(ptr);
    Io.Threaded.mutexLock(&t.table_lock);
    const proxy = for (&t.proxies) |*p| {
        if (p.addr == addr) break p;
        if (p.addr == 0) break null;
    } else null;
    if (proxy) |p| _ = p.seq.fetchAdd(1, .seq_cst);
    Io.Threaded.mutexUnlock(&t.table_lock);
    if (proxy) |p| t.threaded.io().futexWake(u32, &p.seq.raw, max_waiters);
}

const Event = @import("Event.zig");
const Mutex = @import("Mutex.zig");
const Queue = @import("queue.zig").Queue;
const Cancelable = Io.Cancelable;

test "wakes do not cross instances" {
    const gpa = std.testing.allocator;

    var a: TestIo = .init(gpa);
    defer a.deinit();
    var b: TestIo = .init(gpa);
    defer b.deinit();

    var word: std.atomic.Value(u32) = .init(0);
    var done: std.atomic.Value(bool) = .init(false);

    const T = struct {
        fn waiter(w_io: Io, w: *std.atomic.Value(u32), d: *std.atomic.Value(bool)) Cancelable!void {
            while (w.load(.acquire) == 0) {
                try w_io.futexWait(u32, &w.raw, 0);
            }
            d.store(true, .release);
        }
    };

    var group: Io.Group = .init;
    defer group.cancel(a.io());
    group.concurrent(a.io(), T.waiter, .{ a.io(), &word, &done }) catch return error.SkipZigTest;

    // Give the waiter a moment to park, then wake through the wrong io: the
    // waiter must stay asleep.
    for (0..100_000) |_| std.atomic.spinLoopHint();
    b.io().futexWake(u32, &word.raw, std.math.maxInt(u32));
    for (0..100_000) |_| std.atomic.spinLoopHint();
    try std.testing.expect(!done.load(.acquire));

    // The right io still delivers.
    word.store(1, .release);
    a.io().futexWake(u32, &word.raw, std.math.maxInt(u32));
    try group.await(a.io());
    try std.testing.expect(done.load(.acquire));
}

test "event across incompatible ios" {
    const gpa = std.testing.allocator;

    var a: TestIo = .init(gpa);
    defer a.deinit();
    var b: TestIo = .init(gpa);
    defer b.deinit();

    var e: Event = .init;
    var woken: std.atomic.Value(u32) = .init(0);

    const T = struct {
        fn waiter(w_io: Io, ev: *Event, count: *std.atomic.Value(u32)) Cancelable!void {
            try ev.wait(w_io);
            _ = count.fetchAdd(1, .monotonic);
        }
    };

    var ga: Io.Group = .init;
    defer ga.cancel(a.io());
    var gb: Io.Group = .init;
    defer gb.cancel(b.io());
    ga.concurrent(a.io(), T.waiter, .{ a.io(), &e, &woken }) catch return error.SkipZigTest;
    gb.concurrent(b.io(), T.waiter, .{ b.io(), &e, &woken }) catch return error.SkipZigTest;

    // Wait until at least one waiter parked (word moves to the waiting
    // state), so set() must go through the wake path; a wake that only
    // reaches one instance's namespace hangs the other waiter.
    while (e.parker.word.load(.acquire) == 0) std.atomic.spinLoopHint();
    for (0..100_000) |_| std.atomic.spinLoopHint();
    e.set(a.io());

    try ga.await(a.io());
    try gb.await(b.io());
    try std.testing.expectEqual(2, woken.load(.monotonic));
}

test "mutex contention across incompatible ios" {
    const gpa = std.testing.allocator;

    var a: TestIo = .init(gpa);
    defer a.deinit();
    var b: TestIo = .init(gpa);
    defer b.deinit();

    var m: Mutex = .init;
    var counter: u64 = 0;

    const T = struct {
        fn worker(w_io: Io, mtx: *Mutex, ctr: *u64) Cancelable!void {
            for (0..500) |_| {
                try mtx.lock(w_io);
                ctr.* += 1;
                mtx.unlock(w_io);
            }
        }
    };

    var ga: Io.Group = .init;
    defer ga.cancel(a.io());
    var gb: Io.Group = .init;
    defer gb.cancel(b.io());
    for (0..2) |_| {
        ga.concurrent(a.io(), T.worker, .{ a.io(), &m, &counter }) catch return error.SkipZigTest;
        gb.concurrent(b.io(), T.worker, .{ b.io(), &m, &counter }) catch return error.SkipZigTest;
    }
    try ga.await(a.io());
    try gb.await(b.io());

    try std.testing.expectEqual(2000, counter);
}

test "queue across incompatible ios" {
    const gpa = std.testing.allocator;

    var a: TestIo = .init(gpa);
    defer a.deinit();
    var b: TestIo = .init(gpa);
    defer b.deinit();

    var buf: [1]u64 = undefined;
    var q: Queue(u64) = .init(&buf);
    var sum: u64 = 0;

    const T = struct {
        fn producer(w_io: Io, qq: *Queue(u64)) Cancelable!void {
            for (1..101) |i| qq.putOne(w_io, i) catch return;
            qq.close(w_io);
        }
        fn consumer(w_io: Io, qq: *Queue(u64), total: *u64) Cancelable!void {
            while (true) {
                const v = qq.getOne(w_io) catch return;
                total.* += v;
            }
        }
    };

    var ga: Io.Group = .init;
    defer ga.cancel(a.io());
    var gb: Io.Group = .init;
    defer gb.cancel(b.io());
    ga.concurrent(a.io(), T.producer, .{ a.io(), &q }) catch return error.SkipZigTest;
    gb.concurrent(b.io(), T.consumer, .{ b.io(), &q, &sum }) catch return error.SkipZigTest;
    try ga.await(a.io());
    try gb.await(b.io());

    try std.testing.expectEqual(5050, sum);
}
