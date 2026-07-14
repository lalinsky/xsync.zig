// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! Bounded MPMC byte/element queue with the std.Io.Queue API and transfer
//! logic, but a different synchronization layer: the queue state is guarded
//! by an OS-level mutex (never parking a task), and each pending operation
//! parks on its own futex word through its own `Io`, so producers and
//! consumers can live on different `Io` instances.
//!
//! Wakes happen outside the mutex. A completed waiter is unlinked and chained
//! onto a local wake list under the lock, and may not return until its futex
//! word is set, so the deferred store cannot touch a dead frame.

const std = @import("std");
const Io = std.Io;
const Threaded = Io.Threaded;
const Cancelable = Io.Cancelable;
const assert = std.debug.assert;

pub const QueueClosedError = error{Closed};

pub const TypeErasedQueue = struct {
    /// Locked via Threaded.mutexLock: blocks the calling thread, not the task.
    /// Critical sections are short and never hold the lock across a park.
    mutex: Io.Mutex,
    closed: bool,

    /// Ring buffer. This data is logically *after* queued getters.
    buffer: []u8,
    start: usize,
    len: usize,

    putters: std.DoublyLinkedList,
    getters: std.DoublyLinkedList,

    const Waiter = struct {
        io: Io,
        futex: std.atomic.Value(u32),
        node: std.DoublyLinkedList.Node,
        queued: bool,
    };

    const Put = struct {
        remaining: []const u8,
        needed: usize,
        waiter: Waiter,
    };

    const Get = struct {
        remaining: []u8,
        needed: usize,
        waiter: Waiter,
    };

    fn waiterOf(node: *std.DoublyLinkedList.Node) *Waiter {
        return @alignCast(@fieldParentPtr("node", node));
    }

    fn putOf(node: *std.DoublyLinkedList.Node) *Put {
        return @alignCast(@fieldParentPtr("waiter", waiterOf(node)));
    }

    fn getOf(node: *std.DoublyLinkedList.Node) *Get {
        return @alignCast(@fieldParentPtr("waiter", waiterOf(node)));
    }

    /// Moves a completed waiter from its wait list onto `wakes`. Its node is
    /// safe to reuse: the waiter cannot return until the futex word is set.
    fn chainWake(list: *std.DoublyLinkedList, wakes: *std.DoublyLinkedList, node: *std.DoublyLinkedList.Node) void {
        list.remove(node);
        waiterOf(node).queued = false;
        wakes.append(node);
    }

    /// Delivers chained wakes. Must be called after releasing the mutex.
    fn drainWakes(wakes: *std.DoublyLinkedList) void {
        while (wakes.popFirst()) |node| {
            const waiter = waiterOf(node);
            const io = waiter.io;
            const futex = &waiter.futex.raw;
            @atomicStore(u32, futex, 1, .release);
            io.futexWake(u32, futex, 1);
        }
    }

    /// Blocks until an in-flight wake has stored the futex word, so the
    /// pending frame can be safely destroyed. Called with the mutex held.
    fn awaitWake(q: *TypeErasedQueue, io: Io, futex: *std.atomic.Value(u32)) void {
        if (futex.load(.acquire) != 0) return;
        Threaded.mutexUnlock(&q.mutex);
        while (futex.load(.acquire) == 0) {
            io.futexWaitUncancelable(u32, &futex.raw, 0);
        }
        Threaded.mutexLock(&q.mutex);
    }

    pub fn init(buffer: []u8) TypeErasedQueue {
        return .{
            .mutex = .init,
            .closed = false,
            .buffer = buffer,
            .start = 0,
            .len = 0,
            .putters = .{},
            .getters = .{},
        };
    }

    /// After this is called, the queue enters a "closed" state. A closed
    /// queue always returns `error.Closed` for put attempts even when
    /// there is space in the buffer. However, existing elements of the
    /// queue are retrieved before `error.Closed` is returned.
    ///
    /// Idempotent. Threadsafe.
    pub fn close(q: *TypeErasedQueue, io: Io) void {
        _ = io;
        var wakes: std.DoublyLinkedList = .{};
        Threaded.mutexLock(&q.mutex);
        q.closed = true;
        while (q.getters.first) |node| chainWake(&q.getters, &wakes, node);
        while (q.putters.first) |node| chainWake(&q.putters, &wakes, node);
        Threaded.mutexUnlock(&q.mutex);
        drainWakes(&wakes);
    }

    pub fn put(q: *TypeErasedQueue, io: Io, elements: []const u8, min: usize) (QueueClosedError || Cancelable)!usize {
        assert(elements.len >= min);
        if (elements.len == 0) return 0;
        try io.checkCancel();
        var wakes: std.DoublyLinkedList = .{};
        Threaded.mutexLock(&q.mutex);
        const result = q.putLocked(io, elements, min, false, &wakes);
        Threaded.mutexUnlock(&q.mutex);
        drainWakes(&wakes);
        return result;
    }

    /// Same as `put`, except does not introduce a cancelation point.
    ///
    /// For a description of cancelation and cancelation points, see `Future.cancel`.
    pub fn putUncancelable(q: *TypeErasedQueue, io: Io, elements: []const u8, min: usize) QueueClosedError!usize {
        assert(elements.len >= min);
        if (elements.len == 0) return 0;
        var wakes: std.DoublyLinkedList = .{};
        Threaded.mutexLock(&q.mutex);
        const result = q.putLocked(io, elements, min, true, &wakes);
        Threaded.mutexUnlock(&q.mutex);
        drainWakes(&wakes);
        return result catch |err| switch (err) {
            error.Canceled => unreachable,
            error.Closed => |e| return e,
        };
    }

    fn puttableSlice(q: *const TypeErasedQueue) ?[]u8 {
        const unwrapped_index = q.start + q.len;
        const wrapped_index, const overflow = @subWithOverflow(unwrapped_index, q.buffer.len);
        const slice = switch (overflow) {
            1 => q.buffer[unwrapped_index..],
            0 => q.buffer[wrapped_index..q.start],
        };
        return if (slice.len > 0) slice else null;
    }

    fn putLocked(q: *TypeErasedQueue, io: Io, elements: []const u8, min: usize, uncancelable: bool, wakes: *std.DoublyLinkedList) (QueueClosedError || Cancelable)!usize {
        // A closed queue cannot be added to, even if there is space in the buffer.
        if (q.closed) return error.Closed;

        // Getters have first priority on the data, and only when the getters
        // queue is empty do we start populating the buffer.

        // The number of elements we add immediately, before possibly blocking.
        var n: usize = 0;

        while (q.getters.first) |getter_node| {
            const getter = getOf(getter_node);
            const copy_len = @min(getter.remaining.len, elements.len - n);
            assert(copy_len > 0);
            @memcpy(getter.remaining[0..copy_len], elements[n..][0..copy_len]);
            getter.remaining = getter.remaining[copy_len..];
            getter.needed -|= copy_len;
            n += copy_len;
            if (getter.needed == 0) {
                chainWake(&q.getters, wakes, getter_node);
            } else {
                assert(n == elements.len); // we didn't have enough elements for the getter
            }
            if (n == elements.len) return elements.len;
        }

        while (q.puttableSlice()) |slice| {
            const copy_len = @min(slice.len, elements.len - n);
            assert(copy_len > 0);
            @memcpy(slice[0..copy_len], elements[n..][0..copy_len]);
            q.len += copy_len;
            n += copy_len;
            if (n == elements.len) return elements.len;
        }

        // Don't block if we hit the min.
        if (n >= min) return n;

        var pending: Put = .{
            .remaining = elements[n..],
            .needed = min - n,
            .waiter = .{ .io = io, .futex = .init(0), .node = .{}, .queued = true },
        };
        q.putters.append(&pending.waiter.node);
        defer if (pending.waiter.queued) q.putters.remove(&pending.waiter.node);

        while (pending.needed > 0 and !q.closed) {
            Threaded.mutexUnlock(&q.mutex);
            const result = if (uncancelable) blk: {
                io.futexWaitUncancelable(u32, &pending.waiter.futex.raw, 0);
                break :blk {};
            } else io.futexWait(u32, &pending.waiter.futex.raw, 0);
            Threaded.mutexLock(&q.mutex);
            result catch |err| switch (err) {
                error.Canceled => {
                    // If we were completed or the queue closed, a wake is in
                    // flight; it must land before this frame goes away.
                    if (!pending.waiter.queued) q.awaitWake(io, &pending.waiter.futex);
                    if (pending.remaining.len == elements.len) {
                        // Canceled while waiting, and appended no elements.
                        return error.Canceled;
                    }
                    // Canceled while waiting, but appended some elements, so report those first.
                    io.recancel();
                    return elements.len - pending.remaining.len;
                },
            };
        }
        if (!pending.waiter.queued) q.awaitWake(io, &pending.waiter.futex);
        if (pending.remaining.len == elements.len) {
            // The queue was closed while we were waiting. We appended no elements.
            assert(q.closed);
            return error.Closed;
        }
        return elements.len - pending.remaining.len;
    }

    pub fn get(q: *TypeErasedQueue, io: Io, buffer: []u8, min: usize) (QueueClosedError || Cancelable)!usize {
        assert(buffer.len >= min);
        if (buffer.len == 0) return 0;
        try io.checkCancel();
        var wakes: std.DoublyLinkedList = .{};
        Threaded.mutexLock(&q.mutex);
        const result = q.getLocked(io, buffer, min, false, &wakes);
        Threaded.mutexUnlock(&q.mutex);
        drainWakes(&wakes);
        return result;
    }

    /// Same as `get`, except does not introduce a cancelation point.
    ///
    /// For a description of cancelation and cancelation points, see `Future.cancel`.
    pub fn getUncancelable(q: *TypeErasedQueue, io: Io, buffer: []u8, min: usize) QueueClosedError!usize {
        assert(buffer.len >= min);
        if (buffer.len == 0) return 0;
        var wakes: std.DoublyLinkedList = .{};
        Threaded.mutexLock(&q.mutex);
        const result = q.getLocked(io, buffer, min, true, &wakes);
        Threaded.mutexUnlock(&q.mutex);
        drainWakes(&wakes);
        return result catch |err| switch (err) {
            error.Canceled => unreachable,
            error.Closed => |e| return e,
        };
    }

    fn gettableSlice(q: *const TypeErasedQueue) ?[]const u8 {
        const overlong_slice = q.buffer[q.start..];
        const slice = overlong_slice[0..@min(overlong_slice.len, q.len)];
        return if (slice.len > 0) slice else null;
    }

    fn getLocked(q: *TypeErasedQueue, io: Io, buffer: []u8, min: usize, uncancelable: bool, wakes: *std.DoublyLinkedList) (QueueClosedError || Cancelable)!usize {
        // The ring buffer gets first priority, then data should come from any
        // queued putters, then finally the ring buffer should be filled with
        // data from putters so they can be resumed.

        // The number of elements we received immediately, before possibly blocking.
        var n: usize = 0;

        while (q.gettableSlice()) |slice| {
            const copy_len = @min(slice.len, buffer.len - n);
            assert(copy_len > 0);
            @memcpy(buffer[n..][0..copy_len], slice[0..copy_len]);
            q.start += copy_len;
            if (q.buffer.len - q.start == 0) q.start = 0;
            q.len -= copy_len;
            n += copy_len;
            if (n == buffer.len) {
                q.fillRingBufferFromPutters(wakes);
                return buffer.len;
            }
        }

        // Copy directly from putters into buffer.
        while (q.putters.first) |putter_node| {
            const putter = putOf(putter_node);
            const copy_len = @min(putter.remaining.len, buffer.len - n);
            assert(copy_len > 0);
            @memcpy(buffer[n..][0..copy_len], putter.remaining[0..copy_len]);
            putter.remaining = putter.remaining[copy_len..];
            putter.needed -|= copy_len;
            n += copy_len;
            if (putter.needed == 0) {
                chainWake(&q.putters, wakes, putter_node);
            } else {
                assert(n == buffer.len); // we didn't have enough space for the putter
            }
            if (n == buffer.len) {
                q.fillRingBufferFromPutters(wakes);
                return buffer.len;
            }
        }

        // No need to call `fillRingBufferFromPutters` from this point onwards,
        // because we emptied the ring buffer *and* the putter queue!

        // Don't block if we hit the min or if the queue is closed. Return how
        // many elements we could get immediately, unless the queue was closed and
        // empty, in which case report `error.Closed`.
        if (n == 0 and q.closed) return error.Closed;
        if (n >= min or q.closed) return n;

        var pending: Get = .{
            .remaining = buffer[n..],
            .needed = min - n,
            .waiter = .{ .io = io, .futex = .init(0), .node = .{}, .queued = true },
        };
        q.getters.append(&pending.waiter.node);
        defer if (pending.waiter.queued) q.getters.remove(&pending.waiter.node);

        while (pending.needed > 0 and !q.closed) {
            Threaded.mutexUnlock(&q.mutex);
            const result = if (uncancelable) blk: {
                io.futexWaitUncancelable(u32, &pending.waiter.futex.raw, 0);
                break :blk {};
            } else io.futexWait(u32, &pending.waiter.futex.raw, 0);
            Threaded.mutexLock(&q.mutex);
            result catch |err| switch (err) {
                error.Canceled => {
                    // If we were completed or the queue closed, a wake is in
                    // flight; it must land before this frame goes away.
                    if (!pending.waiter.queued) q.awaitWake(io, &pending.waiter.futex);
                    if (pending.remaining.len == buffer.len) {
                        // Canceled while waiting, and received no elements.
                        return error.Canceled;
                    }
                    // Canceled while waiting, but received some elements, so report those first.
                    io.recancel();
                    return buffer.len - pending.remaining.len;
                },
            };
        }
        if (!pending.waiter.queued) q.awaitWake(io, &pending.waiter.futex);
        if (pending.remaining.len == buffer.len) {
            // The queue was closed while we were waiting. We received no elements.
            assert(q.closed);
            return error.Closed;
        }
        return buffer.len - pending.remaining.len;
    }

    /// Called when there is nonzero space available in the ring buffer and
    /// potentially putters waiting. The mutex is already held and the task is
    /// to copy putter data to the ring buffer and chain wakes for any putters
    /// whose buffers have been fully copied.
    fn fillRingBufferFromPutters(q: *TypeErasedQueue, wakes: *std.DoublyLinkedList) void {
        while (q.putters.first) |putter_node| {
            const putter = putOf(putter_node);
            while (q.puttableSlice()) |slice| {
                const copy_len = @min(slice.len, putter.remaining.len);
                assert(copy_len > 0);
                @memcpy(slice[0..copy_len], putter.remaining[0..copy_len]);
                q.len += copy_len;
                putter.remaining = putter.remaining[copy_len..];
                putter.needed -|= copy_len;
                if (putter.needed == 0) {
                    chainWake(&q.putters, wakes, putter_node);
                    break;
                }
            } else {
                break;
            }
        }
    }
};
/// Many producer, many consumer, thread-safe, runtime configurable buffer size.
/// When buffer is empty, consumers suspend and are resumed by producers.
/// When buffer is full, producers suspend and are resumed by consumers.
pub fn Queue(Elem: type) type {
    return struct {
        type_erased: TypeErasedQueue,

        pub fn init(buffer: []Elem) @This() {
            return .{ .type_erased = .init(@ptrCast(buffer)) };
        }

        /// After this is called, the queue enters a "closed" state. A closed
        /// queue always returns `error.Closed` for put attempts even when
        /// there is space in the buffer. However, existing elements of the
        /// queue are retrieved before `error.Closed` is returned.
        ///
        /// Threadsafe.
        pub fn close(q: *@This(), io: Io) void {
            q.type_erased.close(io);
        }

        /// Appends elements to the end of the queue, potentially blocking if
        /// there is insufficient capacity. Returns when any one of the
        /// following conditions is satisfied:
        ///
        /// * At least `min` elements have been added to the queue
        /// * The queue is closed
        /// * The current task is canceled
        ///
        /// Returns how many of `elements` have been added to the queue, if any.
        /// If an error is returned, no elements have been added.
        ///
        /// If the queue is closed or the task is canceled, but some items were
        /// already added before the closure or cancelation, then `put` may
        /// return a number lower than `min`, in which case future calls are
        /// guaranteed to return `error.Canceled` or `error.Closed`.
        ///
        /// A return value of 0 is only possible if `min` is 0, in which case
        /// the call is guaranteed to queue as many of `elements` as is possible
        /// *without* blocking.
        ///
        /// Asserts that `elements.len >= min`.
        pub fn put(q: *@This(), io: Io, elements: []const Elem, min: usize) (QueueClosedError || Cancelable)!usize {
            return @divExact(try q.type_erased.put(io, @ptrCast(elements), min * @sizeOf(Elem)), @sizeOf(Elem));
        }

        /// Same as `put` but blocks until all elements have been added to the queue.
        ///
        /// If the queue is closed or canceled, `error.Closed` or `error.Canceled`
        /// is returned, and it is unspecified how many, if any, of `elements` were
        /// added to the queue prior to cancelation or closure.
        pub fn putAll(q: *@This(), io: Io, elements: []const Elem) (QueueClosedError || Cancelable)!void {
            const n = try q.put(io, elements, elements.len);
            if (n != elements.len) {
                _ = try q.put(io, elements[n..], elements.len - n);
                unreachable; // partial `put` implies queue was closed or we were canceled
            }
        }

        /// Same as `put`, except does not introduce a cancelation point.
        ///
        /// For a description of cancelation and cancelation points, see `Future.cancel`.
        pub fn putUncancelable(q: *@This(), io: Io, elements: []const Elem, min: usize) QueueClosedError!usize {
            return @divExact(try q.type_erased.putUncancelable(io, @ptrCast(elements), min * @sizeOf(Elem)), @sizeOf(Elem));
        }

        /// Appends `item` to the end of the queue, blocking if the queue is full.
        pub fn putOne(q: *@This(), io: Io, item: Elem) (QueueClosedError || Cancelable)!void {
            assert(try q.put(io, &.{item}, 1) == 1);
        }

        /// Same as `putOne`, except does not introduce a cancelation point.
        ///
        /// For a description of cancelation and cancelation points, see `Future.cancel`.
        pub fn putOneUncancelable(q: *@This(), io: Io, item: Elem) QueueClosedError!void {
            assert(try q.putUncancelable(io, &.{item}, 1) == 1);
        }

        /// Receives elements from the beginning of the queue, potentially blocking
        /// if there are insufficient elements currently in the queue. Returns when
        /// any one of the following conditions is satisfied:
        ///
        /// * At least `min` elements have been received from the queue
        /// * The queue is closed and contains no buffered elements
        /// * The current task is canceled
        ///
        /// Returns how many elements of `buffer` have been populated, if any.
        /// If an error is returned, no elements have been populated.
        ///
        /// If the queue is closed or the task is canceled, but some items were
        /// already received before the closure or cancelation, then `get` may
        /// return a number lower than `min`, in which case future calls are
        /// guaranteed to return `error.Canceled` or `error.Closed`.
        ///
        /// A return value of 0 is only possible if `min` is 0, in which case
        /// the call is guaranteed to fill as much of `buffer` as is possible
        /// *without* blocking.
        ///
        /// Asserts that `buffer.len >= min`.
        pub fn get(q: *@This(), io: Io, buffer: []Elem, min: usize) (QueueClosedError || Cancelable)!usize {
            return @divExact(try q.type_erased.get(io, @ptrCast(buffer), min * @sizeOf(Elem)), @sizeOf(Elem));
        }

        /// Same as `get`, except does not introduce a cancelation point.
        ///
        /// For a description of cancelation and cancelation points, see `Future.cancel`.
        pub fn getUncancelable(q: *@This(), io: Io, buffer: []Elem, min: usize) QueueClosedError!usize {
            return @divExact(try q.type_erased.getUncancelable(io, @ptrCast(buffer), min * @sizeOf(Elem)), @sizeOf(Elem));
        }

        /// Receives one element from the beginning of the queue, blocking if the queue is empty.
        pub fn getOne(q: *@This(), io: Io) (QueueClosedError || Cancelable)!Elem {
            var buf: [1]Elem = undefined;
            assert(try q.get(io, &buf, 1) == 1);
            return buf[0];
        }

        /// Same as `getOne`, except does not introduce a cancelation point.
        ///
        /// For a description of cancelation and cancelation points, see `Future.cancel`.
        pub fn getOneUncancelable(q: *@This(), io: Io) QueueClosedError!Elem {
            var buf: [1]Elem = undefined;
            assert(try q.getUncancelable(io, &buf, 1) == 1);
            return buf[0];
        }

        /// Returns buffer length in `Elem` units.
        pub fn capacity(q: *const @This()) usize {
            return @divExact(q.type_erased.buffer.len, @sizeOf(Elem));
        }
    };
}

test "buffered put/get" {
    const io = std.testing.io;

    var buf: [4]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    try q.putOne(io, 1);
    try q.putOne(io, 2);
    try std.testing.expectEqual(1, try q.getOne(io));
    try std.testing.expectEqual(2, try q.getOne(io));

    // min=0 never blocks
    var out: [4]u64 = undefined;
    try std.testing.expectEqual(0, try q.get(io, &out, 0));
}

test "close drains buffer then reports Closed" {
    const io = std.testing.io;

    var buf: [4]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    try q.putOne(io, 7);
    q.close(io);

    try std.testing.expectError(error.Closed, q.putOne(io, 8));
    try std.testing.expectEqual(7, try q.getOne(io));
    try std.testing.expectError(error.Closed, q.getOne(io));
}

test "blocking producer/consumer" {
    const io = std.testing.io;

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

    var group: Io.Group = .init;
    group.concurrent(io, T.producer, .{ io, &q }) catch return error.SkipZigTest;
    group.concurrent(io, T.consumer, .{ io, &q, &sum }) catch return error.SkipZigTest;
    try group.await(io);

    try std.testing.expectEqual(5050, sum);
}

test "batch put and min get" {
    const io = std.testing.io;

    var buf: [8]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    try q.putAll(io, &.{ 1, 2, 3, 4, 5 });
    var out: [8]u64 = undefined;
    // min=2: returns at least 2, up to whatever is buffered
    const got = try q.get(io, &out, 2);
    try std.testing.expect(got >= 2 and got <= 5);
}
