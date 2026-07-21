// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! Bounded MPMC byte/element queue with the std.Io.Queue API and transfer
//! logic, but a different synchronization layer: the queue state is guarded
//! by an OS-level mutex, and each pending operation parks on its own futex
//! word through its own `Io`, so producers and consumers can live on
//! different `Io` instances.

const std = @import("std");
const Io = std.Io;
const Threaded = Io.Threaded;
const Cancelable = Io.Cancelable;
const assert = std.debug.assert;

pub const QueueClosedError = error{Closed};

pub const TypeErasedQueue = struct {
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

    /// Moves a completed waiter onto `wakes`. Reusing the node is fine: the
    /// waiter cannot return until its futex word is set.
    fn chainWake(list: *std.DoublyLinkedList, wakes: *std.DoublyLinkedList, node: *std.DoublyLinkedList.Node) void {
        list.remove(node);
        waiterOf(node).queued = false;
        wakes.append(node);
    }

    /// Delivers chained wakes; called after releasing the mutex.
    fn drainWakes(wakes: *std.DoublyLinkedList) void {
        while (wakes.popFirst()) |node| {
            const waiter = waiterOf(node);
            const io = waiter.io;
            const futex = &waiter.futex.raw;
            @atomicStore(u32, futex, 1, .release);
            io.futexWake(u32, futex, 1);
        }
    }

    /// Waits out an in-flight wake before the pending frame goes away.
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
        return q.putTimeout(io, elements, min, .none) catch |err| switch (err) {
            error.Timeout => unreachable,
            else => |e| return e,
        };
    }

    /// Same as `put`, except gives up when `timeout` expires. Returns
    /// `error.Timeout` only if nothing was transferred; partial progress is
    /// reported as a count.
    pub fn putTimeout(q: *TypeErasedQueue, io: Io, elements: []const u8, min: usize, timeout: Io.Timeout) (QueueClosedError || Cancelable || Io.Timeout.Error)!usize {
        assert(elements.len >= min);
        if (elements.len == 0) return 0;
        try io.checkCancel();
        const deadline = timeout.toDeadline(io);
        var wakes: std.DoublyLinkedList = .{};
        Threaded.mutexLock(&q.mutex);
        const result = q.putLocked(io, elements, min, false, deadline, &wakes);
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
        const result = q.putLocked(io, elements, min, true, .none, &wakes);
        Threaded.mutexUnlock(&q.mutex);
        drainWakes(&wakes);
        return result catch |err| switch (err) {
            error.Canceled => unreachable,
            error.Timeout => unreachable,
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

    fn putLocked(q: *TypeErasedQueue, io: Io, elements: []const u8, min: usize, comptime uncancelable: bool, deadline: Io.Timeout, wakes: *std.DoublyLinkedList) (QueueClosedError || Cancelable || Io.Timeout.Error)!usize {
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
            // Wakes chained while servicing must go out before we park.
            drainWakes(wakes);
            const result: Cancelable!void = if (uncancelable)
                io.futexWaitUncancelable(u32, &pending.waiter.futex.raw, 0)
            else
                io.futexWaitTimeout(u32, &pending.waiter.futex.raw, 0, deadline);
            Threaded.mutexLock(&q.mutex);
            result catch |err| switch (err) {
                error.Canceled => {
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
            // Completion takes priority over an expired deadline.
            if (!uncancelable) switch (deadline) {
                .none => {},
                .deadline => |d| if (pending.needed > 0 and !q.closed and d.untilNow(io).raw.nanoseconds >= 0) {
                    if (pending.remaining.len == elements.len) return error.Timeout;
                    return elements.len - pending.remaining.len;
                },
                .duration => unreachable,
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
        return q.getTimeout(io, buffer, min, .none) catch |err| switch (err) {
            error.Timeout => unreachable,
            else => |e| return e,
        };
    }

    /// Same as `get`, except gives up when `timeout` expires. Returns
    /// `error.Timeout` only if nothing was transferred; partial progress is
    /// reported as a count.
    pub fn getTimeout(q: *TypeErasedQueue, io: Io, buffer: []u8, min: usize, timeout: Io.Timeout) (QueueClosedError || Cancelable || Io.Timeout.Error)!usize {
        assert(buffer.len >= min);
        if (buffer.len == 0) return 0;
        try io.checkCancel();
        const deadline = timeout.toDeadline(io);
        var wakes: std.DoublyLinkedList = .{};
        Threaded.mutexLock(&q.mutex);
        const result = q.getLocked(io, buffer, min, false, deadline, &wakes);
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
        const result = q.getLocked(io, buffer, min, true, .none, &wakes);
        Threaded.mutexUnlock(&q.mutex);
        drainWakes(&wakes);
        return result catch |err| switch (err) {
            error.Canceled => unreachable,
            error.Timeout => unreachable,
            error.Closed => |e| return e,
        };
    }

    fn gettableSlice(q: *const TypeErasedQueue) ?[]const u8 {
        const overlong_slice = q.buffer[q.start..];
        const slice = overlong_slice[0..@min(overlong_slice.len, q.len)];
        return if (slice.len > 0) slice else null;
    }

    fn getLocked(q: *TypeErasedQueue, io: Io, buffer: []u8, min: usize, comptime uncancelable: bool, deadline: Io.Timeout, wakes: *std.DoublyLinkedList) (QueueClosedError || Cancelable || Io.Timeout.Error)!usize {
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
            // Wakes chained while servicing must go out before we park.
            drainWakes(wakes);
            const result: Cancelable!void = if (uncancelable)
                io.futexWaitUncancelable(u32, &pending.waiter.futex.raw, 0)
            else
                io.futexWaitTimeout(u32, &pending.waiter.futex.raw, 0, deadline);
            Threaded.mutexLock(&q.mutex);
            result catch |err| switch (err) {
                error.Canceled => {
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
            // Completion takes priority over an expired deadline.
            if (!uncancelable) switch (deadline) {
                .none => {},
                .deadline => |d| if (pending.needed > 0 and !q.closed and d.untilNow(io).raw.nanoseconds >= 0) {
                    if (pending.remaining.len == buffer.len) return error.Timeout;
                    return buffer.len - pending.remaining.len;
                },
                .duration => unreachable,
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

        /// Same as `put`, except gives up when `timeout` expires. Returns
        /// `error.Timeout` only if nothing was added; partial progress is
        /// reported as a count.
        pub fn putTimeout(q: *@This(), io: Io, elements: []const Elem, min: usize, timeout: Io.Timeout) (QueueClosedError || Cancelable || Io.Timeout.Error)!usize {
            return @divExact(try q.type_erased.putTimeout(io, @ptrCast(elements), min * @sizeOf(Elem), timeout), @sizeOf(Elem));
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

        /// Same as `putOne`, except gives up when `timeout` expires.
        pub fn putOneTimeout(q: *@This(), io: Io, item: Elem, timeout: Io.Timeout) (QueueClosedError || Cancelable || Io.Timeout.Error)!void {
            assert(try q.putTimeout(io, &.{item}, 1, timeout) == 1);
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

        /// Same as `get`, except gives up when `timeout` expires. Returns
        /// `error.Timeout` only if nothing was received; partial progress is
        /// reported as a count.
        pub fn getTimeout(q: *@This(), io: Io, buffer: []Elem, min: usize, timeout: Io.Timeout) (QueueClosedError || Cancelable || Io.Timeout.Error)!usize {
            return @divExact(try q.type_erased.getTimeout(io, @ptrCast(buffer), min * @sizeOf(Elem), timeout), @sizeOf(Elem));
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

        /// Same as `getOne`, except gives up when `timeout` expires.
        pub fn getOneTimeout(q: *@This(), io: Io, timeout: Io.Timeout) (QueueClosedError || Cancelable || Io.Timeout.Error)!Elem {
            var buf: [1]Elem = undefined;
            assert(try q.getTimeout(io, &buf, 1, timeout) == 1);
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

test "getTimeout on empty queue times out" {
    const io = std.testing.io;

    var buf: [4]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } };
    try std.testing.expectError(error.Timeout, q.getOneTimeout(io, timeout));

    // still usable afterwards
    try q.putOne(io, 9);
    try std.testing.expectEqual(9, try q.getOneTimeout(io, timeout));
}

test "putTimeout on full queue times out" {
    const io = std.testing.io;

    var buf: [1]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    try q.putOne(io, 1);
    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } };
    try std.testing.expectError(error.Timeout, q.putOneTimeout(io, 2, timeout));

    try std.testing.expectEqual(1, try q.getOne(io));
}

fn waitParked(q: *TypeErasedQueue, list: enum { getters, putters }) void {
    while (true) {
        Threaded.mutexLock(&q.mutex);
        const parked = switch (list) {
            .getters => q.getters.first != null,
            .putters => q.putters.first != null,
        };
        Threaded.mutexUnlock(&q.mutex);
        if (parked) return;
        std.atomic.spinLoopHint();
    }
}

test "cancel parked getter and putter" {
    const io = std.testing.io;

    var buf: [1]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    const T = struct {
        fn getter(w_io: Io, qq: *Queue(u64)) Cancelable!void {
            _ = qq.getOne(w_io) catch |err| switch (err) {
                error.Closed => return,
                error.Canceled => |e| return e,
            };
        }
        fn putter(w_io: Io, qq: *Queue(u64)) Cancelable!void {
            qq.putOne(w_io, 99) catch |err| switch (err) {
                error.Closed => return,
                error.Canceled => |e| return e,
            };
        }
    };

    var gfut = io.concurrent(T.getter, .{ io, &q }) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };
    waitParked(&q.type_erased, .getters);
    try std.testing.expectEqual(error.Canceled, gfut.cancel(io));

    // Queue unaffected: fill it, then cancel a parked putter.
    try q.putOne(io, 1);
    var pfut = io.concurrent(T.putter, .{ io, &q }) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };
    waitParked(&q.type_erased, .putters);
    try std.testing.expectEqual(error.Canceled, pfut.cancel(io));

    try std.testing.expectEqual(1, try q.getOne(io));
    var out: [1]u64 = undefined;
    try std.testing.expectEqual(0, try q.get(io, &out, 0));
}

test "cancel racing completion delivers exactly once" {
    const io = std.testing.io;

    var buf: [1]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    const T = struct {
        fn getter(w_io: Io, qq: *Queue(u64), out: *std.atomic.Value(u64)) Cancelable!void {
            const v = qq.getOne(w_io) catch |err| switch (err) {
                error.Closed => return,
                error.Canceled => |e| return e,
            };
            out.store(v, .seq_cst);
        }
        fn canceler(c_io: Io, f: *Io.Future(Cancelable!void)) Cancelable!void {
            f.cancel(c_io) catch {};
        }
    };

    for (0..100) |round| {
        const elem: u64 = round + 1;
        var got: std.atomic.Value(u64) = .init(0);

        var fut = io.concurrent(T.getter, .{ io, &q, &got }) catch |err| switch (err) {
            error.ConcurrencyUnavailable => return error.SkipZigTest,
        };
        waitParked(&q.type_erased, .getters);

        // Cancel from a second task so it genuinely races the completion.
        var canceler = io.concurrent(T.canceler, .{ io, &fut }) catch {
            _ = fut.cancel(io) catch {};
            return error.SkipZigTest;
        };
        try q.putOne(io, elem);
        canceler.await(io) catch {};

        // Exactly once: the getter either received the element, or it was
        // canceled first and the element stayed buffered.
        var out: [1]u64 = undefined;
        const leftover = try q.get(io, &out, 0);
        if (got.load(.seq_cst) != 0) {
            try std.testing.expectEqual(elem, got.load(.seq_cst));
            try std.testing.expectEqual(0, leftover);
        } else {
            try std.testing.expectEqual(1, leftover);
            try std.testing.expectEqual(elem, out[0]);
        }
    }
}

test "cancel after partial put returns count then Canceled" {
    const io = std.testing.io;

    var buf: [4]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    const T = struct {
        fn producer(w_io: Io, qq: *Queue(u64), n_out: *usize, second_canceled: *bool) Cancelable!void {
            const vals: [8]u64 = .{ 1, 2, 3, 4, 5, 6, 7, 8 };
            const n = qq.put(w_io, &vals, 8) catch |err| switch (err) {
                error.Closed => unreachable,
                error.Canceled => |e| return e,
            };
            n_out.* = n;
            // recancel: the next cancelable call must observe the cancellation
            _ = qq.put(w_io, &vals, 8) catch |err| switch (err) {
                error.Closed => unreachable,
                error.Canceled => {
                    second_canceled.* = true;
                    return error.Canceled;
                },
            };
        }
    };

    var n_out: usize = 0;
    var second_canceled = false;
    var fut = io.concurrent(T.producer, .{ io, &q, &n_out, &second_canceled }) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };
    waitParked(&q.type_erased, .putters);

    // Take two elements; servicing refills the ring from the parked putter,
    // leaving it with partial progress (6 of 8 transferred, still parked).
    var out2: [2]u64 = undefined;
    try std.testing.expectEqual(2, try q.get(io, &out2, 2));
    try std.testing.expectEqual(1, out2[0]);
    try std.testing.expectEqual(2, out2[1]);

    try std.testing.expectEqual(error.Canceled, fut.cancel(io));
    try std.testing.expectEqual(6, n_out);
    try std.testing.expect(second_canceled);

    for (0..4) |i| try std.testing.expectEqual(i + 3, try q.getOne(io));
    var out: [1]u64 = undefined;
    try std.testing.expectEqual(0, try q.get(io, &out, 0));
}

test "two ios: producer/consumer" {
    const gpa = std.testing.allocator;

    var t1: Io.Threaded = .init(gpa, .{});
    defer t1.deinit();
    var t2: Io.Threaded = .init(gpa, .{});
    defer t2.deinit();

    var buf: [4]u64 = undefined;
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

    var pg: Io.Group = .init;
    defer pg.cancel(t1.io());
    var cg: Io.Group = .init;
    defer cg.cancel(t2.io());
    pg.concurrent(t1.io(), T.producer, .{ t1.io(), &q }) catch return error.SkipZigTest;
    cg.concurrent(t2.io(), T.consumer, .{ t2.io(), &q, &sum }) catch return error.SkipZigTest;
    try pg.await(t1.io());
    try cg.await(t2.io());

    try std.testing.expectEqual(5050, sum);
}

test "two ios: MPMC stress conserves elements" {
    const gpa = std.testing.allocator;

    var t1: Io.Threaded = .init(gpa, .{});
    defer t1.deinit();
    var t2: Io.Threaded = .init(gpa, .{});
    defer t2.deinit();
    const ios: [2]Io = .{ t1.io(), t2.io() };

    const num_producers = 4;
    const num_consumers = 4;
    const per_producer = 1000;

    var buf: [8]u64 = undefined;
    var q: Queue(u64) = .init(&buf);
    // seen[v] counts deliveries of value v; every element must arrive exactly once.
    var seen: [num_producers * per_producer]std.atomic.Value(u8) = @splat(.init(0));

    const T = struct {
        fn producer(w_io: Io, qq: *Queue(u64), id: usize) Cancelable!void {
            var prng = std.Random.DefaultPrng.init(std.testing.random_seed +% id);
            const rnd = prng.random();
            var vals: [per_producer]u64 = undefined;
            for (&vals, 0..) |*v, i| v.* = id * per_producer + i;

            var pos: usize = 0;
            while (pos < per_producer) {
                const len = @min(per_producer - pos, rnd.intRangeAtMost(usize, 1, 7));
                const min = rnd.intRangeAtMost(usize, 1, len);
                const n = qq.put(w_io, vals[pos..][0..len], min) catch |err| switch (err) {
                    error.Closed => unreachable, // closed only after all producers finish
                    error.Canceled => |e| return e,
                };
                pos += n;
            }
        }
        fn consumer(w_io: Io, qq: *Queue(u64), marks: []std.atomic.Value(u8), dup: *std.atomic.Value(u32)) Cancelable!void {
            var prng = std.Random.DefaultPrng.init(std.testing.random_seed);
            const rnd = prng.random();
            var out: [8]u64 = undefined;
            while (true) {
                const min = rnd.intRangeAtMost(usize, 1, out.len);
                const n = qq.get(w_io, &out, min) catch |err| switch (err) {
                    error.Closed => return,
                    error.Canceled => |e| return e,
                };
                for (out[0..n]) |v| {
                    if (marks[@intCast(v)].fetchAdd(1, .monotonic) != 0) {
                        _ = dup.fetchAdd(1, .monotonic);
                    }
                }
            }
        }
    };

    var dup: std.atomic.Value(u32) = .init(0);
    var producers: Io.Group = .init;
    defer producers.cancel(ios[0]);
    var consumers: Io.Group = .init;
    defer consumers.cancel(ios[1]);

    for (0..num_producers) |id| {
        producers.concurrent(ios[0], T.producer, .{ ios[0], &q, id }) catch return error.SkipZigTest;
    }
    for (0..num_consumers) |_| {
        consumers.concurrent(ios[1], T.consumer, .{ ios[1], &q, &seen, &dup }) catch return error.SkipZigTest;
    }

    try producers.await(ios[0]);
    q.close(ios[0]);
    try consumers.await(ios[1]);

    try std.testing.expectEqual(0, dup.load(.monotonic));
    for (&seen, 0..) |*s, v| {
        if (s.load(.monotonic) != 1) {
            std.debug.print("element {d} delivered {d} times (seed {d})\n", .{ v, s.load(.monotonic), std.testing.random_seed });
            return error.TestUnexpectedResult;
        }
    }
}

test "timeout with partial progress returns count" {
    const io = std.testing.io;

    var buf: [4]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } };

    // putTimeout: the buffer takes 4 of 8, then the deadline passes.
    const vals: [8]u64 = .{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try std.testing.expectEqual(4, try q.putTimeout(io, &vals, 8, timeout));

    // getTimeout: receives the 4 buffered, min 6 stays out of reach.
    var out: [8]u64 = undefined;
    try std.testing.expectEqual(4, try q.getTimeout(io, &out, 6, timeout));
    for (out[0..4], 1..) |v, i| try std.testing.expectEqual(i, v);
}

test "pending putter is woken before its servicer parks" {
    const io = std.testing.io;

    var buf: [1]u64 = undefined;
    var q: Queue(u64) = .init(&buf);

    const T = struct {
        fn producer(w_io: Io, qq: *Queue(u64)) Cancelable!void {
            qq.putAll(w_io, &.{ 1, 2 }) catch |err| switch (err) {
                error.Closed => return,
                error.Canceled => |e| return e,
            };
            qq.close(w_io);
        }
        fn consumer(w_io: Io, qq: *Queue(u64), got: *usize) Cancelable!void {
            var out: [4]u64 = undefined;
            got.* = qq.get(w_io, &out, 4) catch |err| switch (err) {
                error.Closed => 0,
                error.Canceled => |e| return e,
            };
        }
    };

    var got: usize = 0;
    var group: Io.Group = .init;
    defer group.cancel(io);
    group.concurrent(io, T.producer, .{ io, &q }) catch return error.SkipZigTest;

    // Wait until the producer is parked with a pending element, so the
    // consumer both completes it and then pends itself.
    while (true) {
        Threaded.mutexLock(&q.type_erased.mutex);
        const parked = q.type_erased.putters.first != null;
        Threaded.mutexUnlock(&q.type_erased.mutex);
        if (parked) break;
        std.atomic.spinLoopHint();
    }

    group.concurrent(io, T.consumer, .{ io, &q, &got }) catch return error.SkipZigTest;
    try group.await(io);

    try std.testing.expectEqual(2, got);
}

test {
    std.testing.refAllDecls(Queue(u64));
}
