// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

//! Synchronization primitives on the `std.Io` futex interface that work across
//! `Io` instances: each waiter records the `Io` it parked on, so the waker
//! reaches it through the right one. Unlike the extern `std.Io.Mutex`, the queue
//! holds pointers to stack waiters, so these can't live in shared memory.

pub const Mutex = @import("Mutex.zig");
pub const Condition = @import("Condition.zig");
pub const Event = @import("Event.zig");
pub const Semaphore = @import("Semaphore.zig");
pub const RwLock = @import("RwLock.zig");
pub const Queue = @import("queue.zig").Queue;
pub const QueueClosedError = @import("queue.zig").QueueClosedError;

test {
    _ = Mutex;
    _ = Condition;
    _ = Event;
    _ = Semaphore;
    _ = RwLock;
    _ = Queue;
}
