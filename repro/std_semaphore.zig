// Verbatim copy of `std.Io.Semaphore` from ziglang/zig branch `rwlock-fixes`,
// rewired onto the local `Condition` copy. Unmodified otherwise.

const Semaphore = @This();

const std = @import("std");
const Io = std.Io;

const Condition = @import("std_condition.zig").Condition;

mutex: Io.Mutex = .init,
cond: Condition = .init,
/// It is OK to initialize this field to any value.
permits: usize = 0,

/// Blocks until a `permit` is available and consumes a single one.
/// Unblocks without consuming a `permit` when canceled.
///
/// See also:
/// * `waitTimeout`
/// * `waitUncancelable`
pub fn wait(s: *Semaphore, io: Io) Io.Cancelable!void {
    s.waitTimeout(io, .none) catch |err| switch (err) {
        error.Timeout => unreachable,
        error.Canceled => |e| return e,
    };
}

pub const WaitTimeoutError = Io.Cancelable || Io.Timeout.Error;

/// Blocks until a `permit` is available and consumes a single one.
/// Unblocks without consuming a `permit` when canceled or when the provided
/// timeout expires before a `permit` is available.
///
/// See also:
/// * `wait`
/// * `waitUncancelable`
pub fn waitTimeout(s: *Semaphore, io: Io, timeout: Io.Timeout) WaitTimeoutError!void {
    const deadline = timeout.toDeadline(io);
    try s.mutex.lock(io);
    defer s.mutex.unlock(io);
    while (s.permits == 0) try s.cond.waitTimeout(io, &s.mutex, deadline);
    s.permits -= 1;
    if (s.permits > 0) s.cond.signal(io);
}

/// Blocks until a `permit` is available and consumes a single one.
///
/// See also:
/// * `wait`
/// * `waitTimeout`
pub fn waitUncancelable(s: *Semaphore, io: Io) void {
    s.mutex.lockUncancelable(io);
    defer s.mutex.unlock(io);
    while (s.permits == 0) s.cond.waitUncancelable(io, &s.mutex);
    s.permits -= 1;
    if (s.permits > 0) s.cond.signal(io);
}

/// Makes an additional `permit` available.
pub fn post(s: *Semaphore, io: Io) void {
    s.mutex.lockUncancelable(io);
    defer s.mutex.unlock(io);

    s.permits += 1;
    s.cond.signal(io);
}
