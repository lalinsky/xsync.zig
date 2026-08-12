// Verbatim copy of `std.Io.Condition` from ziglang/zig branch `rwlock-fixes`
// (commit b4bab0f6aa + d4b422e56c, i.e. PR #35564 applied), so that this repro
// exercises the std implementation rather than xsync's parker-based one.
//
// Do not "fix" anything here. It must track upstream exactly.

const std = @import("std");
const Io = std.Io;
const math = std.math;
const assert = std.debug.assert;

const Mutex = Io.Mutex;
const Cancelable = Io.Cancelable;
const Timeout = Io.Timeout;

pub const Condition = struct {
    state: std.atomic.Value(State),
    /// Incremented whenever the condition is signaled
    epoch: std.atomic.Value(u32),

    const State = packed struct(u32) {
        waiters: u16,
        signals: u16,
    };

    pub const init: Condition = .{
        .state = .init(.{ .waiters = 0, .signals = 0 }),
        .epoch = .init(0),
    };

    /// Blocks until the condition is signaled or canceled.
    ///
    /// See also:
    /// * `waitUncancelable`
    /// * `waitTimeout`
    pub fn wait(cond: *Condition, io: Io, mutex: *Mutex) Cancelable!void {
        waitTimeout(cond, io, mutex, .none) catch |err| switch (err) {
            error.Timeout => unreachable,
            error.Canceled => |e| return e,
        };
    }

    pub const WaitTimeoutError = Cancelable || Timeout.Error;

    /// Blocks until the condition is signaled, canceled, or the provided
    /// timeout expires.
    ///
    /// See also:
    /// * `wait`
    /// * `waitUncancelable`
    pub fn waitTimeout(cond: *Condition, io: Io, mutex: *Mutex, timeout: Timeout) WaitTimeoutError!void {
        const deadline = timeout.toDeadline(io);

        var epoch = cond.epoch.load(.acquire); // `.acquire` to ensure ordered before state load

        {
            const prev_state = cond.state.fetchAdd(.{ .waiters = 1, .signals = 0 }, .monotonic);
            assert(prev_state.waiters < math.maxInt(u16)); // overflow caused by too many waiters
        }

        mutex.unlock(io);
        defer mutex.lockUncancelable(io);

        while (true) {
            const result = io.futexWaitTimeout(u32, &cond.epoch.raw, epoch, deadline);

            epoch = cond.epoch.load(.acquire); // `.acquire` to ensure ordered before `state` laod

            // We were woken normally, so try to consume a pending signal. A signal takes
            // priority over an expired deadline, so this is checked before the deadline
            // below. On error we safely remove ourselves as a waiter and propagate the error.
            if (result) |_| {
                var prev_state = cond.state.load(.monotonic);
                while (prev_state.signals > 0) {
                    prev_state = cond.state.cmpxchgWeak(prev_state, .{
                        .waiters = prev_state.waiters - 1,
                        .signals = prev_state.signals - 1,
                    }, .acquire, .monotonic) orelse {
                        // We successfully consumed a signal.
                        return;
                    };
                }
            } else |err| {
                cond.deregister(io);
                return err;
            }

            // There are no signals available and no error; if a timeout was specified and
            // the deadline has passed, remove ourselves as a waiter and return
            // `error.Timeout`. Otherwise, this was a spurious wakeup: loop back to the
            // futex wait.
            switch (deadline) {
                .none => {},
                .deadline => |d| if (d.untilNow(io).raw.nanoseconds >= 0) {
                    cond.deregister(io);
                    return error.Timeout;
                },
                .duration => unreachable,
            }
        }
    }

    /// Same as `wait`, except does not introduce a cancelation point.
    ///
    /// See `Future.cancel` for a description of cancelation points.
    pub fn waitUncancelable(cond: *Condition, io: Io, mutex: *Mutex) void {
        var epoch = cond.epoch.load(.acquire); // `.acquire` to ensure ordered before state load

        {
            const prev_state = cond.state.fetchAdd(.{ .waiters = 1, .signals = 0 }, .monotonic);
            assert(prev_state.waiters < math.maxInt(u16)); // overflow caused by too many waiters
        }

        mutex.unlock(io);
        defer mutex.lockUncancelable(io);

        while (true) {
            io.futexWaitUncancelable(u32, &cond.epoch.raw, epoch);

            epoch = cond.epoch.load(.acquire); // `.acquire` to ensure ordered before `state` laod

            // Even on error, try to consume a pending signal first. Otherwise a race might
            // cause a signal to get stuck in the state with no corresponding waiter.
            {
                var prev_state = cond.state.load(.monotonic);
                while (prev_state.signals > 0) {
                    prev_state = cond.state.cmpxchgWeak(prev_state, .{
                        .waiters = prev_state.waiters - 1,
                        .signals = prev_state.signals - 1,
                    }, .acquire, .monotonic) orelse {
                        // We successfully consumed a signal.
                        return;
                    };
                }
            }

            // There are no more signals available; this was a spurious wakeup,
            // so we'll loop back to the futex wait.
        }
    }

    fn deregister(cond: *Condition, io: Io) void {
        var prev_state = cond.state.load(.monotonic);
        while (true) {
            const new_signals = @min(prev_state.signals, prev_state.waiters - 1);
            prev_state = cond.state.cmpxchgWeak(prev_state, .{
                .waiters = prev_state.waiters - 1,
                .signals = new_signals,
            }, .monotonic, .monotonic) orelse {
                if (prev_state.signals > 0 and prev_state.signals < prev_state.waiters) {
                    // We kept a signal we are not consuming; wake a remaining waiter for it.
                    _ = cond.epoch.fetchAdd(1, .release);
                    io.futexWake(u32, &cond.epoch.raw, 1);
                }
                return;
            };
        }
    }

    pub fn signal(cond: *Condition, io: Io) void {
        var prev_state = cond.state.load(.monotonic);
        while (prev_state.waiters > prev_state.signals) {
            @branchHint(.unlikely);
            prev_state = cond.state.cmpxchgWeak(prev_state, .{
                .waiters = prev_state.waiters,
                .signals = prev_state.signals + 1,
            }, .release, .monotonic) orelse {
                // Update the epoch to tell the waiting threads that there are new signals for them.
                // Note that a waiting thread could miss a take if *exactly* (1<<32)-1 wakes happen
                // between it observing the epoch and sleeping on it, but this is extraordinarily
                // unlikely due to the precise number of calls required.
                _ = cond.epoch.fetchAdd(1, .release); // `.release` to ensure ordered after `state` update
                io.futexWake(u32, &cond.epoch.raw, 1);
                return;
            };
        }
    }

    pub fn broadcast(cond: *Condition, io: Io) void {
        var prev_state = cond.state.load(.monotonic);
        while (prev_state.waiters > prev_state.signals) {
            @branchHint(.unlikely);
            prev_state = cond.state.cmpxchgWeak(prev_state, .{
                .waiters = prev_state.waiters,
                .signals = prev_state.waiters,
            }, .release, .monotonic) orelse {
                // Update the epoch to tell the waiting threads that there are new signals for them.
                // Note that a waiting thread could miss a take if *exactly* (1<<32)-1 wakes happen
                // between it observing the epoch and sleeping on it, but this is extraordinarily
                // unlikely due to the precise number of calls required.
                _ = cond.epoch.fetchAdd(1, .release); // `.release` to ensure ordered after `state` update
                io.futexWake(u32, &cond.epoch.raw, prev_state.waiters - prev_state.signals);
                return;
            };
        }
    }
};
