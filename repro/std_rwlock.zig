// Copy of `std.Io.RwLock` from ziglang/zig branch `rwlock-fixes`, i.e. with the
// candidate fixes for ziglang#36217 (`tryLock` CAS) and ziglang#36178 (canceled
// writer reclaims the stale semaphore permit). Rewired onto the local
// `Semaphore` copy. Tests live in `main.zig`.
//
// `fix_36178` toggles only the #36178 cancel-path fix, so one binary can be
// built both ways and the hang attributed.

const RwLock = @This();

const std = @import("std");
const Io = std.Io;

const Semaphore = @import("std_semaphore.zig");

pub const fix_36178 = @import("build_options").fix_36178;

/// Set while a canceled writer is parked in the uncancelable permit reclaim.
/// Purely diagnostic: the watchdog reads it to tell "stuck in the new wait"
/// apart from "stuck somewhere else".
pub var waiting_uncancelable: std.atomic.Value(bool) = .init(false);

state: usize,
mutex: Io.Mutex,
semaphore: Semaphore,

pub const init: RwLock = .{
    .state = 0,
    .mutex = .init,
    .semaphore = .{},
};

pub const is_writing: usize = 1;
pub const writer: usize = 1 << 1;
pub const reader: usize = 1 << (1 + @bitSizeOf(Count));
pub const writer_mask: usize = std.math.maxInt(Count) << @ctz(writer);
pub const reader_mask: usize = std.math.maxInt(Count) << @ctz(reader);
const Count = @Int(.unsigned, @divFloor(@bitSizeOf(usize) - 1, 2));

pub fn tryLock(rl: *RwLock, io: Io) bool {
    if (rl.mutex.tryLock()) {
        // Unlike `lock`, this never registers in `writer_mask`, so holding the mutex does
        // not stop a reader from taking the fast path; the CAS catches one that raced in
        // after `state` was loaded.
        const state = @atomicLoad(usize, &rl.state, .seq_cst);
        if (state & reader_mask == 0) {
            _ = @cmpxchgStrong(
                usize,
                &rl.state,
                state,
                state | is_writing,
                .seq_cst,
                .seq_cst,
            ) orelse return true;
        }

        rl.mutex.unlock(io);
    }

    return false;
}

pub fn lockUncancelable(rl: *RwLock, io: Io) void {
    _ = @atomicRmw(usize, &rl.state, .Add, writer, .seq_cst);
    rl.mutex.lockUncancelable(io);

    const state = @atomicRmw(usize, &rl.state, .Add, is_writing -% writer, .seq_cst);
    if (state & reader_mask != 0)
        rl.semaphore.waitUncancelable(io);
}

pub fn lock(rl: *RwLock, io: Io) Io.Cancelable!void {
    _ = @atomicRmw(usize, &rl.state, .Add, writer, .seq_cst);
    rl.mutex.lock(io) catch |err| switch (err) {
        error.Canceled => {
            _ = @atomicRmw(usize, &rl.state, .Sub, writer, .seq_cst);
            return error.Canceled;
        },
    };

    const state = @atomicRmw(usize, &rl.state, .Add, is_writing -% writer, .seq_cst);
    if (state & reader_mask != 0)
        rl.semaphore.wait(io) catch |err| switch (err) {
            error.Canceled => {
                if (!fix_36178) {
                    rl.unlock(io);
                    return error.Canceled;
                }
                // Clearing `is_writing` while still holding the mutex means the last reader
                // either saw it set, and posts a permit only we can consume, or did not and
                // never posts. A stale permit would let the next writer in past the readers.
                const prev_state = @atomicRmw(usize, &rl.state, .And, ~is_writing, .seq_cst);
                if (prev_state & reader_mask == 0) {
                    waiting_uncancelable.store(true, .monotonic);
                    rl.semaphore.waitUncancelable(io);
                    waiting_uncancelable.store(false, .monotonic);
                }
                rl.mutex.unlock(io);
                return error.Canceled;
            },
        };
}

pub fn unlock(rl: *RwLock, io: Io) void {
    _ = @atomicRmw(usize, &rl.state, .And, ~is_writing, .seq_cst);
    rl.mutex.unlock(io);
}

pub fn tryLockShared(rl: *RwLock, io: Io) bool {
    const state = @atomicLoad(usize, &rl.state, .seq_cst);
    if (state & (is_writing | writer_mask) == 0) {
        _ = @cmpxchgStrong(
            usize,
            &rl.state,
            state,
            state + reader,
            .seq_cst,
            .seq_cst,
        ) orelse return true;
    }

    if (rl.mutex.tryLock()) {
        _ = @atomicRmw(usize, &rl.state, .Add, reader, .seq_cst);
        rl.mutex.unlock(io);
        return true;
    }

    return false;
}

pub fn lockSharedUncancelable(rl: *RwLock, io: Io) void {
    var state = @atomicLoad(usize, &rl.state, .seq_cst);
    while (state & (is_writing | writer_mask) == 0) {
        state = @cmpxchgWeak(
            usize,
            &rl.state,
            state,
            state + reader,
            .seq_cst,
            .seq_cst,
        ) orelse return;
    }

    rl.mutex.lockUncancelable(io);
    _ = @atomicRmw(usize, &rl.state, .Add, reader, .seq_cst);
    rl.mutex.unlock(io);
}

pub fn lockShared(rl: *RwLock, io: Io) Io.Cancelable!void {
    var state = @atomicLoad(usize, &rl.state, .seq_cst);
    while (state & (is_writing | writer_mask) == 0) {
        state = @cmpxchgWeak(
            usize,
            &rl.state,
            state,
            state + reader,
            .seq_cst,
            .seq_cst,
        ) orelse return;
    }

    try rl.mutex.lock(io);
    _ = @atomicRmw(usize, &rl.state, .Add, reader, .seq_cst);
    rl.mutex.unlock(io);
}

pub fn unlockShared(rl: *RwLock, io: Io) void {
    const state = @atomicRmw(usize, &rl.state, .Sub, reader, .seq_cst);

    if ((state & reader_mask == reader) and (state & is_writing != 0))
        rl.semaphore.post(io);
}
