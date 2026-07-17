# xsync.zig

Synchronization primitives for Zig that work across `std.Io` instances.

[![CI](https://github.com/lalinsky/xsync.zig/actions/workflows/ci.yml/badge.svg)](https://github.com/lalinsky/xsync.zig/actions/workflows/ci.yml)

`std.Io.Mutex` and friends wake waiters through whichever `Io` the *waker*
happens to run on. That works when everything shares one parking
implementation, but a waiter parked through one `Io` is invisible to another,
so primitives can't be shared between tasks living on different runtimes. The
primitives here remember which `Io` each waiter parked through and wake it
through that one.

## What's in the box

- `Mutex`, `Condition`, `Event`: same API as their `std.Io` counterparts
- `Semaphore`, `RwLock`: lifted from the standard library, running on the above
- `Queue`: the `std.Io.Queue` API plus `putTimeout`/`getTimeout` variants,
  rewritten so each pending operation parks on a plain futex word instead of
  its own mutex/condition pair, which makes it noticeably faster under
  contention

## Example

A worker pool runs on `std.Io.Threaded`, the rest of the program lives on a
[zio](https://github.com/lalinsky/zio) runtime, with two queues shared across the
two `Io`s. (The example uses zio as the evented side while `std.Io.Evented`
is still in development; any `Io` implementation works.)

```zig
const std = @import("std");
const zio = @import("zio");
const xsync = @import("xsync");

fn worker(io: std.Io, requests: *xsync.Queue(u64), results: *xsync.Queue(u64)) std.Io.Cancelable!void {
    while (true) {
        const n = requests.getOne(io) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => |e| return e,
        };
        results.putOne(io, n * n) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => |e| return e,
        };
    }
}

pub fn main() !void {
    const gpa = std.heap.smp_allocator;

    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const threaded_io = threaded.io();

    const rt = try zio.Runtime.init(gpa, .{});
    defer rt.deinit();
    const evented_io = rt.io();

    var request_buf: [8]u64 = undefined;
    var requests: xsync.Queue(u64) = .init(&request_buf);
    var result_buf: [100]u64 = undefined;
    var results: xsync.Queue(u64) = .init(&result_buf);

    // the worker pool runs on OS threads
    var workers: std.Io.Group = .init;
    defer workers.cancel(threaded_io);
    for (0..4) |_| {
        try workers.concurrent(threaded_io, worker, .{ threaded_io, &requests, &results });
    }

    // push work through one io, workers hand results back through the other
    for (0..100) |n| try requests.putOne(evented_io, n);
    requests.close(evented_io);

    var sum: u64 = 0;
    for (0..100) |_| sum += try results.getOne(evented_io);
    std.debug.print("sum: {d}\n", .{sum});
}
```

## Install

```
zig fetch --save git+https://github.com/lalinsky/xsync.zig
```

```zig
const xsync = b.dependency("xsync", .{}).module("xsync");
exe.root_module.addImport("xsync", xsync);
```

## Notes

- Unlike the extern `std.Io.Mutex`, these can't live in shared memory or work
  across processes.
- No FIFO ordering guarantees, same as `std.Io`.
- Any thread-safe `Io` implementation works; the only requirement is that its
  `futexWake` may be called from another thread.

Tested on Zig 0.16 and master.

## Zig bug reports

- [36139](https://codeberg.org/ziglang/zig/issues/36139) - Cancellation safety in std.Io.Conditon
- [36178](https://codeberg.org/ziglang/zig/issues/36178) - Io.RwLock: canceled writer can leave a stale semaphore permit 
- [36217](https://codeberg.org/ziglang/zig/issues/36217) - std.Io.RwLock.tryLock can succeed while a reader holds the lock

## License

MIT, same as Zig. Semaphore, RwLock and the queue's transfer logic come from
the Zig standard library.
