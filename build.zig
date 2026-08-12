// SPDX-FileCopyrightText: Zig contributors
// SPDX-License-Identifier: MIT

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("xsync", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(mod_tests);
    run_tests.has_side_effects = true;

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);

    // Standalone reproducer for the ziglang/zig aarch64-macos RwLock hang. Not
    // part of the library or of `zig build test`; see repro/main.zig.
    const fix_36178 = b.option(bool, "fix-36178", "Apply the #36178 cancel-path fix in the RwLock copy") orelse true;

    const rounds = b.option(u64, "repro-rounds", "Reproducer rounds") orelse 20;
    const iterations = b.option(u64, "repro-iterations", "Reproducer iterations per round") orelse 10_000;

    const repro_options = b.addOptions();
    repro_options.addOption(bool, "fix_36178", fix_36178);
    repro_options.addOption(u64, "rounds", rounds);
    repro_options.addOption(u64, "iterations", iterations);

    const repro_mod = b.createModule(.{
        .root_source_file = b.path("repro/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    repro_mod.addOptions("build_options", repro_options);

    const repro_exe = b.addExecutable(.{ .name = "repro", .root_module = repro_mod });
    b.installArtifact(repro_exe);

    const run_repro = b.addRunArtifact(repro_exe);
    run_repro.has_side_effects = true;

    const repro_step = b.step("repro", "Run the std.Io.RwLock cancelation hang reproducer");
    repro_step.dependOn(&run_repro.step);

    // zig's own two tests, verbatim, under the std test runner on
    // `std.testing.io` — the harness its failing job used. One binary per test
    // so each can be timed on its own; zig's `--test-timeout 2m` is per test.
    const repro_test_step = b.step("repro-test", "Run zig's RwLock tests under the std test runner");

    for ([_]struct { name: []const u8, filter: []const u8 }{
        .{ .name = "test-leak", .filter = "canceled writer" },
        .{ .name = "test-trylock", .filter = "tryLock" },
    }) |t| {
        const test_mod = b.createModule(.{
            .root_source_file = b.path("repro/test.zig"),
            .target = target,
            .optimize = optimize,
        });
        test_mod.addOptions("build_options", repro_options);

        const tests = b.addTest(.{
            .name = t.name,
            .root_module = test_mod,
            .filters = &.{t.filter},
        });
        b.installArtifact(tests);

        const run = b.addRunArtifact(tests);
        run.has_side_effects = true;
        repro_test_step.dependOn(&run.step);
    }
}
