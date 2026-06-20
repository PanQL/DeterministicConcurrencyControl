#!/usr/bin/env python3
"""Run CalvinFS madsim metadata benchmarks and collect CSV results."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "results" / "ipso_metadata" / "ipso-metadata-madsim.csv"
DEFAULT_LATENCY_OUTPUT = (
    REPO_ROOT / "results" / "ipso_metadata" / "ipso-metadata-latency-madsim.csv"
)


def parse_csv_ints(value: str) -> list[int]:
    items: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            raise argparse.ArgumentTypeError("empty list entry")
        try:
            items.append(int(part))
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid integer {part!r}") from exc
    return items


def git_rev() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            text=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return "unknown"
    return result.stdout.strip() or "unknown"


def madsim_env(args: argparse.Namespace, output: Path, trial: int) -> dict[str, str]:
    env = os.environ.copy()
    rustflags = env.get("RUSTFLAGS", "")
    if "--cfg madsim" not in rustflags:
        rustflags = f"{rustflags} --cfg madsim".strip()
    env["RUSTFLAGS"] = rustflags
    env["CALVINFS_BENCH_OUTPUT"] = str(output)
    env["CALVINFS_BENCH_SOURCE"] = "madsim"
    env["CALVINFS_BENCH_TRIAL"] = str(trial)
    env["CALVINFS_BENCH_GIT_REV"] = args.git_rev
    if args.profile:
        env["CALVINFS_SCHED_PROFILE"] = "1"
    else:
        env.pop("CALVINFS_SCHED_PROFILE", None)
    return env


def run_cargo_test(
    name: str,
    test_filter: str,
    env: dict[str, str],
    log_dir: Path,
) -> None:
    cmd = ["cargo", "test", test_filter, "--", "--nocapture"]
    print(f"+ {' '.join(cmd)}  # {name}")
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{name}.log"
    log_path.write_text(result.stdout, encoding="utf-8")
    if result.returncode != 0:
        print(result.stdout)
        raise SystemExit(f"{name} failed; see {log_path}")


def run_mdtest(args: argparse.Namespace, output: Path, log_dir: Path) -> None:
    for trial in range(args.trials):
        for clients in args.mdtest_clients:
            env = madsim_env(args, output, trial)
            env.update(
                {
                    "CALVINFS_MDTEST_CLIENTS": str(clients),
                    "CALVINFS_MDTEST_DIRS_PER_CLIENT": str(args.mdtest_dirs_per_client),
                    "CALVINFS_MDTEST_FILES_PER_CLIENT": str(args.mdtest_files_per_client),
                    "CALVINFS_MDTEST_BATCH_SIZE": str(args.batch_size),
                    "CALVINFS_MDTEST_RESULT_INFLIGHT": str(args.mdtest_result_inflight),
                }
            )
            if args.mdtest_result_channel_capacity is not None:
                env["CALVINFS_MDTEST_RESULT_CHANNEL_CAPACITY"] = str(
                    args.mdtest_result_channel_capacity
                )
            run_cargo_test(
                f"mdtest_t{trial}_n{clients}",
                "mdtest_like_client_workload",
                env,
                log_dir,
            )


def run_mdtest_latency(args: argparse.Namespace, latency_output: Path, log_dir: Path) -> None:
    for trial in range(args.trials):
        env = madsim_env(args, args.output.resolve(), trial)
        env.pop("CALVINFS_BENCH_OUTPUT", None)
        env.update(
            {
                "CALVINFS_BENCH_LATENCY_OUTPUT": str(latency_output),
                "CALVINFS_MDTEST_CLIENTS": str(args.mdtest_latency_clients),
                "CALVINFS_MDTEST_DIRS_PER_CLIENT": str(args.mdtest_dirs_per_client),
                "CALVINFS_MDTEST_FILES_PER_CLIENT": str(args.mdtest_files_per_client),
                "CALVINFS_MDTEST_BATCH_SIZE": str(args.batch_size),
                "CALVINFS_MDTEST_RESULT_INFLIGHT": str(args.mdtest_result_inflight),
            }
        )
        if args.mdtest_result_channel_capacity is not None:
            env["CALVINFS_MDTEST_RESULT_CHANNEL_CAPACITY"] = str(
                args.mdtest_result_channel_capacity
            )
        run_cargo_test(
            f"mdtest_latency_t{trial}_n{args.mdtest_latency_clients}",
            "mdtest_like_client_workload",
            env,
            log_dir,
        )


def run_mdworkbench(args: argparse.Namespace, output: Path, log_dir: Path) -> None:
    for trial in range(args.trials):
        env = madsim_env(args, output, trial)
        env.update(
            {
                "CALVINFS_MDWB_CLIENTS": str(args.mdwb_clients),
                "CALVINFS_MDWB_DATA_SETS": str(args.mdwb_data_sets),
                "CALVINFS_MDWB_PRECREATE_PER_SET": str(args.mdwb_precreate_per_set),
                "CALVINFS_MDWB_OPS_PER_SET": str(args.mdwb_ops_per_set),
                "CALVINFS_MDWB_ITERATIONS": str(args.mdwb_iterations),
                "CALVINFS_MDWB_BATCH_SIZE": str(args.batch_size),
                "CALVINFS_MDWB_OFFSET": str(args.mdwb_offset),
                "CALVINFS_MDWB_PARENT_BUCKETS": ",".join(
                    str(value) for value in args.mdwb_parent_buckets
                ),
            }
        )
        run_cargo_test(
            f"mdworkbench_t{trial}",
            "md_workbench_like_client_workload",
            env,
            log_dir,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--latency-output", type=Path, default=DEFAULT_LATENCY_OUTPUT)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--append", action="store_true")
    parser.add_argument("--no-profile", dest="profile", action="store_false")
    parser.set_defaults(profile=True)
    parser.add_argument("--skip-mdtest", action="store_true")
    parser.add_argument("--skip-mdtest-latency", action="store_true")
    parser.add_argument("--skip-mdworkbench", action="store_true")
    parser.add_argument("--batch-size", type=int, default=512)
    parser.add_argument(
        "--mdtest-clients", type=parse_csv_ints, default=[1, 2, 4, 8, 16, 32, 64]
    )
    parser.add_argument("--mdtest-latency-clients", type=int, default=32)
    parser.add_argument("--mdtest-dirs-per-client", type=int, default=4)
    parser.add_argument("--mdtest-files-per-client", type=int, default=64)
    parser.add_argument("--mdtest-result-inflight", type=int, default=64)
    parser.add_argument("--mdtest-result-channel-capacity", type=int)
    parser.add_argument("--mdwb-clients", type=int, default=8)
    parser.add_argument("--mdwb-data-sets", type=int, default=4)
    parser.add_argument("--mdwb-precreate-per-set", type=int, default=32)
    parser.add_argument("--mdwb-ops-per-set", type=int, default=16)
    parser.add_argument("--mdwb-iterations", type=int, default=2)
    parser.add_argument("--mdwb-offset", type=int, default=1)
    parser.add_argument("--mdwb-parent-buckets", type=parse_csv_ints, default=[8, 4, 2, 1])
    parser.add_argument("--log-dir", type=Path)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.trials <= 0:
        raise SystemExit("--trials must be greater than zero")
    if args.mdtest_result_inflight <= 0:
        raise SystemExit("--mdtest-result-inflight must be greater than zero")
    if (
        args.mdtest_result_channel_capacity is not None
        and args.mdtest_result_channel_capacity <= 0
    ):
        raise SystemExit("--mdtest-result-channel-capacity must be greater than zero")
    args.git_rev = git_rev()

    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    writes_bench_output = not args.skip_mdtest or not args.skip_mdworkbench
    if writes_bench_output and output.exists() and not args.append:
        output.unlink()
    latency_output = args.latency_output.resolve()
    latency_output.parent.mkdir(parents=True, exist_ok=True)
    if latency_output.exists() and not args.append and not args.skip_mdtest_latency:
        latency_output.unlink()
    log_dir = (args.log_dir or (output.parent / "run_logs")).resolve()

    if not args.skip_mdtest:
        run_mdtest(args, output, log_dir)
    if not args.skip_mdtest_latency:
        run_mdtest_latency(args, latency_output, log_dir)
    if not args.skip_mdworkbench:
        run_mdworkbench(args, output, log_dir)

    print(f"Wrote CSV results to {output}")
    if not args.skip_mdtest_latency:
        print(f"Wrote latency CSV results to {latency_output}")
    print(f"Wrote command logs to {log_dir}")


if __name__ == "__main__":
    main()
