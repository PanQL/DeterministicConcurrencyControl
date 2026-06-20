#!/usr/bin/env python3
"""Plot IPSO metadata benchmark figures from CalvinFS benchmark CSV."""

from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from pathlib import Path
from statistics import mean

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLCONFIGDIR", str(REPO_ROOT / "target" / "matplotlib"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

plt.rcParams["font.sans-serif"] = [
    "Arial Unicode MS",
    "STHeiti",
    "Heiti TC",
    "PingFang SC",
    "Songti SC",
    "SimHei",
]
plt.rcParams["axes.unicode_minus"] = False

DEFAULT_CSV = REPO_ROOT / "results" / "ipso_metadata" / "ipso-metadata-madsim.csv"
DEFAULT_LATENCY_CSV = (
    REPO_ROOT / "results" / "ipso_metadata" / "ipso-metadata-latency-madsim.csv"
)
DEFAULT_OUT_DIR = REPO_ROOT / "results" / "ipso_metadata" / "figures"

SCHED_ORDER = ["calvin", "aria", "scc"]
SCHED_LABELS = {"calvin": "Calvin", "aria": "Aria", "scc": "SCC"}
SCHED_COLORS = {"calvin": "#4C78A8", "aria": "#F58518", "scc": "#54A24B"}
SCHED_MARKERS = {"calvin": "o", "aria": "^", "scc": "s"}
SCHED_LINESTYLES = {"calvin": "-", "aria": "-.", "scc": "--"}

LEGEND_FONTSIZE = 11
PANEL_TITLE_FONTSIZE = 13
AXIS_LABEL_FONTSIZE = 12
TICK_LABEL_FONTSIZE = 11

MDTEST_OPERATIONS = [
    ("file_create", "文件创建"),
    ("file_stat", "属性查询"),
    ("file_unlink", "文件删除"),
]
MDTEST_MODES = [("private", "私有目录"), ("public", "公共目录")]


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def as_float(row: dict[str, str], key: str) -> float | None:
    value = row.get(key, "")
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def mean_by_x(
    rows: list[dict[str, str]],
    x_key: str,
    y_key: str,
    *,
    y_scale: float = 1.0,
) -> tuple[list[float], list[float]]:
    grouped: dict[float, list[float]] = defaultdict(list)
    for row in rows:
        x_value = as_float(row, x_key)
        y_value = as_float(row, y_key)
        if x_value is None or y_value is None:
            continue
        grouped[x_value].append(y_value / y_scale)
    xs = sorted(grouped)
    ys = [mean(grouped[x]) for x in xs]
    return xs, ys


def plot_scheduler_lines(
    ax: plt.Axes,
    rows: list[dict[str, str]],
    x_key: str,
    y_key: str,
    *,
    y_scale: float = 1.0,
) -> None:
    for scheduler in SCHED_ORDER:
        scheduler_rows = [row for row in rows if row.get("scheduler") == scheduler]
        xs, ys = mean_by_x(scheduler_rows, x_key, y_key, y_scale=y_scale)
        if not xs:
            continue
        ax.plot(
            xs,
            ys,
            color=SCHED_COLORS[scheduler],
            marker=SCHED_MARKERS[scheduler],
            linewidth=1.8,
            markersize=5.5,
            label=SCHED_LABELS[scheduler],
        )


def style_axis(ax: plt.Axes) -> None:
    ax.grid(True, axis="y", alpha=0.28, linewidth=0.8)
    ax.grid(True, axis="x", alpha=0.12, linewidth=0.6)
    ax.tick_params(labelsize=TICK_LABEL_FONTSIZE)


def add_horizontal_figure_legend(
    fig: plt.Figure,
    handles: list,
    labels: list[str],
) -> None:
    if handles:
        fig.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 0.985),
            ncol=len(handles),
            frameon=False,
            fontsize=LEGEND_FONTSIZE,
        )


def plot_mdtest(rows: list[dict[str, str]], out_dir: Path) -> Path:
    mdtest_rows = [row for row in rows if row.get("workload") == "mdtest"]
    client_ticks = sorted(
        {
            int(value)
            for row in mdtest_rows
            if (value := as_float(row, "clients")) is not None
        }
    )
    fig, axes = plt.subplots(2, 3, figsize=(11.2, 6.3), sharex=True)
    for row_index, (mode, mode_label) in enumerate(MDTEST_MODES):
        for col_index, (operation, operation_label) in enumerate(MDTEST_OPERATIONS):
            ax = axes[row_index][col_index]
            selected = [
                row
                for row in mdtest_rows
                if row.get("mode") == mode and row.get("operation") == operation
            ]
            plot_scheduler_lines(ax, selected, "clients", "ops_per_sec", y_scale=1000.0)
            ax.set_title(operation_label, fontsize=PANEL_TITLE_FONTSIZE)
            if row_index == 1:
                ax.set_xlabel("客户端数量", fontsize=AXIS_LABEL_FONTSIZE)
            if col_index == 0:
                ax.set_ylabel(
                    f"{mode_label}\n吞吐量（千次/秒）", fontsize=AXIS_LABEL_FONTSIZE
                )
            if client_ticks:
                ax.set_xscale("log", base=2)
                ax.set_xticks(client_ticks)
                ax.set_xticklabels([str(value) for value in client_ticks])
            style_axis(ax)
            ax.tick_params(axis="x", labelbottom=True)

    handles, labels = axes[0][0].get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ordered_labels = [
        SCHED_LABELS[scheduler]
        for scheduler in SCHED_ORDER
        if SCHED_LABELS[scheduler] in by_label
    ]
    ordered_handles = [by_label[label] for label in ordered_labels]
    add_horizontal_figure_legend(fig, ordered_handles, ordered_labels)
    fig.tight_layout(rect=(0, 0, 1, 0.93), h_pad=1.0)
    out_path = out_dir / "mdtest-scalability.pdf"
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def plot_mdworkbench(
    rows: list[dict[str, str]],
    *,
    mode: str,
    x_key: str,
    x_label: str,
    output_name: str,
    out_dir: Path,
) -> Path:
    selected = [
        row
        for row in rows
        if row.get("workload") == "mdworkbench"
        and row.get("mode") == mode
        and row.get("operation") == "benchmark"
    ]
    fig, axes = plt.subplots(
        2,
        1,
        figsize=(7.4, 5.5),
        sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1.0]},
    )

    plot_scheduler_lines(axes[0], selected, x_key, "ops_per_sec", y_scale=1000.0)
    axes[0].set_ylabel("吞吐量（千次/秒）", fontsize=AXIS_LABEL_FONTSIZE)
    style_axis(axes[0])

    plot_scheduler_lines(axes[1], selected, x_key, "fallback_tx_count")
    axes[1].set_xlabel(x_label, fontsize=AXIS_LABEL_FONTSIZE)
    axes[1].set_ylabel("回退事务数", fontsize=AXIS_LABEL_FONTSIZE)
    style_axis(axes[1])

    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        axes[0].legend(
            handles,
            labels,
            loc="lower center",
            bbox_to_anchor=(0.5, 1.02),
            ncol=len(handles),
            frameon=False,
            fontsize=LEGEND_FONTSIZE,
            borderaxespad=0.0,
        )

    fig.tight_layout(pad=0.6, h_pad=0.7)
    out_path = out_dir / output_name
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def cdf_points(values: list[float]) -> tuple[list[float], list[float]]:
    xs = sorted(value for value in values if value > 0)
    if not xs:
        return [], []
    count = len(xs)
    ys = [(index + 1) / count for index in range(count)]
    return xs, ys


def plot_mdtest_latency_cdf(
    rows: list[dict[str, str]],
    out_dir: Path,
    *,
    clients: int,
) -> Path:
    latency_rows = [
        row
        for row in rows
        if row.get("workload") == "mdtest"
        and row.get("status") == "ok"
        and as_float(row, "clients") == float(clients)
    ]
    fig, axes = plt.subplots(2, 3, figsize=(11.2, 6.3), sharex=False, sharey=True)
    for row_index, (mode, mode_label) in enumerate(MDTEST_MODES):
        for col_index, (operation, operation_label) in enumerate(MDTEST_OPERATIONS):
            ax = axes[row_index][col_index]
            selected = [
                row
                for row in latency_rows
                if row.get("mode") == mode and row.get("operation") == operation
            ]
            for scheduler in ["scc", "aria", "calvin"]:
                values = [
                    value
                    for row in selected
                    if row.get("scheduler") == scheduler
                    and (value := as_float(row, "latency_ms")) is not None
                ]
                xs, ys = cdf_points(values)
                if not xs:
                    continue
                ax.plot(
                    xs,
                    ys,
                    color=SCHED_COLORS[scheduler],
                    linestyle=SCHED_LINESTYLES[scheduler],
                    linewidth=1.8,
                    label=SCHED_LABELS[scheduler],
                )
            ax.set_title(operation_label, fontsize=PANEL_TITLE_FONTSIZE)
            if row_index == 1:
                ax.set_xlabel("延迟（毫秒）", fontsize=AXIS_LABEL_FONTSIZE)
            if col_index == 0:
                ax.set_ylabel(f"{mode_label}\nCDF", fontsize=AXIS_LABEL_FONTSIZE)
            style_axis(ax)

    handles, labels = axes[0][0].get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ordered_labels = [
        SCHED_LABELS[scheduler]
        for scheduler in SCHED_ORDER
        if SCHED_LABELS[scheduler] in by_label
    ]
    ordered_handles = [by_label[label] for label in ordered_labels]
    add_horizontal_figure_legend(fig, ordered_handles, ordered_labels)
    fig.tight_layout(rect=(0, 0, 1, 0.93), h_pad=1.0)
    out_path = out_dir / "mdtest-latency-cdf.pdf"
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", nargs="?", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--latency-csv", type=Path, default=DEFAULT_LATENCY_CSV)
    parser.add_argument("--latency-clients", type=int, default=32)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    rows = read_rows(args.csv)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    outputs = [
        plot_mdtest(rows, args.out_dir),
        plot_mdworkbench(
            rows,
            mode="bucket-hotness",
            x_key="fan_in",
            x_label="父目录扇入度（N/M）",
            output_name="mdwb-bucket-hotness.pdf",
            out_dir=args.out_dir,
        ),
    ]
    if args.latency_csv.exists():
        latency_rows = read_rows(args.latency_csv)
        outputs.append(
            plot_mdtest_latency_cdf(
                latency_rows, args.out_dir, clients=args.latency_clients
            )
        )
    for output in outputs:
        print(f"已保存 {output}")


if __name__ == "__main__":
    main()
