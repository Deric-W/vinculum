#!/usr/bin/env python3

"""Utility script for generating benchmark data as CSV"""

import csv
import itertools
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import random
from argparse import ArgumentParser, FileType, Namespace
from collections.abc import Callable
from contextlib import ExitStack, contextmanager
from pathlib import Path

ARGS = ArgumentParser(description=__doc__)
ARGS.add_argument("location", type=Path, help="Location for temporary data")
ARGS.add_argument(
    "-o",
    "--output",
    type=FileType("w", encoding="utf8"),
    default=sys.stdout,
    help="Output file",
)
ARGS.add_argument(
    "-c", "--chunk-size", type=int, default=1048576, help="Chunk size in bytes"
)
ARGS.add_argument("--borg1", type=str, default="borg", help="Borg1 executable")
ARGS.add_argument("--borg2", type=str, default="borg", help="Borg2 executable")
ARGS.add_argument(
    "--chunks-per-manifest",
    type=int,
    help="Number of chunks per manifest, defaults to the total number of \
        chunks divided by the number of manifests",
)
ARGS.add_argument(
    "-a",
    "--auxiliary-dir",
    type=Path,
    default=Path(tempfile.gettempdir()),
    help="Directory for auxiliary files (like caches)",
)
for i in ("chunks", "manifests"):
    ARGS.add_argument(f"--{i}-start", type=int, help=f"Starting number of {i}")
    ARGS.add_argument(f"--{i}-step", type=int, default=1, help=f"Step size of {i}")
    ARGS.add_argument(f"--{i}-end", type=int, help=f"Final number of {i}")


def measure_call(callable: Callable) -> int:
    before = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    callable()
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC) - before


@contextmanager
def setup_borg1(args: Namespace) -> None:
    repo = args.location / "borg1_repo"
    try:
        shutil.rmtree(repo)
    except FileNotFoundError:
        pass
    base = args.auxiliary_dir / "borg1_base"
    try:
        shutil.rmtree(base)
    except FileNotFoundError:
        pass
    env = dict(os.environ)
    env["BORG_BASE_DIR"] = str(base)
    subprocess.run(
        (args.borg1, "init", "--encryption=none", repo),
        stdout=sys.stderr,
        env=env,
        check=True,
    )
    try:
        yield
    finally:
        shutil.rmtree(repo)
        shutil.rmtree(base)


def create_borg1_manifest(name: str, args: Namespace) -> subprocess.Popen:
    (file,) = (args.location / "dataset").iterdir()
    env = dict(os.environ)
    env["BORG_BASE_DIR"] = str(args.auxiliary_dir / "borg1_base")
    return subprocess.Popen(
        (
            args.borg1,
            "create",
            "--compression=none",
            f"--chunker-params=fixed,{args.chunk_size}",
            f"{args.location / 'borg1_repo'}::{name}",
            file,
        ),
        stdout=sys.stderr,
        env=env,
    )


def prune_borg1_manifest(name: str, args: Namespace) -> tuple[int, int]:
    env = dict(os.environ)
    env["BORG_BASE_DIR"] = str(args.auxiliary_dir / "borg1_base")
    deletion = measure_call(
        lambda: subprocess.run(
            (args.borg1, "delete", f"{args.location / 'borg1_repo'}::{name}"),
            stdout=sys.stderr,
            env=env,
            check=True,
        )
    )
    compaction = measure_call(
        lambda: subprocess.run(
            (args.borg1, "compact", args.location / "borg1_repo"),
            stdout=sys.stderr,
            env=env,
            check=True,
        )
    )
    return (deletion, compaction)


@contextmanager
def setup_borg2(args: Namespace) -> None:
    repo = args.location / "borg2_repo"
    try:
        shutil.rmtree(repo)
    except FileNotFoundError:
        pass
    base = args.auxiliary_dir / "borg2_base"
    try:
        shutil.rmtree(base)
    except FileNotFoundError:
        pass
    env = dict(os.environ)
    env["BORG_BASE_DIR"] = str(base)
    subprocess.run(
        (
            args.borg2,
            "repo-create",
            "--repo",
            repo,
            "--encryption=none",
        ),
        stdout=sys.stderr,
        env=env,
        check=True,
    )
    try:
        yield
    finally:
        shutil.rmtree(repo)
        shutil.rmtree(base)


def create_borg2_manifest(name: str, args: Namespace) -> subprocess.Popen:
    (file,) = (args.location / "dataset").iterdir()
    env = dict(os.environ)
    env["BORG_BASE_DIR"] = str(args.auxiliary_dir / "borg2_base")
    return subprocess.Popen(
        (
            args.borg2,
            "create",
            "--repo",
            args.location / "borg2_repo",
            "--compression=none",
            f"--chunker-params=fixed,{args.chunk_size}",
            name,
            file,
        ),
        stdout=sys.stderr,
        env=env,
    )


def prune_borg2_manifest(name: str, args: Namespace) -> tuple[int, int]:
    env = dict(os.environ)
    env["BORG_BASE_DIR"] = str(args.auxiliary_dir / "borg2_base")
    deletion = measure_call(
        lambda: subprocess.run(
            (args.borg2, "delete", "--repo", args.location / "borg2_repo", name),
            stdout=sys.stderr,
            env=env,
            check=True,
        )
    )
    compaction = measure_call(
        lambda: subprocess.run(
            (args.borg2, "compact", "--repo", args.location / "borg2_repo"),
            stdout=sys.stderr,
            env=env,
            check=True,
        )
    )
    return (deletion, compaction)


@contextmanager
def setup_duplicacy(args: Namespace) -> None:
    storage = args.location / "duplicacy_storage"
    try:
        shutil.rmtree(storage)
    except FileNotFoundError:
        pass
    prefs = args.auxiliary_dir / "duplicacy_prefs"
    try:
        shutil.rmtree(prefs)
    except FileNotFoundError:
        pass
    try:
        os.unlink(Path.cwd() / ".duplicacy")
    except FileNotFoundError:
        pass
    subprocess.run(
        (
            "duplicacy",
            "init",
            "-repository",
            args.location / "dataset",
            "-pref-dir",
            str(prefs),
            "-c",
            str(args.chunk_size),
            "-min",
            str(args.chunk_size),
            "-max",
            str(args.chunk_size),
            "benchmark",
            storage.absolute(),
        ),
        stdout=sys.stderr,
        check=True,
    )
    try:
        config = json.loads((storage / "config").read_bytes())
        assert "compression-level" in config
        config["compression-level"] = 0
        (storage / "config").write_text(json.dumps(config), encoding="utf8")
        yield
    finally:
        os.unlink(Path.cwd() / ".duplicacy")
        shutil.rmtree(storage)
        shutil.rmtree(prefs)


def create_duplicacy_manifest() -> subprocess.Popen:
    return subprocess.Popen(("duplicacy", "backup"), stdout=sys.stderr)


def prune_duplicacy_manifest(revision: int) -> tuple[int, int]:
    collection = measure_call(
        lambda: subprocess.run(
            (
                "duplicacy",
                "prune",
                "-id",
                "benchmark",
                "-r",
                str(revision),
                "-collect-only",
                "-threads",
                "1",
            ),
            stdout=sys.stderr,
            check=True,
        )
    )
    time.sleep(1)
    subprocess.run(("duplicacy", "backup"), stdout=sys.stderr, check=True)
    deletion = measure_call(
        lambda: subprocess.run(
            ("duplicacy", "prune", "-delete-only", "-threads", "1"),
            stdout=sys.stderr,
            check=True,
        )
    )
    return (collection, deletion)


@contextmanager
def setup_vinculum(args: Namespace) -> None:
    repo = args.location / "vinculum_repo"
    try:
        shutil.rmtree(repo)
    except FileNotFoundError:
        pass
    subprocess.run(
        ("vinculum-benchmark", "init", repo),
        stdout=sys.stderr,
        check=True,
    )
    try:
        subprocess.run(
            (
                "vinculum-benchmark",
                "add-client",
                repo,
                "benchmark",
            ),
            stdout=sys.stderr,
            check=True,
        )
        yield
    finally:
        shutil.rmtree(repo)


def create_vinculum_manifest(name: str, args: Namespace) -> subprocess.Popen:
    (file,) = (args.location / "dataset").iterdir()
    return subprocess.Popen(
        (
            "vinculum-benchmark",
            "create",
            args.location / "vinculum_repo",
            name,
            "benchmark",
            file,
            "--chunk-size",
            str(args.chunk_size),
        ),
        stdout=sys.stderr,
    )


def prune_vinculum_manifest(name: str, args: Namespace) -> tuple[int, int]:
    collection = measure_call(
        lambda: subprocess.run(
            (
                "vinculum-benchmark",
                "collect",
                args.location / "vinculum_repo",
                "/tmp/vinculum_benchmark.cbor",
                name,
                "--parallelism",
                "1",
            ),
            stdout=sys.stderr,
            check=True,
        )
    )
    time.sleep(1)
    subprocess.run(
        (
            "vinculum-benchmark",
            "create",
            args.location / "vinculum_repo",
            f"hack_{name}",
            "benchmark",
            "/dev/null",
        ),
        stdout=sys.stderr,
        check=True,
    )
    deletion = measure_call(
        lambda: subprocess.run(
            (
                "vinculum-benchmark",
                "delete",
                args.location / "vinculum_repo",
                "/tmp/vinculum_benchmark.cbor",
                "--parallelism",
                "1",
            ),
            stdout=sys.stderr,
            check=True,
        )
    )
    return (collection, deletion)


@contextmanager
def create_manifests(chunks: int, manifests: int, args: Namespace) -> None:
    assert manifests > 0
    if args.chunks_per_manifest is not None:
        assert args.chunks_per_manifest <= chunks
    path = args.location / "dataset" / "dataset"
    try:
        for manifest in range(manifests):
            with open(path, "wb") as dataset:
                if args.chunks_per_manifest is None:
                    print(
                        f"Generating '{path}' with {chunks // manifests} chunks...",
                        file=sys.stderr,
                    )
                    manifest_part = args.chunk_size // 2
                    chunk_part = args.chunk_size - manifest_part
                    for chunk in range(chunks // manifests):
                        dataset.write(
                            manifest.to_bytes(manifest_part, "big", signed=False)
                        )
                        dataset.write(chunk.to_bytes(chunk_part, "big", signed=False))
                else:
                    print(
                        f"Generating '{path}' with {args.chunks_per_manifest} random chunks...",
                        file=sys.stderr,
                    )
                    for chunk in random.sample(range(chunks), args.chunks_per_manifest):
                        dataset.write(
                            chunk.to_bytes(args.chunk_size, "big", signed=False)
                        )
            processes = []
            try:
                processes.append(create_borg1_manifest(f"manifest_{manifest}", args))
                processes.append(create_borg2_manifest(f"manifest_{manifest}", args))
                processes.append(create_duplicacy_manifest())
                processes.append(create_vinculum_manifest(f"manifest_{manifest}", args))
                while processes:
                    process = processes.pop()
                    ret = process.wait()
                    if ret != 0:
                        raise RuntimeError(f"process {process} terminated with {ret}")
            finally:
                for process in processes:
                    process.terminate()
                for process in processes:
                    process.wait()
    finally:
        os.unlink(path)


if __name__ == "__main__":
    args = ARGS.parse_args()
    chunk_range = range(args.chunks_start, args.chunks_end, args.chunks_step)
    manifest_range = range(
        args.manifests_start, args.manifests_end, args.manifests_step
    )
    writer = csv.writer(args.output)
    writer.writerow(
        (
            "chunks",
            "manifests",
            "Borg 1 deletion",
            "Borg 1 compaction",
            "Borg 2 deletion",
            "Borg 2 compaction",
            "Duplicacy collection",
            "Duplicacy deletion",
            "Vinculum collection",
            "Vinculum deletion",
        )
    )
    dataset = args.location / "dataset"
    try:
        shutil.rmtree(dataset)
    except FileNotFoundError:
        pass
    for chunks, manifests in itertools.product(chunk_range, manifest_range):
        print(
            f"Running measurements with chunks={chunks} and manifests={manifests}...",
            file=sys.stderr,
        )
        os.mkdir(dataset)
        with ExitStack() as exit_stack:
            exit_stack.callback(lambda: shutil.rmtree(dataset))
            exit_stack.enter_context(setup_borg1(args))
            exit_stack.enter_context(setup_borg2(args))
            exit_stack.enter_context(setup_duplicacy(args))
            exit_stack.enter_context(setup_vinculum(args))
            create_manifests(chunks, manifests, args)
            (dataset / "dataset").write_bytes(b"")
            os.sync()
            borg1_deletion, borg1_compaction = prune_borg1_manifest("manifest_0", args)
            os.sync()
            borg2_deletion, borg2_compaction = prune_borg2_manifest("manifest_0", args)
            os.sync()
            duplicacy_collection, duplicacy_deletion = prune_duplicacy_manifest(1)
            os.sync()
            vinculum_collection, vinculum_deletion = prune_vinculum_manifest(
                "manifest_0", args
            )
            writer.writerow(
                (
                    chunks,
                    manifests,
                    borg1_deletion / 1e9,
                    borg1_compaction / 1e9,
                    borg2_deletion / 1e9,
                    borg2_compaction / 1e9,
                    duplicacy_collection / 1e9,
                    duplicacy_deletion / 1e9,
                    vinculum_collection / 1e9,
                    vinculum_deletion / 1e9,
                )
            )
    print("Done!", file=sys.stderr)
