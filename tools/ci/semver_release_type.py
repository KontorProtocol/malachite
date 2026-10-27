"""Require numeric version bumps for API breaks in unpublished fork releases."""

from pathlib import Path
import subprocess
import sys
import tomllib


def release_type(baseline: str, current: str) -> str:
    def numeric(version: str) -> tuple[int, int, int]:
        parts = tuple(int(part) for part in version.split("+")[0].split("-")[0].split("."))
        if len(parts) != 3 or any(part < 0 for part in parts):
            raise ValueError(f"invalid numeric version: {version}")
        return parts

    old, new = numeric(baseline), numeric(current)
    if new < old:
        raise ValueError(f"fork version regressed: {baseline} -> {current}")
    # Unchanged fork identities permit additive APIs, but not breaking changes.
    # cargo-semver-checks otherwise treats unchanged prereleases as major.
    if new == old:
        return "minor"
    if new[0] > old[0] or (new[0] == 0 and new[1] > old[1]):
        return "major"
    if new[2] > old[2] and new[:2] == (0, 0):
        return "major"
    if new[1] > old[1] or (new[0] == 0 and new[2] > old[2]):
        return "minor"
    return "patch"


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    baseline = tomllib.loads(subprocess.check_output(
        ["git", "show", f"{sys.argv[1]}:code/Cargo.toml"], cwd=root, text=True
    ))["workspace"]["package"]["version"]
    current = tomllib.loads((root / "code/Cargo.toml").read_text())["workspace"]["package"]["version"]
    print(release_type(baseline, current))


if __name__ == "__main__":
    main()
