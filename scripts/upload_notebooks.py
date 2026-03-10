"""Upload exercise notebooks to Databricks workspace using the SDK.

Alternative to DABs — useful if you just want the notebooks in your workspace
without creating jobs.

Usage:
    pip install databricks-sdk
    export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
    export DATABRICKS_TOKEN=dapi...
    python scripts/upload_notebooks.py [--target-dir /Workspace/Users/you/exercises]
"""

import argparse
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language


def main():
    parser = argparse.ArgumentParser(description="Upload notebooks to Databricks")
    parser.add_argument(
        "--target-dir",
        default="/Workspace/Users/{user}/spark_databricks_project/exercises",
        help="Workspace path to upload notebooks into",
    )
    parser.add_argument(
        "--source-dir",
        default="exercises",
        help="Local directory containing exercise .py files",
    )
    args = parser.parse_args()

    w = WorkspaceClient()

    # Resolve {user} placeholder
    target_dir = args.target_dir
    if "{user}" in target_dir:
        me = w.current_user.me()
        target_dir = target_dir.replace("{user}", me.user_name)

    source = Path(args.source_dir)
    if not source.is_dir():
        print(f"Error: source directory '{source}' not found")
        sys.exit(1)

    py_files = sorted(source.glob("*.py"))
    if not py_files:
        print(f"No .py files found in {source}")
        sys.exit(1)

    print(f"Uploading {len(py_files)} notebooks to {target_dir}")

    # Ensure target directory exists
    try:
        w.workspace.mkdirs(target_dir)
    except Exception:
        pass  # already exists

    for f in py_files:
        notebook_path = f"{target_dir}/{f.stem}"
        content = f.read_bytes()
        print(f"  {f.name} -> {notebook_path}")
        w.workspace.import_(
            path=notebook_path,
            content=content,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )

    print(f"\nDone. {len(py_files)} notebooks uploaded to {target_dir}")


if __name__ == "__main__":
    main()
