#!/usr/bin/env python3
"""Extract a JSON service account from .env into secrets/sa.json and rewrite .env.

Usage: python scripts/extract_sa.py [path_to_project]
If run without args, assumes current working directory.
"""
import json
import os
import re
import shutil
import sys


def main(root: str):
    env_path = os.path.join(root, ".env")
    if not os.path.exists(env_path):
        print(".env not found at", env_path)
        return 1

    with open(env_path, "r", encoding="utf-8") as f:
        raw = f.read()

    # find the largest JSON-looking blob between first { and last }
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        print("No JSON blob found in .env; nothing to do.")
        return 0

    candidate = raw[start : end + 1]
    try:
        json.loads(candidate)
    except Exception as e:
        print("Found braces in .env but content is not valid JSON:", e)
        return 2

    secrets_dir = os.path.join(root, "secrets")
    os.makedirs(secrets_dir, exist_ok=True)
    sa_path = os.path.join(secrets_dir, "sa.json")

    # Backup original .env
    shutil.copy2(env_path, env_path + ".bak")

    # Write service account JSON
    with open(sa_path, "w", encoding="utf-8") as f:
        f.write(candidate)
    os.chmod(sa_path, 0o600)

    # Remove the JSON blob from .env content
    new_env = raw[:start] + raw[end + 1 :]

    # Ensure GOOGLE_SERVICE_ACCOUNT_FILE line exists and points to /secrets/sa.json
    if "GOOGLE_SERVICE_ACCOUNT_FILE" not in new_env:
        if not new_env.endswith("\n"):
            new_env += "\n"
        new_env += "GOOGLE_SERVICE_ACCOUNT_FILE=/secrets/sa.json\n"
    else:
        # replace existing line
        new_env = re.sub(r"^GOOGLE_SERVICE_ACCOUNT_FILE=.*$", "GOOGLE_SERVICE_ACCOUNT_FILE=/secrets/sa.json", new_env, flags=re.M)

    # Remove excessive blank lines
    new_env = re.sub(r"\n{3,}", "\n\n", new_env)

    with open(env_path, "w", encoding="utf-8") as f:
        f.write(new_env)

    print("Extracted service account to", sa_path)
    print("Backed up original .env to .env.bak and wrote cleaned .env")
    return 0


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    raise SystemExit(main(root))
