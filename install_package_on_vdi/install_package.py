import os
import re
import subprocess
import requests
from pathlib import Path
import time

# Configuration
wheel_download_dir = Path("./wheelhouse")
wheel_download_dir.mkdir(exist_ok=True)

# Main function
def install_package_offline(package_name):
    max_retries = 10  # Avoid infinite loops
    attempt = 0

    while attempt < max_retries:
        attempt += 1
        print(f"\n🌀 Attempt {attempt}: pip install {package_name}\n")

        # Run pip install and capture stderr
        result = subprocess.run(
            ["pip", "install", package_name, "--no-cache-dir"],
            stderr=subprocess.PIPE,
            text=True
        )

        stderr_output = result.stderr

        # Check if install succeeded
        if result.returncode == 0:
            print("✅ Package installed successfully!")
            break

        # Extract all .whl URLs
        wheel_urls = re.findall(r"https://files\.pythonhosted\.org/[^\s]+\.whl", stderr_output)

        if not wheel_urls:
            print("❌ No .whl URLs found. Cannot proceed further.")
            print(stderr_output)
            break

        print(f"🔗 Found {len(wheel_urls)} wheel URL(s). Downloading...")

        # Download each wheel if not already downloaded
        for whl_url in wheel_urls:
            filename = whl_url.split("/")[-1]
            local_path = wheel_download_dir / filename
            if local_path.exists():
                print(f"✅ Already downloaded: {filename}")
                continue

            try:
                print(f"⬇️ Downloading {filename}...")
                r = requests.get(whl_url)
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    f.write(r.content)
                print(f"✅ Saved: {local_path}")
            except requests.RequestException as e:
                print(f"❌ Failed to download {whl_url}: {e}")

        # Add local wheelhouse to pip install options
        # This time, try installing from local wheels
        print("\n🔁 Retrying installation using local wheels...\n")
        package_name = wheel_download_dir.as_posix()  # Install from local wheel directory

    else:
        print("❌ Maximum retries reached. Some dependencies may still be missing.")

# 🔧 Entry point
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python install_package.py <package-name>")
        sys.exit(1)

    pkg = sys.argv[1]
    install_package_offline(pkg)
