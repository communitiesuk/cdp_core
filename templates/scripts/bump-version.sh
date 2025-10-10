#!/bin/bash
 set -e

# Detect the commit SHA that triggered the build
# (Azure Pipelines automatically sets this env var)
COMMIT_SHA="${BUILD_SOURCEVERSION:-$(git rev-parse HEAD)}"

echo "Using commit: $COMMIT_SHA"

# Get the full commit message for that specific commit
commit_msg=$(git show -s --pretty=%B "$COMMIT_SHA")
echo "Commit message: $commit_msg"

# Try to extract the merged branch name (e.g. "feature/cdp-123-new-feature")
merged_branch=$(echo "$commit_msg" | grep -oE 'from [^ ]*' | sed 's/from //')

# Fallback if merge message not found
if [ -z "$merged_branch" ]; then
  merged_branch=$(git rev-parse --abbrev-ref HEAD)
  echo "Could not detect merged branch, using current branch: $merged_branch"
else
  echo "Detected merged branch: $merged_branch"
fi

# Set a single branch variable for downstream use
branch_name="${merged_branch}"
echo "Final branch name: $branch_name"
 
# Get latest numeric tag (e.g., v1.2.3 or 1.2.3)
latest_tag=$(git tag | grep -E '^v?[0-9]+\.[0-9]+\.[0-9]+$' | sed 's/^v//' | sort -V | tail -n 1)
 
# Split version into components
IFS='.' read -r major minor patch <<< "$latest_tag"
 
# Determine bump type from branch name
if [[ "$branch_name" == *"PATCH"* ]]; then
    patch=$((patch + 1))
elif [[ "$branch_name" == *"MINOR"* ]]; then
    minor=$((minor + 1))
    patch=0
elif [[ "$branch_name" == *"MAJOR"* ]]; then
    major=$((major + 1))
    minor=0
    patch=0
else
    # Default to MAJOR if no keyword found
    major=$((major + 1))
    minor=0
    patch=0
fi
 
# Construct new version
new_tag="$major.$minor.$patch"
 
# Output the new tag
echo "New tag: $new_tag"
echo "##vso[task.setvariable variable=BUILD_VERSION;isOutput=true]$new_tag";
