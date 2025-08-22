#!/bin/bash
 
# Get current branch name
branch_name=$(git rev-parse --abbrev-ref HEAD)
 
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
new_version="$major.$minor.$patch"
new_tag="v$new_version"
 
# Output the new tag
echo "New tag: $new_tag"
 
# Optional: create and push the tag
# git tag "$new_tag"
# git push origin "$new_tag"