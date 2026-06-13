#!/usr/bin/env bash
# Compute the next release version from git tags and the head commit subject.
#
# With squash merges the head commit subject on main is the PR title, so the
# PR title decides the bump:
#   feat: / feat(scope):            -> minor
#   anything else (fix:, docs:, ..) -> patch
#   ! before the colon (feat!:) or at the end of the title -> major
#
# The current version is the highest existing semver git tag — Cargo.toml
# stays at its 0.0.0-dev placeholder and is stamped at Docker build time.
#
# Idempotent: if HEAD is already tagged (e.g. a workflow re-run after the tag
# job), that tag is reused instead of bumping again.
#
# Prints "version=X.Y.Z" (suitable for $GITHUB_OUTPUT).
set -euo pipefail

semver='^[0-9]+\.[0-9]+\.[0-9]+$'

existing=$(git tag --points-at HEAD | grep -E "$semver" | head -1 || true)
if [ -n "$existing" ]; then
    echo "version=$existing"
    exit 0
fi

last=$(git tag --list --sort=-v:refname | grep -E "$semver" | head -1 || true)
last=${last:-0.0.0}
IFS=. read -r major minor patch <<< "$last"

subject=$(git log -1 --format=%s)
subject=${subject% (#*)} # strip the " (#123)" suffix GitHub appends on squash merge

major_re='^[a-zA-Z]+(\([^)]*\))?!:'
minor_re='^feat(\([^)]*\))?:'
if [[ "$subject" =~ $major_re ]] || [[ "$subject" == *! ]]; then
    version="$((major + 1)).0.0"
elif [[ "$subject" =~ $minor_re ]]; then
    version="$major.$((minor + 1)).0"
else
    version="$major.$minor.$((patch + 1))"
fi

echo "version=$version"
