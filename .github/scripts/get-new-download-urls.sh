#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="New download urls"
BRANCH_NAME="get-new-download-urls"
PR_TITLE="$JOB_NAME"
PR_BODY="Automated PR for new download URLs."

# Run the CLI and capture output
SCRAPE_RESULT=$(opendpm scrape --json)

# Always try to create the branch (safe if it already exists)
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git checkout -B "$BRANCH_NAME"
git commit --allow-empty -m "Trigger PR for new download URLs" || true
git push -u origin "$BRANCH_NAME"

# Try to create the PR (ignore error if it already exists)
gh pr create --title "$PR_TITLE" --body "$PR_BODY" --head "$BRANCH_NAME" || true

# Get the PR number for the branch
PR_NUMBER=$(gh pr view "$BRANCH_NAME" --json number --jq '.number')

# Always comment on the PR with the scrape result
COMMENT_BODY="Result of opendpm scrape:\n\n\`\`\`\n$SCRAPE_RESULT\n\`\`\`"
gh pr comment "$PR_NUMBER" --body "$COMMENT_BODY"
