---
name: pr-review-fix
description: Evaluate and address unresolved PR review comments with adversarial analysis. Use when the user asks to "address PR comments", "fix PR review", "go through review feedback", or similar. Defaults to processing all unresolved threads on the PR for the current branch, picking up where the last run left off.
---

# pr-review-fix

Evaluate unresolved PR review comments, fix the ones whose suggestion is well-supported by evidence, and consult the user on the rest.

## Inputs

The user may specify:
- A PR number ("address comments on #485"). Otherwise, find the PR for the current branch via `gh pr list --head $(git branch --show-current) --json number`.
- A count ("last 3 unresolved"). Otherwise, process **all** unresolved threads, picking up after the last-processed one (see State).
- A scope override ("all from scratch", "only Copilot", etc.).

If no PR is found for the current branch, ask the user.

## State

Persist progress per repo+PR in `.claude/pr-review-fix-state.json` at the repo root (gitignored if not already - check `.gitignore` and add the entry only if absent). Schema:

```json
{
  "<pr-number>": {
    "lastProcessedCommentId": 3214242325,
    "lastProcessedAt": "2026-05-10T03:20:05Z",
    "decisions": [
      { "commentId": 3214242308, "outcome": "fixed", "summary": "stripped non-alphanumeric in safeSetCode" },
      { "commentId": 3214242319, "outcome": "fixed", "summary": "added length>0 assertion" },
      { "commentId": 3214242325, "outcome": "consulted", "summary": "user chose Object.freeze" }
    ]
  }
}
```

On each run:
1. Load the state file (create if missing).
2. Filter unresolved threads to those with `createdAt > lastProcessedAt` (or all, if user said "from scratch").
3. After each comment is handled, append to `decisions` and update `lastProcessedCommentId` / `lastProcessedAt` immediately - so an interrupted run resumes correctly.

## Fetching unresolved comments

The REST `/pulls/:n/comments` endpoint does not expose resolution status. Use GraphQL:

```bash
gh api graphql -f query='
query($owner:String!, $name:String!, $pr:Int!) {
  repository(owner:$owner, name:$name) {
    pullRequest(number:$pr) {
      reviewThreads(first:100) {
        nodes {
          id
          isResolved
          comments(first:10) {
            nodes { databaseId path line body createdAt author { login } }
          }
        }
      }
    }
  }
}' -F owner=<owner> -F name=<repo> -F pr=<num>
```

Filter `isResolved == false`, sort threads by the first comment's `createdAt` ascending. The thread `id` (GraphQL node ID) is what you need to resolve the thread later.

## Evaluation: 90:10 adversarial standard

For each comment, do an honest adversarial analysis **before** deciding:

1. **Read the cited code** at the path/line. Read enough surrounding context to understand the call sites, storage, and assumptions involved.
2. **Steelman the suggestion**: list concrete evidence that the comment is correct (failing scenarios, broken invariants, divergence between code and storage, missing assertions, etc.).
3. **Steelman the opposite**: actively try to disprove the suggestion. Look for:
   - Existing code/tests that contradict the comment's premise (a common failure mode of bot reviewers).
   - Whether the "bug" is actually unreachable given other validation.
   - Whether the suggested change would break callers, tests, or downstream behavior.
   - Whether the suggestion is stylistic/theoretical with no real defect.
4. **Weigh the evidence**:
   - Quality matters more than quantity. A direct grep result beats a plausible-sounding hypothesis.
   - If two pieces of evidence directly contradict, **nullify both** unless one is materially more likely true (e.g., observed in code now vs. recalled from memory).
   - Estimate the ratio of remaining quality evidence: roughly what fraction favors the suggestion vs. against.
5. **Decide**:
   - **>=90% in favor of the suggestion** AND the fix is small/local/reversible -> implement it.
   - **>=90% against** -> mark `wont-fix`, note the reason for the user, optionally reply on the thread.
   - **Anything in between** -> stop and consult: present the evidence on both sides, your tentative lean, and ask the user to weigh in.

Document the ratio call in the per-comment `decisions` entry's `summary` so future runs (and the user) can audit it.

## Implementation

When implementing a fix:
- Make only the change the comment asks for. Do not refactor adjacent code.
- Run targeted tests after each fix (`npm test -- --testPathPattern=...` or the project equivalent). If a test command is unclear, check `CLAUDE.md` or `package.json`.
- If a fix breaks an existing test, that's signal: re-evaluate whether the suggestion was right. Surface to the user before continuing.

## Resolving threads

After a fix is implemented and tests pass, resolve the thread:

```bash
gh api graphql -f query='
mutation($id:ID!) { resolveReviewThread(input:{threadId:$id}) { thread { isResolved } } }' \
  -F id=<thread-node-id>
```

Only resolve when:
- Outcome was `fixed` and tests pass, **or**
- Outcome was `wont-fix` after explicit user confirmation.

Never resolve a thread the user is still weighing in on.

## Reporting

At the end of the run, summarize in 1-2 sentences per comment:
- What was done (`fixed` / `wont-fix` / `consulted-pending`)
- One-line evidence summary
- Files touched

Do not produce a long writeup unless asked.

## Anti-patterns

- Do **not** trust the comment just because a reviewer wrote it (especially Copilot/bot reviewers - they frequently misread context).
- Do **not** silently skip comments. If skipping, log it in `decisions` with a reason.
- Do **not** batch resolutions at the end - resolve as you go so an interrupted run leaves the PR in a coherent state.
- Do **not** create commits unless the user explicitly asks.
