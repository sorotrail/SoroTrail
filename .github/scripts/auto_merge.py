#!/usr/bin/env python3
"""Auto-merge open pull requests that are genuinely ready.

Policy, in order. A pull request is merged only when every one of these holds:

  1. It is open, not a draft, and carries the opt-in label.
  2. No reviewer has requested changes, and any required approvals are in.
  3. It has no merge conflict.
  4. Every required status check has actually passed - checked against the
     branch protection contexts, not inferred from GitHub's summary field.
  5. It is not behind the base branch. If it is, the branch is updated and the
     run stops there; the next run merges once CI has re-passed.

Anything that does not hold results in a skip and, for the states a human
needs to act on, a single explanatory comment.

This runs as automation and says so. It does not impersonate a person: the
comments are signed, and the account it posts under should be the Actions bot
or a GitHub App, both of which are permitted to automate. Dressing a bot up as
a human to avoid automation detection would violate GitHub's terms and put the
account at risk, which is the opposite of what this is for.
"""

import json
import os
import subprocess
import tempfile
import time

REPO = os.environ.get("REPO", "sorotrail/SoroTrail")
BASE = os.environ.get("BASE_BRANCH", "main")
LABEL = os.environ.get("AUTOMERGE_LABEL", "automerge")
REQUIRE_LABEL = os.environ.get("REQUIRE_LABEL", "true").lower() == "true"
MERGE_METHOD = os.environ.get("MERGE_METHOD", "merge")
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
ONLY_PR = os.environ.get("ONLY_PR", "").strip()
# Authors whose PRs may merge without the label, comma separated. Empty means
# the label is the only route in.
TRUSTED = [a.strip() for a in os.environ.get("TRUSTED_AUTHORS", "").split(",") if a.strip()]

# Every comment carries this so the bot can recognise its own previous notes
# and avoid repeating one that already stands.
MARKER = "<!-- sorotrail-automerge -->"


def gh(*args, check=False):
    """Run gh and return (exit_code, stdout, stderr)."""
    r = subprocess.run(["gh", *args], capture_output=True, text=True,
                       encoding="utf-8", errors="replace")
    if check and r.returncode != 0:
        raise RuntimeError("gh %s failed: %s" % (" ".join(args), r.stderr.strip()))
    return r.returncode, r.stdout, r.stderr


def api(path, method=None, fields=None, check=False):
    args = ["api"]
    if method:
        args += ["-X", method]
    args.append(path)
    for k, v in (fields or {}).items():
        args += ["-f", "%s=%s" % (k, v)]
    code, out, err = gh(*args, check=check)
    if code != 0:
        return None, err.strip()
    try:
        return json.loads(out) if out.strip() else {}, None
    except json.JSONDecodeError:
        return out, None


def log(pr, msg):
    print("PR #%s: %s" % (pr, msg), flush=True)


def required_contexts():
    """Read the required checks from branch protection.

    Falling back to an empty list would mean 'nothing is required', which would
    let the bot merge a PR with no passing checks at all. So a failure here is
    fatal rather than permissive.
    """
    data, err = api("repos/%s/branches/%s/protection/required_status_checks" % (REPO, BASE))
    if data is None:
        raise SystemExit(
            "Cannot read branch protection for %s (%s).\n"
            "Refusing to run: without the required-check list this bot cannot "
            "tell a green PR from an untested one." % (BASE, err))
    return list(data.get("contexts") or [])


def existing_comment_states(pr):
    """Return the set of states this bot has already reported on the PR."""
    data, _ = api("repos/%s/issues/%s/comments?per_page=100" % (REPO, pr))
    states = set()
    for c in data or []:
        body = c.get("body") or ""
        if MARKER in body:
            for line in body.splitlines():
                if line.startswith("state:"):
                    states.add(line.split(":", 1)[1].strip())
    return states


def comment_once(pr, state, text):
    """Post an explanatory comment, unless the same state was already reported.

    Re-posting the same note on every scheduled run would be noise, and noise
    is what makes people mute a bot.
    """
    if state in existing_comment_states(pr):
        log(pr, "comment for state '%s' already stands, not repeating" % state)
        return
    body = "%s\nstate: %s\n\n%s\n\n*Posted automatically by the auto-merge workflow.*" % (
        MARKER, state, text)
    if DRY_RUN:
        log(pr, "[dry-run] would comment: %s" % state)
        return
    # A real temp file, not the working directory: run locally, the previous
    # version dropped a comment.md next to the checkout, where it could be
    # committed by accident.
    fd, path = tempfile.mkstemp(prefix="automerge-comment-", suffix=".md")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(body)
        code, _, err = gh("pr", "comment", str(pr), "--repo", REPO, "--body-file", path)
        if code != 0:
            log(pr, "could not comment: %s" % err.strip())
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def check_state(pr, sha, required):
    """Return (ok, pending, failed) for the required contexts on this SHA.

    Both check-runs and legacy commit statuses are consulted, because a repo
    can carry either and a required context satisfied by a status would
    otherwise look permanently absent.
    """
    results = {}

    runs, _ = api("repos/%s/commits/%s/check-runs?per_page=100" % (REPO, sha))
    for run in (runs or {}).get("check_runs", []):
        name = run.get("name")
        if run.get("status") != "completed":
            results[name] = "pending"
        else:
            results[name] = ("pass" if run.get("conclusion") in ("success", "neutral", "skipped")
                             else "fail")

    statuses, _ = api("repos/%s/commits/%s/status" % (REPO, sha))
    for st in (statuses or {}).get("statuses", []):
        name = st.get("context")
        s = st.get("state")
        if name in results and results[name] == "pass":
            continue
        results[name] = {"success": "pass", "pending": "pending"}.get(s, "fail")

    pending = [c for c in required if results.get(c) in (None, "pending")]
    failed = [c for c in required if results.get(c) == "fail"]
    return (not pending and not failed), pending, failed


def review_state(pr):
    """Return (changes_requested, approvals)."""
    data, _ = api("repos/%s/pulls/%s/reviews?per_page=100" % (REPO, pr))
    latest = {}
    for r in data or []:
        state = r.get("state")
        if state in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            latest[(r.get("user") or {}).get("login")] = state
    return ("CHANGES_REQUESTED" in latest.values(),
            sum(1 for v in latest.values() if v == "APPROVED"))


def update_branch(pr, fork, can_modify):
    """Bring the PR branch up to date with the base.

    A fork PR opened without 'allow edits by maintainers' cannot be updated by
    anyone on this side - the API answers 403. That is a state only the
    contributor can resolve, so it gets an explanatory comment rather than a
    retry on every run.
    """
    if fork and not can_modify:
        comment_once(
            pr, "behind-cannot-update",
            "This branch is behind `%s`, and `%s` requires branches to be up to "
            "date before merging.\n\n"
            "The branch lives in a fork that was opened without **Allow edits by "
            "maintainers**, so nobody on the maintainer side can update it - the "
            "API returns 403.\n\n"
            "To unblock it, either:\n\n"
            "- tick **Allow edits by maintainers** in the pull request sidebar, or\n"
            "- merge `%s` into your branch and push.\n\n"
            "Once it is up to date and CI is green, this will merge on its own."
            % (BASE, BASE, BASE))
        return False

    if DRY_RUN:
        log(pr, "[dry-run] would update branch")
        return False

    data, err = api("repos/%s/pulls/%s/update-branch" % (REPO, pr), method="PUT")
    if data is None:
        log(pr, "update-branch failed: %s" % err)
        comment_once(pr, "update-failed",
                     "Tried to update this branch with `%s` and could not:\n\n"
                     "```\n%s\n```\n\nIt needs a manual update before it can merge."
                     % (BASE, (err or "")[:400]))
        return False
    log(pr, "branch updated; waiting for CI to re-run before merging")
    return True


def merge(pr, title):
    if DRY_RUN:
        log(pr, "[dry-run] would merge")
        return True
    code, _, err = gh("pr", "merge", str(pr), "--repo", REPO, "--" + MERGE_METHOD)
    if code != 0:
        log(pr, "merge failed: %s" % err.strip())
        comment_once(pr, "merge-failed",
                     "Everything looked ready, but the merge call was rejected:\n\n"
                     "```\n%s\n```\n\nLeaving this one for a human."
                     % err.strip()[:400])
        return False
    log(pr, "merged: %s" % title)
    return True


def consider(pr_data, required):
    num = pr_data["number"]
    title = pr_data.get("title", "")

    if pr_data.get("draft"):
        log(num, "skip: draft")
        return
    labels = [l["name"] for l in pr_data.get("labels", [])]
    author = (pr_data.get("user") or {}).get("login", "")
    if REQUIRE_LABEL and LABEL not in labels and author not in TRUSTED:
        log(num, "skip: no '%s' label" % LABEL)
        return

    # Re-read the PR: the list endpoint omits mergeable/mergeable_state, and
    # GitHub computes them lazily, so a fresh read is sometimes still null.
    detail, err = api("repos/%s/pulls/%s" % (REPO, num))
    if detail is None:
        log(num, "skip: cannot read PR (%s)" % err)
        return

    mergeable = detail.get("mergeable")
    state = detail.get("mergeable_state")
    if mergeable is None or state == "unknown":
        log(num, "skip: mergeability not computed yet, will retry next run")
        return

    if mergeable is False or state == "dirty":
        comment_once(num, "conflict",
                     "This branch has a merge conflict with `%s`, so it cannot be "
                     "merged automatically.\n\nResolving it needs a human - once the "
                     "conflict is gone and CI is green, this will pick it up again."
                     % BASE)
        log(num, "skip: conflict")
        return

    changes_requested, _ = review_state(num)
    if changes_requested:
        log(num, "skip: changes requested")
        return

    sha = detail["head"]["sha"]
    ok, pending, failed = check_state(num, sha, required)
    if failed:
        comment_once(num, "checks-failed",
                     "Not merging: the following required checks failed on `%s`.\n\n%s\n\n"
                     "Push a fix and this will re-evaluate on the next run."
                     % (sha[:7], "\n".join("- `%s`" % c for c in failed)))
        log(num, "skip: failed checks %s" % failed)
        return
    if pending:
        log(num, "skip: checks still pending %s" % pending)
        return

    # Green, no conflict. Update first if behind - the base branch requires it,
    # and merging a stale branch means merging code CI never tested together.
    if state == "behind":
        log(num, "behind %s, updating" % BASE)
        update_branch(num, detail["head"]["repo"]["full_name"] != REPO,
                      bool(detail.get("maintainer_can_modify")))
        return

    if state == "blocked":
        log(num, "skip: blocked by branch protection (reviews or other gate)")
        return

    if state not in ("clean", "has_hooks", "unstable"):
        log(num, "skip: unexpected mergeable_state '%s'" % state)
        return
    if state == "unstable":
        # Required checks all passed; something non-required is red. Merging is
        # allowed, but say so rather than doing it silently.
        log(num, "note: non-required checks are failing, required ones passed")

    merge(num, title)


def main():
    # Check gh can actually authenticate rather than that a particular
    # variable is set: in Actions the token arrives as GH_TOKEN, but locally
    # gh's own stored credentials are just as valid, and this lets the same
    # script be dry-run from a workstation before it is trusted in CI.
    code, _, err = gh("auth", "status")
    if code != 0:
        raise SystemExit("gh is not authenticated: %s" % err.strip())

    required = required_contexts()
    print("required checks on %s: %s" % (BASE, ", ".join(required) or "(none)"), flush=True)
    if not required:
        raise SystemExit(
            "Branch protection lists no required checks. Refusing to run: every "
            "PR would look green regardless of whether it was tested.")

    if ONLY_PR:
        detail, err = api("repos/%s/pulls/%s" % (REPO, ONLY_PR))
        if detail is None:
            raise SystemExit("Cannot read PR #%s: %s" % (ONLY_PR, err))
        prs = [detail]
    else:
        prs, err = api("repos/%s/pulls?state=open&base=%s&per_page=100" % (REPO, BASE))
        if prs is None:
            raise SystemExit("Cannot list pull requests: %s" % err)

    print("considering %d open pull request(s)" % len(prs), flush=True)
    for pr in prs:
        try:
            consider(pr, required)
        except Exception as exc:                                  # noqa: BLE001
            # One bad PR must not stop the sweep.
            print("PR #%s: unexpected error: %s" % (pr.get("number"), exc), flush=True)
        time.sleep(1)  # stay well clear of the secondary rate limit


if __name__ == "__main__":
    main()
