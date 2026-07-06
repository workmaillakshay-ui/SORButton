import os
import sys
import time
import logging
import requests
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials


JOTFORM_BASE_URL  = "https://pw.jotform.com/API"  # swap to api.jotform.com if non-enterprise
PAGE_SIZE         = 1000  # JotForm's hard max per request is 1000; loop below pages past that
THREAD_PAGE_SIZE  = 1000
REQUEST_DELAY     = 0.15
MAX_WORKERS       = 12    # concurrent submission-thread fetches; lower this if you hit 429s
HEADERS = ["Unique ID", "Submission Date", "Status", "Approval Status", "Date"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# These are read from environment variables so the same code works locally
# and as a Cloud Function (set them as env vars / secrets in the deploy step).
JOTFORM_API_KEY   = os.environ.get("JOTFORM_API_KEY", "YOUR_JOTFORM_API_KEY")
JOTFORM_FORM_ID   = os.environ.get("JOTFORM_FORM_ID", "YOUR_FORM_ID")
GOOGLE_SHEET_ID   = os.environ.get("GOOGLE_SHEET_ID", "YOUR_GOOGLE_SHEET_ID")
GOOGLE_CREDS_FILE = os.environ.get("GOOGLE_CREDS_FILE", "service_account.json")
SHEET_NAME        = "Sheet3"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_session = requests.Session()


# ─── JotForm API ─────────────────────────────────────────────────────────────

def jf_get(endpoint: str, params: dict = None) -> dict:
    url = f"{JOTFORM_BASE_URL}{endpoint}"
    p = {"apikey": JOTFORM_API_KEY}
    p.update(params or {})
    resp = _session.get(url, params=p, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("responseCode") != 200:
        raise RuntimeError(f"JotForm error on {endpoint}: {data}")
    return data


def fetch_all_submissions(form_id: str) -> list[dict]:
    submissions, offset = [], 0
    while True:
        log.info("Fetching submissions offset=%d ...", offset)
        data = jf_get(
            f"/form/{form_id}/submissions",
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "orderby": "created_at",
                "direction": "ASC",
                "addWorkflowStatus": 1,
            },
        )
        batch = data.get("content", [])
        submissions.extend(batch)
        total = data["resultSet"]["count"]
        offset += len(batch)
        if offset >= total or not batch:
            break
        time.sleep(REQUEST_DELAY)
    log.info("Total submissions: %d", len(submissions))
    return submissions


def fetch_thread(submission_id: str) -> list[dict]:
    """The thread endpoint is paginated - page through it fully."""
    events, offset = [], 0
    while True:
        data = jf_get(
            f"/submission/{submission_id}/thread",
            params={"limit": THREAD_PAGE_SIZE, "offset": offset},
        )
        batch = data.get("content", [])
        events.extend(batch)
        result_set = data.get("resultSet", {})
        total = result_set.get("count", len(batch))
        offset += len(batch)
        if offset >= total or not batch:
            break
        time.sleep(REQUEST_DELAY)
    return events


# ─── Parsing ─────────────────────────────────────────────────────────────────

def get_answer(answers: dict, field_name: str) -> str:
    for v in answers.values():
        if v.get("name") == field_name:
            return str(v.get("answer", ""))
    return ""


def parse_unique_id(sub: dict) -> str:
    return get_answer(sub.get("answers", {}), "uniqueId") or sub.get("id", "")


def latest_workflow_instance_id(thread: list[dict]) -> str:
    """
    A submission can be edited and its workflow restarted (RESTART_EDIT),
    which creates a NEW workflowInstanceID but keeps reusing the same
    elementIDs (18, 19, ...) for the new run. If we don't filter to just the
    latest instance, an old run's stale/expired approval events get mixed in
    with the current run's and can overwrite the real status. We find the
    latest instance by taking the workflowInstanceID attached to the last
    event in the thread (thread is chronological).
    """
    for event in reversed(thread):
        wfid = event.get("actionDetails", {}).get("workflowInstanceID")
        if wfid:
            return wfid
    return ""


def filter_to_latest_instance(thread: list[dict]) -> list[dict]:
    latest = latest_workflow_instance_id(thread)
    if not latest:
        return thread
    return [e for e in thread if e.get("actionDetails", {}).get("workflowInstanceID") == latest]


def discover_approval_steps(thread: list[dict]) -> list[str]:
    """
    Finds every distinct Approval-widget elementID present in THIS submission's
    own thread (events whose actionDetails.title == 'Approval'), ordered by
    when each step first appears. Auto-discovering per submission avoids
    hardcoding elementIDs, which silently breaks the moment a submission's
    workflow branches differently or has a different number of approval
    steps than assumed.
    """
    first_seen: dict[str, str] = {}  # elementID -> earliest timestamp
    for event in thread:
        eid = str(event.get("elementID") or "")
        if not eid:
            continue
        details = event.get("actionDetails", {})
        if details.get("title") != "Approval":
            continue
        ts = event.get("timestamp", "")
        if eid not in first_seen or ts < first_seen[eid]:
            first_seen[eid] = ts
    return sorted(first_seen, key=lambda eid: first_seen[eid])


def _initial_recipients(events: list[dict]) -> str:
    """Originally-mailed address(es) for an approval step - handles both
    single-assignee (MAIL) and multi-assignee (MULTIPLE_APPROVAL_MAIL) steps."""
    for e in events:
        if e["actionType"] == "MAIL":
            details = e.get("actionDetails", {})
            if details.get("reason") == "START":
                return details.get("to", "")
        if e["actionType"] == "MULTIPLE_APPROVAL_MAIL":
            details = e.get("actionDetails", {})
            results = details.get("emailResults", [])
            if results:
                return ", ".join(r.get("email", "") for r in results if r.get("email"))
            raw = details.get("assigneeEmails")
            if raw:
                try:
                    import json
                    return ", ".join(json.loads(raw).values())
                except Exception:
                    pass
    return ""


DECISION_ACTION_TYPES = {"APPROVE_REJECT", "MULTIPLE_APPROVE_REJECT", "EXPIRE"}


def parse_one_step(events: list[dict]) -> dict:
    """Returns {email, action_time, status} for a single approval step's events."""
    acting_email = _initial_recipients(events)

    for e in events:
        if e["actionType"] == "REASSIGN":
            acting_email = e.get("actionDetails", {}).get("newAssigneeEmail", acting_email)

    action_time = ""
    status = "Pending"
    decided = False
    for e in events:
        if e["actionType"] in DECISION_ACTION_TYPES:
            details = e.get("actionDetails", {})
            action_time = e.get("timestamp", "")
            acting_email = details.get("assigneeEmail") or acting_email

            if e["actionType"] == "EXPIRE":
                status = "Expired"
                decided = True
                break

            outcome_type = details.get("type", "")
            if outcome_type == "APPROVE":
                status = "Approved"
            elif outcome_type == "REJECT":
                status = "Rejected"
            else:
                # Custom/non-standard outcome - e.g. an "Expire"/timeout
                # button, a designer-added "Escalate"/"Hold" button, or (as
                # seen here) a MULTIPLE_APPROVE_REJECT with no type at all
                # because the whole multi-assignee step timed out
                # (cancelReason: "EXPIRED"). Pull the real label from
                # wherever JotForm put it, in priority order.
                outcome_info = details.get("outcomeInfo", {}) or {}
                cancel_reason = details.get("cancelReason", "")
                status = (
                    details.get("text")
                    or outcome_info.get("text")
                    or outcome_type
                    or (cancel_reason.title() if cancel_reason else "")
                    or f"Unknown (id={details.get('id')})"
                )
            decided = True
            break

    if not decided:
        # No decision was reached - check whether the step actually errored
        # out (e.g. the next approver's email field was empty/invalid, so
        # JotForm couldn't even send the assignment). This is NOT the same
        # as "waiting on someone" - it needs human attention.
        for e in events:
            if e["actionType"] == "FAIL":
                status = "Failed"
                action_time = e.get("timestamp", "")
                break

    return {"email": acting_email, "action_time": action_time, "status": status}


def compute_walk_status(thread: list[dict]) -> dict:
    """
    Filters to the latest workflow instance (in case this submission was
    edited/restarted), auto-discovers its approval steps in chronological
    order, and walks them:
      - First step found Rejected -> "Rejected", date = that step's action_time.
      - First step found with a non-approve/reject CUSTOM outcome (e.g. an
        "Expire"/timeout button) -> that outcome's own label, date = its
        action_time.
      - All discovered steps Approved -> "Approved", date = the LAST step's
        action_time (i.e. the final approval, meaning every approval step
        is done).
      - Otherwise -> "Pending", date blank.
      - No approval steps found in the thread at all -> "Pending", date blank.
    """
    thread = filter_to_latest_instance(thread)

    by_elem: dict[str, list[dict]] = {}
    for event in thread:
        eid = str(event.get("elementID") or "")
        by_elem.setdefault(eid, []).append(event)

    step_order = discover_approval_steps(thread)
    if not step_order:
        return {"status": "Pending", "date": ""}

    last_action_time = ""
    for eid in step_order:
        step = parse_one_step(by_elem.get(eid, []))
        if step["status"] == "Rejected":
            return {"status": "Rejected", "date": step["action_time"]}
        if step["status"] == "Failed":
            return {"status": "Failed", "date": step["action_time"]}
        if step["status"] == "Pending":
            return {"status": "Pending", "date": ""}
        if step["status"] != "Approved":
            # Custom outcome (e.g. Expired/timed out) - report it as-is
            return {"status": step["status"], "date": step["action_time"]}
        last_action_time = step["action_time"]  # Approved -> keep walking

    return {"status": "Approved", "date": last_action_time}


def build_row(sub: dict, thread: list[dict]) -> list:
    unique_id = parse_unique_id(sub)
    submission_date = sub.get("created_at", "")
    raw_status = sub.get("status", "")  # e.g. ACTIVE

    # Approval Status is driven entirely by the step-by-step walk of the
    # thread's actual approval events (compute_walk_status), NOT by the
    # form's own 'overallStatus' field. The Date column is only populated
    # when the walk says every discovered approval step is Approved -
    # i.e. the workflow is fully done - otherwise it stays blank.
    walk = compute_walk_status(thread)
    approval_status = walk["status"]
    date = walk["date"] if walk["status"] == "Approved" else ""

    return [unique_id, submission_date, raw_status, approval_status, date]


def process_submission(sub: dict) -> list:
    thread = fetch_thread(sub.get("id", ""))
    return build_row(sub, thread)


def process_all_submissions(submissions: list[dict]) -> list[list]:
    all_rows = []
    total = len(submissions)
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_submission, sub): sub for sub in submissions}
        for future in as_completed(futures):
            sub = futures[future]
            done += 1
            try:
                all_rows.append(future.result())
            except Exception as exc:
                log.warning("  Skipped %s: %s", sub.get("id", ""), exc)
            if done % 25 == 0 or done == total:
                log.info("Processed %d/%d submissions ...", done, total)
    return all_rows


# ─── Google Sheets helpers ────────────────────────────────────────────────────

def get_or_create_sheet(client: gspread.Client, spreadsheet_id: str) -> gspread.Worksheet:
    ss = client.open_by_key(spreadsheet_id)
    try:
        ws = ss.worksheet(SHEET_NAME)
        log.info("Found existing sheet: '%s'", SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=SHEET_NAME, rows=5000, cols=len(HEADERS) + 2)
        log.info("Created new sheet: '%s'", SHEET_NAME)
    return ws


def setup_headers(ws: gspread.Worksheet):
    if ws.row_values(1) == HEADERS:
        return
    ws.update("A1", [HEADERS])
    sheet_id = ws.id
    ws.spreadsheet.batch_update({"requests": [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": 1,
                    "startColumnIndex": 0, "endColumnIndex": len(HEADERS),
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.122, "green": 0.306, "blue": 0.475},
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                            "fontSize": 10,
                        },
                        "horizontalAlignment": "CENTER",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
    ]})
    log.info("Headers written and formatted.")


def setup_conditional_formatting(ws: gspread.Worksheet):
    """Traffic-light colours on the Approval Status column."""
    sheet_id = ws.id
    col = HEADERS.index("Approval Status")

    rules = []
    for val, r, g, b in [
        ("Approved", 0.714, 0.843, 0.659),
        ("Pending", 1.0, 0.878, 0.698),
        ("Rejected", 0.918, 0.600, 0.600),
        ("Failed", 0.6, 0.2, 0.2),
        ("Expired", 0.7, 0.7, 0.7),
    ]:
        rules.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": [{"userEnteredValue": val}],
                        },
                        "format": {"backgroundColor": {"red": r, "green": g, "blue": b}},
                    },
                },
                "index": 0,
            }
        })

    ws.spreadsheet.batch_update({"requests": rules})
    log.info("Conditional formatting applied.")


def clear_sheet_body(ws: gspread.Worksheet):
    """
    Wipes every row below the header (row 1), leaving the header row and its
    formatting untouched. Used instead of the old upsert-by-Unique-ID logic
    so every run produces a clean, fully-fresh rewrite of the data.
    """
    total_rows = ws.row_count
    if total_rows > 1:
        ws.batch_clear([f"A2:{gspread.utils.rowcol_to_a1(total_rows, len(HEADERS))}"])
    log.info("Cleared existing data rows (kept header).")


def write_all_rows(ws: gspread.Worksheet, rows: list[list]):
    if not rows:
        log.info("No rows to write.")
        return
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    log.info("Wrote %d fresh rows.", len(rows))


# ─── Core sync (shared by CLI + Cloud Function entry points) ─────────────────

def run_sync() -> dict:
    if JOTFORM_API_KEY == "YOUR_JOTFORM_API_KEY":
        raise RuntimeError("Set JOTFORM_API_KEY env var.")
    if JOTFORM_FORM_ID == "YOUR_FORM_ID":
        raise RuntimeError("Set JOTFORM_FORM_ID env var.")
    if GOOGLE_SHEET_ID == "YOUR_GOOGLE_SHEET_ID":
        raise RuntimeError("Set GOOGLE_SHEET_ID env var.")
    if not os.path.exists(GOOGLE_CREDS_FILE):
        raise RuntimeError(f"Creds file not found: {GOOGLE_CREDS_FILE}")

    log.info("Connecting to Google Sheets ...")
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    ws = get_or_create_sheet(client, GOOGLE_SHEET_ID)

    is_new = ws.row_values(1) != HEADERS
    setup_headers(ws)
    if is_new:
        setup_conditional_formatting(ws)

    # Full wipe-and-rewrite: clear every data row, then re-fetch and
    # re-write everything from scratch. No more merge-by-Unique-ID.
    clear_sheet_body(ws)

    submissions = fetch_all_submissions(JOTFORM_FORM_ID)
    log.info("Fetching threads for %d submissions with %d workers ...", len(submissions), MAX_WORKERS)
    all_rows = process_all_submissions(submissions)

    log.info("Writing %d rows ...", len(all_rows))
    write_all_rows(ws, all_rows)

    log.info("Done! Sheet: https://docs.google.com/spreadsheets/d/%s", GOOGLE_SHEET_ID)
    return {"status": "ok", "rows_written": len(all_rows)}


# ─── Cloud Function HTTP entry point ─────────────────────────────────────────
# Deploy with: gcloud functions deploy syncJotformToSheet --entry-point sync_http ...
# (see deployment steps provided alongside this file)

def sync_http(request):
    """HTTP-triggered Cloud Function entry point (functions-framework compatible)."""
    try:
        result = run_sync()
        return (result, 200)
    except Exception as exc:
        log.exception("Sync failed")
        return ({"status": "error", "message": str(exc)}, 500)


# ─── CLI entry point ──────────────────────────────────────────────────────────

def main():
    result = run_sync()
    print(result)


if __name__ == "__main__":
    main()