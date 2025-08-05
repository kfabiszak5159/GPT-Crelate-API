from fastapi import FastAPI, Query, Body
import httpx
import os
from fastapi.responses import JSONResponse
from datetime import datetime
from fastapi.staticfiles import StaticFiles
import pandas as pd

app = FastAPI()
API_KEY = os.getenv("CRELATE_API_KEY") or "46gcq4k7bw9yysb9thazasxxwy"
BASE_URL = "https://app.crelate.com/api3"

# Load local contact fallback database
EXCEL_CONTACTS_PATH = "API Contacts.xlsx"
try:
    local_contacts_df = pd.read_excel(EXCEL_CONTACTS_PATH)
    local_contacts_df.columns = local_contacts_df.columns.str.strip()
except Exception:
    local_contacts_df = pd.DataFrame()


def lookup_local_contact(full_name: str):
    if local_contacts_df.empty:
        return None
    match = local_contacts_df[
        local_contacts_df["Full Name"].str.lower() == full_name.strip().lower()
    ]
    if not match.empty:
        return match.iloc[0]["Id"]
    return None


def filter_local_contacts(
    full_name=None, tag=None, created_by=None, owner=None, primary_owner=None
):
    if local_contacts_df.empty:
        return []
    df = local_contacts_df.copy()

    def safe_filter(col, val, contains=False):
        if col not in df.columns:
            return df
        series = df[col].astype(str).str.lower()
        val = val.lower()
        return df[series.str.contains(val, na=False)] if contains else df[series == val]

    if full_name:
        df = safe_filter("Full Name", full_name)
    if created_by:
        df = safe_filter("Created By", created_by)
    if owner:
        df = safe_filter("Owner", owner)
    if primary_owner:
        df = safe_filter("Primary Owner", primary_owner)
    if tag:
        df = safe_filter("Tags", tag, contains=True)

    return df.fillna("").to_dict(orient="records")


async def fetch_crelate_data(path: str, params: dict = {}):
    url = f"{BASE_URL}/{path}"
    params["api_key"] = API_KEY
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
    if response.status_code != 200:
        return {
            "requested_url": str(response.url),
            "status_code": response.status_code,
            "error": response.text,
        }
    try:
        return response.json()
    except Exception as e:
        return {
            "requested_url": str(response.url),
            "status_code": response.status_code,
            "error": f"Failed to parse JSON: {str(e)}",
            "raw_text": response.text,
        }


def safe_get(d, *keys):
    for key in keys:
        if d is None:
            return ""
        d = d.get(key)
    return d or ""


def normalize_name(name: str):
    return " ".join(name.lower().replace(",", "").split())


async def fetch_filtered_contacts(
    limit=100,
    offset=0,
    full_name=None,
    tag=None,
    created_by=None,
    owner=None,
    primary_owner=None,
    debug=False,
):
    params = {"limit": limit, "offset": offset}

    # -- split full_name into first_name & last_name for server‐side filter
    if full_name:
        parts = full_name.strip().split()
        params["first_name"] = parts[0]
        if len(parts) > 1:
            params["last_name"] = " ".join(parts[1:])

    if tag:
        params["tag_names"] = tag

    if created_by:
        params["created_by"] = created_by

    if owner:
        params["owner"] = owner

    if primary_owner:
        params["primary_owner"] = primary_owner

    raw_data = await fetch_crelate_data("contacts", params)
    if debug:
        print(f"[fetch_filtered_contacts] params={params} raw_data={raw_data}")

    if not raw_data or not isinstance(raw_data, dict):
        return []

    contacts = raw_data.get("Data", []) or []
    target = normalize_name(full_name) if full_name else None

    def matches_filters(contact):
        if not isinstance(contact, dict):
            return False

        if target:
            contact_name = normalize_name(contact.get("Name", "") or "")
            reversed_contact = " ".join(reversed(contact_name.split()))
            if target not in contact_name and target not in reversed_contact:
                return False

        if created_by:
            creator = contact.get("CreatedById") or {}
            if (creator.get("Title") or "").strip().lower() != created_by.strip().lower():
                return False

        if owner:
            owners = contact.get("Owners") or []
            if not any(
                (o.get("Title") or "").strip().lower() == owner.strip().lower()
                for o in owners
                if isinstance(o, dict)
            ):
                return False

        if primary_owner:
            owners = contact.get("Owners") or []
            primary = next(
                (o for o in owners if o.get("IsPrimary") and isinstance(o, dict)), None
            )
            if not primary or (
                primary.get("Title") or ""
            ).strip().lower() != primary_owner.strip().lower():
                return False

        if tag:
            tags_dict = contact.get("Tags") or {}
            match = False
            for tag_list in tags_dict.values():
                if isinstance(tag_list, list) and any(
                    (t.get("Title") or "").strip().lower() == tag.strip().lower()
                    for t in tag_list
                    if isinstance(t, dict)
                ):
                    match = True
                    break
            if not match:
                return False

        return True

    results = []
    for c in contacts:
        if matches_filters(c):
            results.append(
                {
                    "Id": c.get("Id", ""),
                    "FullName": c.get("Name", ""),
                    "CreatedBy": safe_get(c.get("CreatedById"), "Title"),
                    "PrimaryOwner": next(
                        (o.get("Title") for o in c.get("Owners", []) if o.get("IsPrimary")),
                        "",
                    ),
                    "Tags": [
                        t.get("Title")
                        for v in (c.get("Tags") or {}).values()
                        for t in (v if isinstance(v, list) else [])
                        if isinstance(t, dict) and t.get("Title")
                    ],
                    "Location": safe_get(c.get("Addresses_Home"), "Value")
                    or safe_get(c.get("Addresses_Business"), "Value"),
                    "Email_Work": safe_get(c.get("EmailAddresses_Work"), "Value"),
                    "Email_Personal": safe_get(
                        c.get("EmailAddresses_Personal"), "Value"
                    ),
                    "Phone_Work": safe_get(c.get("PhoneNumbers_Work_Main"), "Value"),
                    "Phone_Mobile": safe_get(c.get("PhoneNumbers_Mobile"), "Value"),
                    "LastActivityDate": c.get("LastActivityDate", ""),
                    "LastActivityRegarding": safe_get(
                        c.get("LastActivityRegardingId"), "Title"
                    ),
                    "Description": c.get("Description", ""),
                }
            )

    return results


@app.get("/contacts")
async def get_contacts(
    limit: int = Query(100, ge=1, le=100),
    offset: int = 0,
    full_name: str = None,
    tag: str = None,
    created_by: str = None,
    owner: str = None,
    primary_owner: str = None,
    debug: bool = False,
):
    try:
        filtered = await fetch_filtered_contacts(
            limit, offset, full_name, tag, created_by, owner, primary_owner, debug
        )
        if filtered:
            return {"records": filtered}

        fallback = filter_local_contacts(
            full_name, tag, created_by, owner, primary_owner
        )
        return {"records": fallback}

    except Exception as e:
        return {"error": "Exception caught in get_contacts", "detail": str(e)}


@app.post("/post_screen_activity")
async def post_screen_activity(payload: dict = Body(...)):
    try:
        contact_id = payload.get("EntityId")
        notes = payload.get("Notes")
        if not contact_id or not notes:
            return JSONResponse(
                status_code=400, content={"error": "Missing required EntityId or Notes"}
            )

        current_time = datetime.utcnow().isoformat() + "Z"
        activity_payload = {
            "entity": {
                "ParentId": {"Id": contact_id, "EntityName": "Contacts"},
                "VerbId": {
                    "Id": "2d4edbf9-a7a2-4174-ae53-a8f900bb0381",
                    "Title": "Screen",
                },
                "Subject": "Screen via API",
                "Html": notes,
                "IsEngagement": True,
                "Completed": True,
                "When": current_time,
            }
        }

        url = f"{BASE_URL}/activities"
        headers = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, json=activity_payload, headers=headers
            )
        if response.status_code != 200:
            return {
                "error": "Failed to post activity",
                "status_code": response.status_code,
                "response": response.text,
            }

        return {"success": True, "response": response.json()}

    except Exception as e:
        return {"error": "Exception occurred while posting activity", "detail": str(e)}


@app.post("/post_screen_activity_by_name")
async def post_screen_activity_by_name(payload: dict = Body(...)):
    try:
        full_name = payload.get("FullName")
        notes = payload.get("Notes")
        if not full_name or not notes:
            return JSONResponse(
                status_code=400, content={"error": "Missing required FullName or Notes"}
            )

        contact_list = await fetch_filtered_contacts(full_name=full_name)
        contact_id = contact_list[0].get("Id") if contact_list else lookup_local_contact(full_name)

        if not contact_id:
            return JSONResponse(
                status_code=404,
                content={"error": f"No contact found with full name '{full_name}'"},
            )

        return await post_screen_activity({"EntityId": contact_id, "Notes": notes})

    except Exception as e:
        return {"error": "Exception occurred while posting by name", "detail": str(e)}


 @app.get("/test-contacts-filter")
 async def test_contacts_filter(
     tag_names: str = Query(None, alias="tag_names"),
     full_name: str = Query(None, alias="full_name"),
+    ids: str = Query(None, alias="ids"),
 ):
     try:
         params = {"api_key": API_KEY}

         if tag_names:
             params["tag_names"] = tag_names

+        if ids:
+            # pass comma-separated list of UUIDs through to server-side filter
+            params["ids"] = ids

         if full_name:
             parts = full_name.strip().split()
             params["first_name"] = parts[0]
             if len(parts) > 1:
                 params["last_name"] = " ".join(parts[1:])

         url = f"{BASE_URL}/contacts"
         async with httpx.AsyncClient() as client:
             response = await client.get(url, params=params)

         status = response.status_code
         url_str = str(response.url)
         try:
             parsed = response.json()
         except Exception:
             parsed = response.text

         return {"status": status, "url": url_str, "response": parsed}

     except Exception as e:
         return {"error": "Exception occurred in /test-contacts-filter", "detail": str(e)}




@app.get("/test-jobs-filter")
async def test_jobs_filter(
    tag_names: str = Query(None, description="Filter jobs by tag"),
    name: str = Query(None, description="Filter jobs by job name/title"),
    limit: int = 100,
):
    try:
        params = {"limit": limit, "api_key": API_KEY}
        if tag_names:
            params["tag_names"] = tag_names
        if name:
            params["name"] = name

        url = f"{BASE_URL}/jobs"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)

        return {
            "status": response.status_code,
            "url": str(response.url),
            "response": response.json() if response.status_code == 200 else response.text,
        }
    except Exception as e:
        return {"error": "Exception in /test-jobs-filter", "detail": str(e)}


@app.get("/contacts/id/{contact_id}/artifacts")
async def get_contact_artifacts_by_id(contact_id: str):
    try:
        headers = {"X-Api-Key": API_KEY}
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{BASE_URL}/entities/{contact_id}/artifacts", headers=headers)
            if response.status_code != 200:
                return {
                    "error": "Failed to retrieve artifacts",
                    "status_code": response.status_code,
                    "response": response.text
                }
            data = response.json()

        return {"artifacts": data.get("Data", []), "total": data.get("Metadata", {}).get("TotalRecords")}

    except Exception as e:
        return {"error": "Exception retrieving contact artifacts", "detail": str(e)}

# Helper to fetch raw contacts from Crelate with minimal server-side filtering (like test-contacts-filter)
async def fetch_raw_contacts_from_crelate(
    limit=100,
    offset=0,
    full_name=None,
    tag=None,
    created_by=None,
    owner=None,
    primary_owner=None,
):
    params = {"limit": limit, "offset": offset}
    if full_name:
        parts = full_name.strip().split()
        params["first_name"] = parts[0]
        if len(parts) > 1:
            params["last_name"] = " ".join(parts[1:])
    if tag:
        params["tag_names"] = tag
    if created_by:
        params["created_by"] = created_by
    if owner:
        params["owner"] = owner
    if primary_owner:
        params["primary_owner"] = primary_owner

    # Use header for API key (more consistent than query param)
    url = f"{BASE_URL}/contacts"
    headers = {"X-Api-Key": API_KEY}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, headers=headers)
    if response.status_code != 200:
        return {
            "requested_url": str(response.url),
            "status_code": response.status_code,
            "error": response.text,
        }
    try:
        return response.json()
    except Exception as e:
        return {
            "requested_url": str(response.url),
            "status_code": response.status_code,
            "error": f"Failed to parse JSON: {str(e)}",
            "raw_text": response.text,
        }



app.mount("/.well-known", StaticFiles(directory=".well-known"), name="well-known")
