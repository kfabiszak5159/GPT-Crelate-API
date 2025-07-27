from fastapi import FastAPI, Query, Body
import httpx
import os
from fastapi.responses import JSONResponse
from datetime import datetime
from fastapi.staticfiles import StaticFiles
import pandas as pd

app = FastAPI()

API_KEY = os.getenv("CRELATE_API_KEY") or "your_default_api_key_here"
BASE_URL = "https://app.crelate.com/api3"

EXCEL_CONTACTS_PATH = "API Contacts.xlsx"
try:
    local_contacts_df = pd.read_excel(EXCEL_CONTACTS_PATH)
    local_contacts_df.columns = local_contacts_df.columns.str.strip().str.lower()
except Exception:
    local_contacts_df = pd.DataFrame()

def lookup_local_contact(full_name: str):
    if local_contacts_df.empty:
        return None
    match = local_contacts_df[local_contacts_df["full name"].str.lower() == full_name.strip().lower()]
    if not match.empty:
        return match.iloc[0].get("id")
    return None

def filter_local_contacts(**filters):
    if local_contacts_df.empty:
        return []

    df = local_contacts_df.copy()

    def lower_eq(column, value):
        return df[column].str.lower() == value.lower()

    def lower_contains(column, value):
        return df[column].str.lower().str.contains(value.lower(), na=False)

    if filters.get("full_name"):
        df = df[lower_eq("full name", filters["full_name"])]
    if filters.get("created_by") and "created by" in df.columns:
        df = df[lower_eq("created by", filters["created_by"])]
    if filters.get("owner") and "owner" in df.columns:
        df = df[lower_eq("owner", filters["owner"])]
    if filters.get("primary_owner") and "primary owner" in df.columns:
        df = df[lower_eq("primary owner", filters["primary_owner"])]
    if filters.get("tag") and "tags" in df.columns:
        df = df[lower_contains("tags", filters["tag"])]
    if filters.get("description") and "description" in df.columns:
        df = df[lower_contains("description", filters["description"])]
    if filters.get("last_activity_date") and "last activity date" in df.columns:
        df = df[lower_contains("last activity date", filters["last_activity_date"])]
    if filters.get("last_activity_regarding") and "last activity regarding" in df.columns:
        df = df[lower_contains("last activity regarding", filters["last_activity_regarding"])]

    return df.to_dict(orient="records")

async def fetch_crelate_data(path: str, params: dict = {}):
    url = f"{BASE_URL}/{path}"
    params["api_key"] = API_KEY
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        if response.status_code != 200:
            return {
                "requested_url": str(response.url),
                "status_code": response.status_code,
                "error": response.text
            }
        try:
            return response.json()
        except Exception as e:
            return {
                "requested_url": str(response.url),
                "status_code": response.status_code,
                "error": f"Failed to parse JSON: {str(e)}",
                "raw_text": response.text
            }

def extract_display_fields(contact):
    return {
        "FullName": contact.get("FullName"),
        "Description": contact.get("Description"),
        "Email_Work": contact.get("EmailAddresses_Work", {}).get("Value"),
        "Email_Personal": contact.get("EmailAddresses_Personal", {}).get("Value"),
        "Phone": contact.get("PhoneNumbers_Work_Main", {}).get("Value") or contact.get("PhoneNumbers_Mobile", {}).get("Value"),
        "LastActivityDate": contact.get("LastActivityDate"),
        "LastActivityRegarding": contact.get("LastActivityRegardingId", {}).get("Title"),
        "Tags": contact.get("Tags")
    }

def matches_filters(contact, **filters):
    def safe_lower(val):
        return val.lower() if isinstance(val, str) else ""

    for key, value in filters.items():
        if not value:
            continue

        if key == "full_name" and safe_lower(contact.get("FullName", "")) != value.lower():
            return False
        if key == "created_by":
            creator = contact.get("CreatedById") or {}
            if safe_lower(creator.get("Title", "")) != value.lower():
                return False
        if key == "owner":
            owners = contact.get("Owners") or []
            if not any(safe_lower(o.get("Title", "")) == value.lower() for o in owners if isinstance(o, dict)):
                return False
        if key == "primary_owner":
            owners = contact.get("Owners") or []
            primary = next((o for o in owners if o.get("IsPrimary")), {})
            if safe_lower(primary.get("Title", "")) != value.lower():
                return False
        if key == "tag":
            tags_dict = contact.get("Tags") or {}
            match = any(
                value.lower() in (t.get("Title", "").lower() or "")
                for taglist in tags_dict.values()
                if isinstance(taglist, list)
                for t in taglist
            )
            if not match:
                return False
        if key == "description" and safe_lower(contact.get("Description", "")).find(value.lower()) == -1:
            return False
        if key == "last_activity_date" and safe_lower(str(contact.get("LastActivityDate", ""))).find(value.lower()) == -1:
            return False
        if key == "last_activity_regarding":
            regarding = contact.get("LastActivityRegardingId", {})
            if not isinstance(regarding, dict) or safe_lower(regarding.get("Title", "")) != value.lower():
                return False
    return True

@app.get("/contacts")
async def get_contacts(
    limit: int = Query(100, ge=1, le=100),
    offset: int = 0,
    full_name: str = None,
    tag: str = None,
    created_by: str = None,
    owner: str = None,
    primary_owner: str = None,
    description: str = None,
    last_activity_date: str = None,
    last_activity_regarding: str = None
):
    try:
        params = {"limit": limit, "offset": offset}
        raw_data = await fetch_crelate_data("contacts", params)
        if not raw_data or "Data" not in raw_data:
            raise ValueError("Invalid response from Crelate API")

        filtered = [
            extract_display_fields(c)
            for c in raw_data["Data"]
            if matches_filters(
                c,
                full_name=full_name,
                tag=tag,
                created_by=created_by,
                owner=owner,
                primary_owner=primary_owner,
                description=description,
                last_activity_date=last_activity_date,
                last_activity_regarding=last_activity_regarding
            )
        ]

        if not filtered:
            filtered = filter_local_contacts(
                full_name=full_name,
                tag=tag,
                created_by=created_by,
                owner=owner,
                primary_owner=primary_owner,
                description=description,
                last_activity_date=last_activity_date,
                last_activity_regarding=last_activity_regarding
            )

        return {"records": filtered}

    except Exception as e:
        return {"error": "Exception caught in get_contacts", "detail": str(e)}

@app.post("/post_screen_activity")
async def post_screen_activity(payload: dict = Body(...)):
    try:
        contact_id = payload.get("EntityId")
        notes = payload.get("Notes")
        if not contact_id or not notes:
            return JSONResponse(status_code=400, content={"error": "Missing required EntityId or Notes"})

        current_time = datetime.utcnow().isoformat() + "Z"

        activity_payload = {
            "entity": {
                "ParentId": {
                    "Id": contact_id,
                    "EntityName": "Contacts"
                },
                "VerbId": {
                    "Id": "2d4edbf9-a7a2-4174-ae53-a8f900bb0381",
                    "Title": "Screen"
                },
                "Subject": "Screen via API",
                "Html": notes,
                "IsEngagement": True,
                "Completed": True,
                "When": current_time
            }
        }

        url = f"{BASE_URL}/activities"
        headers = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}

        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=activity_payload, headers=headers)
            if response.status_code != 200:
                return {
                    "error": "Failed to post activity",
                    "status_code": response.status_code,
                    "response": response.text
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
            return JSONResponse(status_code=400, content={"error": "Missing required FullName or Notes"})

        params = {"limit": 100, "offset": 0}
        raw_data = await fetch_crelate_data("contacts", params)

        contact_id = None
        if raw_data and "Data" in raw_data:
            match = next((c for c in raw_data["Data"] if c.get("FullName", "").lower() == full_name.lower()), None)
            if match:
                contact_id = match.get("Id")

        if not contact_id:
            contact_id = lookup_local_contact(full_name)

        if not contact_id:
            return JSONResponse(status_code=404, content={"error": f"No contact found with full name '{full_name}'"})

        return await post_screen_activity({"EntityId": contact_id, "Notes": notes})

    except Exception as e:
        return {"error": "Exception occurred while posting by name", "detail": str(e)}

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

app.mount("/.well-known", StaticFiles(directory=".well-known"), name="well-known")
