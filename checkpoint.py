from fastapi import FastAPI, Query
import httpx
import os

app = FastAPI()

API_KEY = os.getenv("CRELATE_API_KEY") or "46gcq4k7bw9yysb9thazasxxwy"
BASE_URL = "https://app.crelate.com/api3"

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
        return response.json()

@app.get("/jobs")
async def get_jobs(
    limit: int = Query(100, ge=1, le=100),
    offset: int = 0,
    tag: str = None,
    created_by: str = None,
    owner: str = None,
    job_type: str = None,
    primary_owner: str = None
):
    try:
        params = {"limit": limit, "offset": offset}
        if tag:
            params["tag"] = tag

        raw_data = await fetch_crelate_data("jobs", params)

        if not raw_data or not isinstance(raw_data, dict):
            return {"error": "Unexpected API response format", "response": raw_data}

        jobs = raw_data.get("Data")
        if jobs is None:
            return {"error": "Missing 'Data' key in API response", "response": raw_data}

        def matches_filters(job):
            try:
                if created_by:
                    creator = job.get("CreatedById") or {}
                    if creator.get("Title", "").lower() != created_by.lower():
                        return False

                if job_type:
                    job_types = job.get("JobTypeIds") or []
                    if not any(jt and jt.get("Title", "").lower() == job_type.lower() for jt in job_types):
                        return False

                if owner:
                    owners = job.get("Owners") or []
                    if not any(o and o.get("Title", "").lower() == owner.lower() for o in owners):
                        return False

                if primary_owner:
                    owners = job.get("Owners") or []
                    primary = next((o for o in owners if o and o.get("IsPrimary")), None)
                    if not primary or primary.get("Title", "").lower() != primary_owner.lower():
                        return False

                return True
            except Exception as e:
                raise RuntimeError(f"Error filtering job {job.get('Id', 'unknown')}: {e}")

        filtered_jobs = [job for job in jobs if matches_filters(job)]

        display_jobs = []
        for job in filtered_jobs:
            account = job.get("AccountId") or {}
            title = job.get("JobTitleId") or {}
            owners = job.get("Owners") or []
            primary = next((o for o in owners if o and o.get("IsPrimary")), None)
            display_jobs.append({
                "company": account.get("Title"),
                "job_title": title.get("Title"),
                "primary_owner": primary.get("Title") if primary else None
            })

        return {"records": display_jobs}

    except Exception as e:
        return {"error": "Exception caught in get_jobs", "detail": str(e)}

# Example: returns all jobs with job type "contract"
# http://127.0.0.1:8000/jobs?job_type=contract
