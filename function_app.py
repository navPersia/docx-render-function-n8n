import json
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.parse import urlparse

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from docxtpl import DocxTemplate, InlineImage
from docx.shared import Mm

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def sanitize_filename(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "report"


def to_bullets(items, empty_fallback="Not evidenced in provided input.") -> str:
    if not items:
        return empty_fallback
    cleaned = [f"• {item.strip()}" for item in items if isinstance(item, str) and item.strip()]
    return "\n".join(cleaned) if cleaned else empty_fallback


def safe_get(d, *keys, default=""):
    current = d
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def download_file(url: str, timeout: int = 30) -> bytes:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def upload_bytes_to_blob(
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
    data: bytes,
    content_type: str,
) -> str:
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True, content_type=content_type)
    return blob_client.url


def generate_read_sas_url(
    account_name: str,
    account_key: str,
    container_name: str,
    blob_name: str,
) -> str:
    sas = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(days=7),
    )
    return f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas}"


def build_context(payload: dict, doc: DocxTemplate, chart_path: str | None) -> dict:
    source_url = payload.get("source_url", "")
    summary = payload.get("assessment_summary", {}) or {}
    meta = payload.get("meta", {}) or {}

    client_name = meta.get("client_name") or "Client name"
    consultant_name = meta.get("consultant_name") or "Consultant name"
    version = meta.get("version") or "0.1"
    issue_date = meta.get("issue_date") or datetime.utcnow().strftime("%Y-%m-%d")

    chart_image = InlineImage(doc, chart_path, width=Mm(120)) if chart_path else None

    return {
        "ClientName": client_name,
        "IssueDate": issue_date,
        "ConsultantName": consultant_name,
        "Version": version,
        "SourceUrl": source_url,

        "ExecutiveSummary": safe_get(summary, "executive_summary", "summary", default=""),
        "OverallReading": safe_get(summary, "executive_summary", "overall_reading", default=""),

        "ChartImage": chart_image,
        "WheelSummary": safe_get(summary, "wheel_interpretation", "summary", default=""),
        "WheelBalanceObservations": to_bullets(
            safe_get(summary, "wheel_interpretation", "balance_observations", default=[])
        ),
        "WheelImbalanceObservations": to_bullets(
            safe_get(summary, "wheel_interpretation", "imbalance_observations", default=[])
        ),
        "WheelHowToUse": safe_get(summary, "wheel_interpretation", "how_to_use", default=""),

        "ObservableStrengths": to_bullets(
            safe_get(summary, "key_insights", "observable_strengths", default=[])
        ),
        "UnderdevelopedAreas": to_bullets(
            safe_get(summary, "key_insights", "underdeveloped_or_uncertain_areas", default=[])
        ),

        "MissingInformation": to_bullets(
            safe_get(summary, "unknowns", "missing_information", default=[])
        ),
        "FollowUpQuestions": to_bullets(
            safe_get(summary, "unknowns", "follow_up_questions", default=[])
        ),

        "DiscussionPoints": to_bullets(
            safe_get(summary, "next_steps", "discussion_points", default=[])
        ),
        "DiscoveryActions": to_bullets(
            safe_get(summary, "next_steps", "discovery_actions", default=[])
        ),
    }


@app.route(route="render-docx", methods=["POST"])
def render_docx(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Request body must be valid JSON"}),
            status_code=400,
            mimetype="application/json",
        )

    template_url = payload.get("template_url")
    chart_url = payload.get("chart_url")
    meta = payload.get("meta", {}) or {}

    if not template_url:
        return func.HttpResponse(
            json.dumps({"error": "template_url is required"}),
            status_code=400,
            mimetype="application/json",
        )

    output_container = os.getenv("OUTPUT_CONTAINER", "generated-reports")
    storage_connection_string = os.getenv("AzureWebJobsStorage")
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
    storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")

    if not storage_connection_string:
        return func.HttpResponse(
            json.dumps({"error": "AzureWebJobsStorage setting is missing"}),
            status_code=500,
            mimetype="application/json",
        )

    if not storage_account_name or not storage_account_key:
        return func.HttpResponse(
            json.dumps({"error": "STORAGE_ACCOUNT_NAME and STORAGE_ACCOUNT_KEY settings are required"}),
            status_code=500,
            mimetype="application/json",
        )

    client_name = meta.get("client_name") or "client"
    safe_name = sanitize_filename(client_name)
    output_blob_name = f"{safe_name}-bpr.docx"

    try:
        template_bytes = download_file(template_url)
        chart_bytes = download_file(chart_url) if chart_url else None

        with tempfile.TemporaryDirectory() as tmpdir:
            template_path = os.path.join(tmpdir, "template.docx")
            chart_path = os.path.join(tmpdir, "chart.png") if chart_bytes else None
            output_path = os.path.join(tmpdir, output_blob_name)

            with open(template_path, "wb") as f:
                f.write(template_bytes)

            if chart_bytes and chart_path:
                with open(chart_path, "wb") as f:
                    f.write(chart_bytes)

            doc = DocxTemplate(template_path)
            context = build_context(payload, doc, chart_path)
            doc.render(context)
            doc.save(output_path)

            with open(output_path, "rb") as f:
                output_bytes = f.read()

        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

        # Ensure container exists
        container_client = blob_service_client.get_container_client(output_container)
        try:
            container_client.create_container()
        except Exception:
            pass

        blob_url = upload_bytes_to_blob(
            blob_service_client=blob_service_client,
            container_name=output_container,
            blob_name=output_blob_name,
            data=output_bytes,
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

        signed_url = generate_read_sas_url(
            account_name=storage_account_name,
            account_key=storage_account_key,
            container_name=output_container,
            blob_name=output_blob_name,
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "file_name": output_blob_name,
                    "blob_url": blob_url,
                    "download_url": signed_url,
                    "source_url": payload.get("source_url", ""),
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except requests.RequestException as exc:
        return func.HttpResponse(
            json.dumps({"error": f"Failed to download template or chart: {str(exc)}"}),
            status_code=502,
            mimetype="application/json",
        )
    except Exception as exc:
        return func.HttpResponse(
            json.dumps({"error": f"Render failed: {str(exc)}"}),
            status_code=500,
            mimetype="application/json",
        )
