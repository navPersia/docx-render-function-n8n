import json
import os
import re
import tempfile
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import azure.functions as func

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


@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"status": "ok"}),
        status_code=200,
        mimetype="application/json",
    )


@app.route(route="render-docx", methods=["POST"])
def render_docx(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Lazy imports so function discovery does not fail at startup
        try:
            from azure.storage.blob import BlobServiceClient
            from docxtpl import DocxTemplate, InlineImage
            from docx.shared import Mm
        except ImportError as exc:
            return func.HttpResponse(
                json.dumps(
                    {
                        "error": (
                            f"Missing runtime dependency: {exc}. "
                            "Ensure requirements are installed in the deployed package."
                        )
                    }
                ),
                status_code=500,
                mimetype="application/json",
            )

        def download_file(url: str, timeout: int = 30) -> bytes:
            try:
                with urlopen(url, timeout=timeout) as resp:
                    return resp.read()
            except HTTPError as exc:
                raise RuntimeError(f"Failed to download {url}: HTTP {exc.code}") from exc
            except URLError as exc:
                raise RuntimeError(f"Failed to download {url}: {exc.reason}") from exc

        def upload_bytes_to_blob(
            blob_service_client: BlobServiceClient,
            container_name: str,
            blob_name: str,
            data: bytes,
            content_type: str,
        ) -> str:
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name,
            )
            blob_client.upload_blob(data, overwrite=True, content_type=content_type)
            return blob_client.url

        def build_context(payload: dict, doc, chart_path: str = None) -> dict:
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
        output_container = payload.get("output_container") or os.getenv("OUTPUT_CONTAINER", "n8n-assesment")

        if not template_url:
            return func.HttpResponse(
                json.dumps({"error": "template_url is required"}),
                status_code=400,
                mimetype="application/json",
            )

        storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        storage_account_url = os.getenv("STORAGE_ACCOUNT_URL")
        if not storage_account_url:
            storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
            if storage_account_name:
                storage_account_url = f"https://{storage_account_name}.blob.core.windows.net"

        client_name = meta.get("client_name") or "client"
        output_blob_name = f"{sanitize_filename(client_name)}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.docx"

        template_bytes = download_file(template_url)
        chart_bytes = download_file(chart_url) if chart_url else None

        with tempfile.TemporaryDirectory() as tmpdir:
            template_path = os.path.join(tmpdir, "template.docx")
            output_path = os.path.join(tmpdir, output_blob_name)
            chart_path = os.path.join(tmpdir, "chart.png") if chart_bytes else None

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

        if storage_connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        else:
            if not storage_account_url:
                return func.HttpResponse(
                    json.dumps(
                        {
                            "error": (
                                "Set AZURE_STORAGE_CONNECTION_STRING, or set STORAGE_ACCOUNT_URL/"
                                "STORAGE_ACCOUNT_NAME and deploy with azure-identity installed."
                            )
                        }
                    ),
                    status_code=500,
                    mimetype="application/json",
                )

            try:
                from azure.identity import DefaultAzureCredential
            except ImportError:
                return func.HttpResponse(
                    json.dumps(
                        {
                            "error": (
                                "Managed identity auth requires azure-identity. "
                                "Install it in deployment or set AZURE_STORAGE_CONNECTION_STRING."
                            )
                        }
                    ),
                    status_code=500,
                    mimetype="application/json",
                )

            blob_service_client = BlobServiceClient(
                account_url=storage_account_url,
                credential=DefaultAzureCredential(),
            )
        blob_url = upload_bytes_to_blob(
            blob_service_client=blob_service_client,
            container_name=output_container,
            blob_name=output_blob_name,
            data=output_bytes,
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "file_name": output_blob_name,
                    "container": output_container,
                    "blob_url": blob_url,
                    "source_url": payload.get("source_url", ""),
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as exc:
        return func.HttpResponse(
            json.dumps({"error": f"Render failed: {str(exc)}"}),
            status_code=500,
            mimetype="application/json",
        )
