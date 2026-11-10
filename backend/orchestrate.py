"""watsonx Orchestrate client: IAM token exchange, agent lookup and chat.

Credentials come from the git-ignored service-credentials JSON file (keys `apikey`, `url`).
The API key never leaves the server.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

from .config import ROOT

IAM_URL = "https://iam.cloud.ibm.com/identity/token"
AGENT_NAME = os.environ.get("ORCHESTRATE_AGENT_NAME", "banking_rm_assistant")


class OrchestrateError(RuntimeError):
    pass


_token: Dict[str, Any] = {"value": None, "expires": 0.0}
_agent_id: Optional[str] = None


def _credentials() -> Dict[str, str]:
    path = ROOT / os.environ.get("ORCHESTRATE_CREDENTIALS_FILE", "ibm-credentials (3).env")
    if not path.exists():
        raise OrchestrateError("Orchestrate credentials file not found")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {"apikey": data["apikey"], "url": data["url"].rstrip("/")}
    except (ValueError, KeyError):
        raise OrchestrateError("Orchestrate credentials file must be service-credentials JSON with apikey and url")


def _http(url: str, method: str = "GET", headers: Optional[Dict[str, str]] = None,
          body: Any = None, form: Optional[Dict[str, str]] = None, timeout: float = 90.0) -> Tuple[int, Any]:
    headers = dict(headers or {})
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        try:
            return exc.code, json.loads(raw)
        except ValueError:
            return exc.code, {"raw": raw[:500]}
    except (urllib.error.URLError, OSError) as exc:
        raise OrchestrateError("Orchestrate unreachable: %s" % exc)


def _get_token(creds: Dict[str, str]) -> str:
    if _token["value"] and time.time() < _token["expires"] - 60:
        return _token["value"]
    status, payload = _http(IAM_URL, "POST", form={
        "grant_type": "urn:ibm:params:oauth:grant-type:apikey", "apikey": creds["apikey"]})
    if status != 200 or "access_token" not in payload:
        raise OrchestrateError("IAM token request failed (HTTP %s)" % status)
    _token["value"] = payload["access_token"]
    _token["expires"] = time.time() + int(payload.get("expires_in", 3600))
    return _token["value"]


def _call(path: str, method: str = "GET", body: Any = None) -> Tuple[int, Any]:
    creds = _credentials()
    return _http(creds["url"] + path, method, {"Authorization": "Bearer " + _get_token(creds)}, body)


def _find_agent_id() -> str:
    global _agent_id
    if _agent_id:
        return _agent_id
    status, payload = _call("/v1/orchestrate/agents")
    if status != 200:
        raise OrchestrateError("Listing agents failed (HTTP %s)" % status)
    agents = payload if isinstance(payload, list) else payload.get("agents", [])
    for agent in agents:
        if agent.get("name") == AGENT_NAME:
            _agent_id = agent["id"]
            return _agent_id
    raise OrchestrateError("Agent %s not found in the Orchestrate instance" % AGENT_NAME)


def status() -> Dict[str, Any]:
    try:
        agent_id = _find_agent_id()
        return {"ok": True, "agent": AGENT_NAME, "agent_id": agent_id}
    except OrchestrateError as exc:
        return {"ok": False, "error": str(exc)}


def chat(message: str) -> str:
    agent_id = _find_agent_id()
    code, payload = _call("/v1/orchestrate/%s/chat/completions" % urllib.parse.quote(agent_id, safe=""),
                          "POST", {"messages": [{"role": "user", "content": message}], "stream": False})
    try:
        text = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        text = None
    if code != 200 or not text:
        raise OrchestrateError("Unexpected Orchestrate response (HTTP %s)" % code)
    return text


def build_prompt(state: Dict[str, Any]) -> str:
    """Turn a customer-state record into the agent prompt. Field names are spelled out to avoid misreads."""
    fields = {
        "customer_name": state.get("name"),
        "segment": state.get("segment"),
        "risk_level_from_streaming_rules": state.get("customer_risk"),
        "risk_reason": state.get("risk_reason"),
        "opening_savings_balance_idr": state.get("opening_savings_balance"),
        "current_savings_balance_idr": state.get("current_balance"),
        "average_balance_idr": state.get("avg_balance"),
        "external_transfers_out_last_24_hours_idr": state.get("external_transfer_24h"),
        "normal_external_transfers_idr": state.get("normal_external_transfer"),
        "external_transfers_multiple_of_normal": state.get("external_transfer_vs_normal"),
        "recent_mobile_search": state.get("recent_product_search"),
        "credit_card_activity": state.get("credit_card_activity"),
        "engagement_level": state.get("engagement"),
        "products_owned": state.get("products_owned"),
    }
    return ("Real-time customer state from the streaming pipeline (amounts in IDR; transfer figures cover the "
            "last 24 hours, not 10 months):\n" + json.dumps(fields, indent=2) +
            "\n\nRecommend the next-best action for the relationship manager.")
