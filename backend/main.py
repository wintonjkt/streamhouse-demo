"""FastAPI application: Db2 setup, data and event generation, and read APIs.

Run from the repo root:  python3 -m uvicorn backend.main:app --port 8000
"""
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import config, db, generator, kafka_io, orchestrate

app = FastAPI(title="Banking Streamhouse Demo API", version="0.1.0",
              description="Synthetic data only. Thresholds are demo logic, not production policy.")


@app.exception_handler(generator.NotFound)
async def _not_found(_, exc):
    return JSONResponse(status_code=404, content={"error": str(exc)})


@app.exception_handler(generator.BadRequest)
async def _bad_request(_, exc):
    return JSONResponse(status_code=400, content={"error": str(exc)})


@app.exception_handler(db.Db2Error)
async def _db_error(_, exc):
    return JSONResponse(status_code=502, content={"error": "Db2: %s" % exc})


@app.exception_handler(orchestrate.OrchestrateError)
async def _orchestrate_error(_, exc):
    return JSONResponse(status_code=502, content={"error": "Orchestrate: %s" % exc})


@app.exception_handler(kafka_io.KafkaError_)
async def _kafka_error(_, exc):
    return JSONResponse(status_code=502, content={"error": "Kafka: %s" % exc})


# ------------------------------------------------------------------ health

@app.get("/api/health", tags=["health"])
def health() -> Dict[str, Any]:
    """Reports each dependency separately; never fails as a whole."""
    out: Dict[str, Any] = {}
    try:
        out["db2"] = {"ok": True, **db.ping(), "tables": db.existing_tables()}
    except Exception as exc:
        out["db2"] = {"ok": False, "error": str(exc)}
    try:
        topics = kafka_io.list_topics()
        out["kafka"] = {"ok": True, "demo_topics": {t: t in topics for t in kafka_io.TOPICS}}
    except Exception as exc:
        out["kafka"] = {"ok": False, "error": str(exc)}
    out["orchestrate"] = orchestrate.status()
    return out


# ------------------------------------------------------------------ admin / setup

@app.post("/api/admin/schema", tags=["admin"])
def create_schema(drop_existing: bool = False) -> Dict[str, Any]:
    """Create the demo tables in Db2. With drop_existing=true the demo tables are dropped first."""
    return db.create_schema(drop_existing)


@app.post("/api/admin/seed", tags=["admin"])
def seed() -> Dict[str, Any]:
    """Delete all demo data and load the seed customer (Andi, C001) and accounts."""
    return db.seed()


@app.post("/api/admin/topics", tags=["admin"])
def create_topics() -> Dict[str, Any]:
    """Create the bank.* Kafka topics (the brokers do not auto-create topics)."""
    return kafka_io.ensure_topics()


@app.post("/api/admin/publish-reference-data", tags=["admin"])
def publish_reference_data() -> Dict[str, Any]:
    """Publish CUSTOMER and ACCOUNT to the compacted topics. Fallback for the Connect JDBC source."""
    return generator.publish_reference_data()


@app.post("/api/admin/reset", tags=["admin"])
def reset() -> Dict[str, Any]:
    """Remove generated events and runs from Db2 and start a fresh scenario run.

    Kafka topics are not purged; delete and recreate them in Control Center for a clean stream.
    """
    cleared = db.clear_events()
    return {"cleared": cleared, "run": generator.start_run("C001")}


# ------------------------------------------------------------------ reference data (read)

@app.get("/api/customers", tags=["data"])
def customers() -> List[Dict[str, Any]]:
    return db.query("SELECT * FROM CUSTOMER ORDER BY CUSTOMER_ID")


@app.get("/api/customers/{customer_id}", tags=["data"])
def customer(customer_id: str) -> Dict[str, Any]:
    row = generator._require_customer(customer_id)
    row["accounts"] = db.query("SELECT * FROM ACCOUNT WHERE CUSTOMER_ID = ? ORDER BY ACCOUNT_ID", (customer_id,))
    return row


# ------------------------------------------------------------------ scenario

@app.get("/api/scenario/steps", tags=["scenario"])
def scenario_steps() -> Dict[str, Any]:
    return {"name": generator.SCENARIO_NAME, "steps": generator.SCENARIO_STEPS}


class RunRequest(BaseModel):
    customer_id: str = "C001"


@app.post("/api/scenario/runs", tags=["scenario"])
def start_run(body: RunRequest = RunRequest()) -> Dict[str, Any]:
    """Start a new scenario run (no events are sent yet)."""
    return generator.start_run(body.customer_id)


@app.get("/api/scenario/runs/latest", tags=["scenario"])
def latest_run(customer_id: str = "C001") -> Dict[str, Any]:
    run = generator.latest_run(customer_id)
    if not run:
        raise generator.NotFound("No runs yet for %s" % customer_id)
    return run


@app.get("/api/scenario/runs/{run_id}", tags=["scenario"])
def get_run(run_id: str) -> Dict[str, Any]:
    return generator.get_run(run_id)


@app.post("/api/scenario/runs/{run_id}/next", tags=["scenario"])
def next_step(run_id: str) -> Dict[str, Any]:
    """Generate the next scripted event: writes the audit row to Db2 and publishes to Kafka."""
    return generator.next_step(run_id)


# ------------------------------------------------------------------ custom events

class TransactionRequest(BaseModel):
    customer_id: str = "C001"
    account_id: str = "C001-SAV"
    txn_type: str = Field("TRANSFER", max_length=30)
    direction: Literal["IN", "OUT"]
    channel: Literal["INTERNAL", "EXTERNAL", "CARD"]
    amount: float = Field(gt=0)
    description: Optional[str] = Field(None, max_length=200)
    counterparty_bank: Optional[str] = Field(None, max_length=60)
    run_id: Optional[str] = None


@app.post("/api/transactions", tags=["events"])
def create_transaction(body: TransactionRequest) -> Dict[str, Any]:
    """Generate a single custom transaction (Db2 audit row + Kafka bank.transactions)."""
    return generator.create_transaction(**body.dict())


class DigitalEventRequest(BaseModel):
    customer_id: str = "C001"
    event_type: str = Field("PRODUCT_SEARCH", max_length=30)
    channel: str = Field("MOBILE", max_length=20)
    event_text: Optional[str] = Field(None, max_length=200)
    numeric_value: Optional[float] = None
    run_id: Optional[str] = None


@app.post("/api/digital-events", tags=["events"])
def create_digital_event(body: DigitalEventRequest) -> Dict[str, Any]:
    """Generate a single custom digital event (Db2 audit row + Kafka bank.digital-events)."""
    return generator.create_digital_event(**body.dict())


@app.get("/api/transactions", tags=["events"])
def list_transactions(customer_id: str = "C001", limit: int = Query(50, ge=1, le=500)) -> List[Dict[str, Any]]:
    """Generated transactions as stored in Db2, newest first."""
    return db.query("SELECT * FROM TXN_EVENT WHERE CUSTOMER_ID = ? ORDER BY EVENT_TIME DESC FETCH FIRST %d ROWS ONLY"
                    % limit, (customer_id,))


@app.get("/api/digital-events", tags=["events"])
def list_digital_events(customer_id: str = "C001", limit: int = Query(50, ge=1, le=500)) -> List[Dict[str, Any]]:
    return db.query("SELECT * FROM DIGITAL_EVENT WHERE CUSTOMER_ID = ? ORDER BY EVENT_TIME DESC "
                    "FETCH FIRST %d ROWS ONLY" % limit, (customer_id,))


# ------------------------------------------------------------------ Kafka viewer

@app.get("/api/kafka/{topic}", tags=["kafka"])
def read_topic(topic: str, limit: int = Query(20, ge=1, le=200)) -> Dict[str, Any]:
    """Most recent records of a bank.* topic (read-only viewer)."""
    if topic not in kafka_io.TOPICS:
        raise HTTPException(status_code=404, detail="Only bank.* demo topics can be read")
    return {"topic": topic, "records": kafka_io.tail(topic, limit)}


# ------------------------------------------------------------------ AI recommendation log

class RecommendationRequest(BaseModel):
    customer_id: str = "C001"
    risk_level: Literal["LOW", "MEDIUM", "HIGH"]
    input_state: Dict[str, Any]
    recommendation: str
    source: str = Field(max_length=40)
    run_id: Optional[str] = None


@app.post("/api/recommendations", tags=["ai"])
def save_recommendation(body: RecommendationRequest) -> Dict[str, Any]:
    """Log the state sent to the AI assistant and what it recommended (governance audit)."""
    return generator.save_recommendation(**body.dict())


@app.get("/api/recommendations", tags=["ai"])
def list_recommendations(customer_id: str = "C001") -> List[Dict[str, Any]]:
    return db.query("SELECT * FROM AI_RECOMMENDATION WHERE CUSTOMER_ID = ? ORDER BY CREATED_AT DESC", (customer_id,))


class DecisionRequest(BaseModel):
    decision: Literal["APPROVED", "DISMISSED"]


@app.post("/api/recommendations/{rec_id}/decision", tags=["ai"])
def decide(rec_id: str, body: DecisionRequest) -> Dict[str, Any]:
    """Record the relationship manager's decision. Nothing is sent to the customer."""
    return generator.decide_recommendation(rec_id, body.decision)


# ------------------------------------------------------------------ Flink output + AI agent

@app.get("/api/state/{customer_id}", tags=["ai"])
def latest_state(customer_id: str, run_id: Optional[str] = None) -> Dict[str, Any]:
    """Latest customer-state record produced by Flink on bank.customer-state (null until Flink has output)."""
    records = [r for r in kafka_io.tail(config.TOPIC_STATE, 50)
               if r["key"] == customer_id and isinstance(r["value"], dict)
               and (run_id is None or r["value"].get("run_id") == run_id)]
    return {"customer_id": customer_id, "state": records[-1]["value"] if records else None}


class RecommendRequest(BaseModel):
    customer_id: str = "C001"
    run_id: Optional[str] = None


@app.post("/api/agent/recommend", tags=["ai"])
def recommend(body: RecommendRequest) -> Dict[str, Any]:
    """Send the latest Flink state to the watsonx Orchestrate agent and log its advisory recommendation."""
    state = latest_state(body.customer_id, body.run_id)["state"]
    if state is None:
        raise HTTPException(status_code=409, detail="No customer state from Flink yet; generate events first")
    prompt = orchestrate.build_prompt(state)
    text = orchestrate.chat(prompt)
    record = generator.save_recommendation(
        customer_id=body.customer_id, risk_level=state["customer_risk"], input_state=state,
        recommendation=text, source="orchestrate", run_id=state.get("run_id"))
    return {"state": state, "prompt": prompt, "recommendation": record}


@app.get("/api/flink-sql", tags=["ai"])
def flink_sql() -> Dict[str, str]:
    """The Flink SQL statements that are deployed on Confluent (flink/*.sql), for display in the web page."""
    return {p.name: p.read_text(encoding="utf-8") for p in sorted((config.ROOT / "flink").glob("*.sql"))}


# ------------------------------------------------------------------ web page

@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(config.ROOT / "web" / "index.html")
