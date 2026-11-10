"""Scenario and event generator.

Every generated event is (1) stored in Db2 as an audit record and (2) published to Kafka.
The Kafka partition/offset is written back to the Db2 row so the two can be correlated.
Balances in Db2 are opening balances and are never changed here; Flink derives the current balance.
"""
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import config, db, kafka_io

M = 1_000_000

# The demo story from banking_streamhouse_demo_plan.md, section 2.
SCENARIO_NAME = "andi-fund-migration"
SCENARIO_STEPS: List[Dict[str, Any]] = [
    {"step": 1, "kind": "txn", "label": "Salary deposit +IDR 35M",
     "account_id": "C001-SAV", "txn_type": "SALARY_DEPOSIT", "direction": "IN", "channel": "INTERNAL",
     "amount": 35 * M, "description": "Monthly salary"},
    {"step": 2, "kind": "txn", "label": "External transfer -IDR 80M",
     "account_id": "C001-SAV", "txn_type": "TRANSFER", "direction": "OUT", "channel": "EXTERNAL",
     "amount": 80 * M, "counterparty_bank": "Other Bank A", "description": "Transfer to external bank"},
    {"step": 3, "kind": "digital", "label": "Mobile search: time deposit rate",
     "event_type": "PRODUCT_SEARCH", "channel": "MOBILE", "event_text": "time deposit rate"},
    {"step": 4, "kind": "txn", "label": "External transfer -IDR 40M",
     "account_id": "C001-SAV", "txn_type": "TRANSFER", "direction": "OUT", "channel": "EXTERNAL",
     "amount": 40 * M, "counterparty_bank": "Other Bank A", "description": "Second transfer to external bank"},
    {"step": 5, "kind": "digital", "label": "Credit-card usage declining (optional)",
     "event_type": "CARD_USAGE_CHANGE", "channel": "CARD", "event_text": "Card usage down 60% versus 4-week average",
     "numeric_value": -60},
]


class NotFound(LookupError):
    pass


class BadRequest(ValueError):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (value.microsecond // 1000)


def _require_customer(customer_id: str) -> Dict[str, Any]:
    row = db.query_one("SELECT * FROM CUSTOMER WHERE CUSTOMER_ID = ?", (customer_id,))
    if not row:
        raise NotFound("Customer %s not found. Seed the database first." % customer_id)
    return row


def _account_for(customer_id: str, account_id: str) -> Dict[str, Any]:
    row = db.query_one("SELECT * FROM ACCOUNT WHERE ACCOUNT_ID = ? AND CUSTOMER_ID = ?", (account_id, customer_id))
    if not row:
        raise NotFound("Account %s not found for customer %s" % (account_id, customer_id))
    return row


# ---------------------------------------------------------------- events

def create_transaction(customer_id: str, account_id: str, txn_type: str, direction: str, channel: str,
                       amount: float, description: Optional[str] = None, counterparty_bank: Optional[str] = None,
                       run_id: Optional[str] = None, currency: str = "IDR") -> Dict[str, Any]:
    _require_customer(customer_id)
    _account_for(customer_id, account_id)
    event_id, when = str(uuid.uuid4()), _now()
    db.execute(
        "INSERT INTO TXN_EVENT (EVENT_ID, RUN_ID, CUSTOMER_ID, ACCOUNT_ID, EVENT_TIME, TXN_TYPE, DIRECTION, CHANNEL, "
        "AMOUNT, CURRENCY, COUNTERPARTY_BANK, DESCRIPTION) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (event_id, run_id, customer_id, account_id, db.ts(when), txn_type, direction, channel,
         amount, currency, counterparty_bank, description))
    payload = {"event_id": event_id, "run_id": run_id, "customer_id": customer_id, "account_id": account_id,
               "event_time": _iso(when), "txn_type": txn_type, "direction": direction, "channel": channel,
               "amount": amount, "currency": currency, "counterparty_bank": counterparty_bank,
               "description": description}
    pub = kafka_io.publish(config.TOPIC_TRANSACTIONS, customer_id, payload)
    db.execute("UPDATE TXN_EVENT SET KAFKA_TOPIC=?, KAFKA_PARTITION=?, KAFKA_OFFSET=? WHERE EVENT_ID=?",
               (pub["topic"], pub["partition"], pub["offset"], event_id))
    return {"event": payload, "kafka": pub}


def create_digital_event(customer_id: str, event_type: str, channel: str, event_text: Optional[str] = None,
                         numeric_value: Optional[float] = None, run_id: Optional[str] = None) -> Dict[str, Any]:
    _require_customer(customer_id)
    event_id, when = str(uuid.uuid4()), _now()
    db.execute(
        "INSERT INTO DIGITAL_EVENT (EVENT_ID, RUN_ID, CUSTOMER_ID, EVENT_TIME, EVENT_TYPE, CHANNEL, EVENT_TEXT, "
        "NUMERIC_VALUE) VALUES (?,?,?,?,?,?,?,?)",
        (event_id, run_id, customer_id, db.ts(when), event_type, channel, event_text, numeric_value))
    payload = {"event_id": event_id, "run_id": run_id, "customer_id": customer_id, "event_time": _iso(when),
               "event_type": event_type, "channel": channel, "event_text": event_text,
               "numeric_value": numeric_value}
    pub = kafka_io.publish(config.TOPIC_DIGITAL, customer_id, payload)
    db.execute("UPDATE DIGITAL_EVENT SET KAFKA_TOPIC=?, KAFKA_PARTITION=?, KAFKA_OFFSET=? WHERE EVENT_ID=?",
               (pub["topic"], pub["partition"], pub["offset"], event_id))
    return {"event": payload, "kafka": pub}


# ---------------------------------------------------------------- scenario

def start_run(customer_id: str = "C001") -> Dict[str, Any]:
    _require_customer(customer_id)
    run_id = str(uuid.uuid4())
    db.execute("INSERT INTO SCENARIO_RUN (RUN_ID, CUSTOMER_ID, SCENARIO_NAME, STATUS, NEXT_STEP) VALUES (?,?,?,?,0)",
               (run_id, customer_id, SCENARIO_NAME, "RUNNING"))
    return get_run(run_id)


def get_run(run_id: str) -> Dict[str, Any]:
    run = db.query_one("SELECT * FROM SCENARIO_RUN WHERE RUN_ID = ?", (run_id,))
    if not run:
        raise NotFound("Run %s not found" % run_id)
    run["total_steps"] = len(SCENARIO_STEPS)
    run["next"] = SCENARIO_STEPS[run["next_step"]]["label"] if run["next_step"] < len(SCENARIO_STEPS) else None
    return run


def latest_run(customer_id: str = "C001") -> Optional[Dict[str, Any]]:
    row = db.query_one("SELECT RUN_ID FROM SCENARIO_RUN WHERE CUSTOMER_ID = ? ORDER BY STARTED_AT DESC "
                       "FETCH FIRST 1 ROWS ONLY", (customer_id,))
    return get_run(row["run_id"]) if row else None


def next_step(run_id: str) -> Dict[str, Any]:
    run = get_run(run_id)
    index = run["next_step"]
    if run["status"] == "COMPLETED" or index >= len(SCENARIO_STEPS):
        raise BadRequest("Run %s is already completed" % run_id)
    step = SCENARIO_STEPS[index]
    customer_id = run["customer_id"]
    if step["kind"] == "txn":
        result = create_transaction(
            customer_id, step["account_id"], step["txn_type"], step["direction"], step["channel"], step["amount"],
            description=step.get("description"), counterparty_bank=step.get("counterparty_bank"), run_id=run_id)
    else:
        result = create_digital_event(
            customer_id, step["event_type"], step["channel"], step.get("event_text"), step.get("numeric_value"),
            run_id=run_id)
    done = index + 1 >= len(SCENARIO_STEPS)
    db.execute("UPDATE SCENARIO_RUN SET NEXT_STEP=?, STATUS=?, UPDATED_AT=CURRENT TIMESTAMP WHERE RUN_ID=?",
               (index + 1, "COMPLETED" if done else "RUNNING", run_id))
    return {"step": step["step"], "label": step["label"], "run": get_run(run_id), **result}


# ---------------------------------------------------------------- reference data

def publish_reference_data() -> Dict[str, Any]:
    """Publish CUSTOMER and ACCOUNT rows to the compacted reference topics.

    This is a direct fallback for the Kafka Connect JDBC source described in the plan; use one or the other.
    """
    customers = db.query("SELECT * FROM CUSTOMER")
    accounts = db.query("SELECT * FROM ACCOUNT")
    for row in customers:
        kafka_io.publish(config.TOPIC_CUSTOMER, row["customer_id"], row)
    for row in accounts:
        kafka_io.publish(config.TOPIC_ACCOUNTS, row["account_id"], row)
    return {"customers": len(customers), "accounts": len(accounts)}


# ---------------------------------------------------------------- AI recommendations

def save_recommendation(customer_id: str, risk_level: str, input_state: Dict[str, Any], recommendation: str,
                        source: str, run_id: Optional[str] = None) -> Dict[str, Any]:
    _require_customer(customer_id)
    rec_id = str(uuid.uuid4())
    db.execute(
        "INSERT INTO AI_RECOMMENDATION (RECOMMENDATION_ID, RUN_ID, CUSTOMER_ID, RISK_LEVEL, INPUT_STATE, "
        "RECOMMENDATION, SOURCE) VALUES (?,?,?,?,?,?,?)",
        (rec_id, run_id, customer_id, risk_level, json.dumps(input_state)[:8000], recommendation[:8000], source))
    return db.query_one("SELECT * FROM AI_RECOMMENDATION WHERE RECOMMENDATION_ID = ?", (rec_id,))


def decide_recommendation(rec_id: str, decision: str) -> Dict[str, Any]:
    if db.execute("UPDATE AI_RECOMMENDATION SET RM_DECISION=?, DECIDED_AT=CURRENT TIMESTAMP WHERE RECOMMENDATION_ID=?",
                  (decision, rec_id)) == 0:
        raise NotFound("Recommendation %s not found" % rec_id)
    return db.query_one("SELECT * FROM AI_RECOMMENDATION WHERE RECOMMENDATION_ID = ?", (rec_id,))
