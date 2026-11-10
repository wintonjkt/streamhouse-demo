"""Settings for the backend. Secrets come from .env.local and data (2).json (both git-ignored)."""
import base64
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CERT_DIR = ROOT / "backend" / "certs"
SQL_DIR = ROOT / "backend" / "sql"


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_env_file(ROOT / ".env.local")


def _db2_settings() -> dict:
    cfg_path = ROOT / os.environ.get("DB2_CONFIG_FILE", "data (2).json")
    d = json.loads(cfg_path.read_text(encoding="utf-8"))["connection"]["db2"]
    cert_path = CERT_DIR / "db2-ca.crt"
    if not cert_path.exists():
        CERT_DIR.mkdir(parents=True, exist_ok=True)
        cert_path.write_bytes(base64.b64decode(d["certificate"]["certificate_base64"]))
    host = d["hosts"][0]
    return {
        "database": d["database"],
        "hostname": host["hostname"],
        "port": host["port"],
        "username": d["authentication"]["username"],
        "password": d["authentication"]["password"],
        "cert": str(cert_path),
    }


class Settings:
    def __init__(self) -> None:
        self.db2 = _db2_settings()
        self.kafka_bootstrap = os.environ["KAFKA_BOOTSTRAP"]
        self.kafka_username = os.environ["KAFKA_USERNAME"]
        self.kafka_password = os.environ["KAFKA_PASSWORD"]
        self.kafka_ca = str(ROOT / os.environ.get("KAFKA_CA_FILE", "backend/certs/kafka-ca.crt"))
        self.kafka_hostname_check = os.environ.get("KAFKA_HOSTNAME_CHECK", "none")

    @property
    def db2_dsn(self) -> str:
        d = self.db2
        return (
            "DATABASE=%(database)s;HOSTNAME=%(hostname)s;PORT=%(port)s;PROTOCOL=TCPIP;"
            "UID=%(username)s;PWD=%(password)s;SECURITY=SSL;SSLServerCertificate=%(cert)s;" % d
        )

    @property
    def kafka_client_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": self.kafka_username,
            "sasl.password": self.kafka_password,
            "ssl.ca.location": self.kafka_ca,
            "ssl.endpoint.identification.algorithm": self.kafka_hostname_check,
        }


settings = Settings()

TOPIC_TRANSACTIONS = "bank.transactions"
TOPIC_DIGITAL = "bank.digital-events"
TOPIC_CUSTOMER = "bank.customer"
TOPIC_ACCOUNTS = "bank.accounts"
TOPIC_STATE = "bank.customer-state"
TOPIC_ALERTS = "bank.alerts"
