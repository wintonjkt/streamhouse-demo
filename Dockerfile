# Banking Streamhouse demo image. In OpenShift the base is the openshift/python:3.11-ubi9 image stream tag
# (set in the BuildConfig); this FROM is the equivalent public Red Hat image for local builds.
FROM registry.access.redhat.com/ubi9/python-311
WORKDIR /opt/app-root/src
COPY backend/requirements.txt backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt
COPY backend backend
COPY flink flink
COPY web web
# The Db2 CA is written to backend/certs at start-up; OpenShift runs as a random uid in group 0.
RUN mkdir -p backend/certs && chmod -R g+rwX backend/certs
EXPOSE 8080
CMD ["python3", "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]
