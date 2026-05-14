import os
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
    
def init_tracer(worker_version: str = "1.0.0"):
    # 1. Create a Resource
    resource = Resource.create({
        "service.name": "email-worker",
        "service.version": worker_version,
    })

    # 2. Setup Exporter (reading from env var)
    endpoint = os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT", 
        "http://localhost:4318/v1/traces"
    )
    exporter = OTLPSpanExporter(endpoint=endpoint)

    # 3. Create Batch Processor
    span_processor = BatchSpanProcessor(exporter)

    # 4. Initialize TracerProvider and register global
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(span_processor)
    trace.set_tracer_provider(provider)
# 5. Auto-instrument requests calls
    RequestsInstrumentor().instrument()

    print(f"Tracing initialized for email-worker ({worker_version}) -> {endpoint}")