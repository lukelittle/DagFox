# Airflow 3.3.2 on Python 3.12 plus DagFox's runtime dependencies.
FROM apache/airflow:3.3.2-python3.12

# Re-pin apache-airflow so pip can never up/downgrade it while resolving extras.
RUN pip install --no-cache-dir \
      "apache-airflow==3.3.2" \
      "pydantic-ai-slim[anthropic,bedrock,openai]>=2.60,<3" \
      "pyyaml>=6.0" \
      "pytest>=8"

ENV PYTHONPATH=/opt/dagfox \
    PYDANTIC_AI_NO_BANNER=1
