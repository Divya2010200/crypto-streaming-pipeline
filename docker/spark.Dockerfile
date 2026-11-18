FROM apache/spark:4.3.0-python3

USER root
RUN pip3 install --no-cache-dir "psycopg[binary]" \
    || pip3 install --no-cache-dir --break-system-packages "psycopg[binary]"
RUN mkdir -p /checkpoints /ivy /app /tmp/spark-home \
    && chown -R 185:0 /checkpoints /ivy /app /tmp/spark-home
COPY --chown=185:0 pipeline /app/pipeline
COPY --chown=185:0 streaming /app/streaming
COPY --chown=185:0 sql /app/sql
USER 185

ENV HOME=/tmp/spark-home
ENV PYTHONPATH=/app
WORKDIR /app
