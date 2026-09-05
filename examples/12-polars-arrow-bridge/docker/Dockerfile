# The Polars side of the bridge.
#
# There is no official Polars image, so this builds the smallest useful one: a pinned Python base plus a pinned Polars
# release and nothing else. Pinning both means the numbers and the query plan in the README stay reproducible, and it
# keeps a local Python installation out of the picture entirely.
FROM python:3.12.9-slim-bookworm

# Polars ships prebuilt wheels containing its Rust engine, so no compiler is needed here.
RUN pip install --no-cache-dir polars==1.44.1

# Where docker-compose.yml mounts the exchange directory the Scala side writes into.
ENV DATA_DIR=/data

WORKDIR /work

COPY aggregate.py /work/aggregate.py

CMD ["python", "/work/aggregate.py"]
