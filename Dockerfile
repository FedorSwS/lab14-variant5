FROM golang:1.21-alpine AS go-builder
WORKDIR /app
COPY collector/go.mod collector/go.sum ./
RUN go mod download
COPY collector/ .
RUN CGO_ENABLED=0 go build -o collector .

FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y curl
COPY --from=go-builder /app/collector /usr/local/bin/
COPY analyzer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY analyzer/ ./analyzer/
RUN mkdir -p /app/data/raw /app/data/processed
EXPOSE 8080 50051
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
ENTRYPOINT ["/usr/local/bin/collector"]
CMD ["-workers=4", "-rate=100"]
