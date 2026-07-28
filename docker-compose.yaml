services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    container_name: local-clickhouse
    restart: unless-stopped
    ports:
      - "8123:8123"  # HTTP interface
      - "9000:9000"  # Native client interface
    environment:
      CLICKHOUSE_DB: jet
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - clickhouse_logs:/var/log/clickhouse-server
      - ./docker/clickhouse/init:/docker-entrypoint-initdb.d:ro
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

volumes:
  clickhouse_data:
  clickhouse_logs: