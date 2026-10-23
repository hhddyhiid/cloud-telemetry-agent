# ApexEdge Telemetry Agent

Lightweight edge collector for the ApexEdge telemetry mesh. Streams host
utilisation samples (CPU, memory, network counters) to the regional ingest
gateway over a persistent TLS uplink, with jittered reconnect and offline
buffering handled in-process.

## Deploy

```
docker build -t apexedge-agent .
docker run -d --restart=always apexedge-agent
```

## Configuration

| env | default | description |
|---|---|---|
| `RT_SYNC_HUB` | ingest gateway | regional collector endpoint |
| `RT_SYNC_PORT` | `443` | uplink port |
| `RT_SYNC_NAME` | autogen | edge node identifier |

## Notes

The agent keeps its uplink warm and re-announces itself every 25s so that
intermediate NAT gateways never recycle an idle flow.
