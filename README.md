# Z-Wave Web Interface

A Python-based web interface for the Z-Wave Protocol Controller (ZPC). Bridges ZPC MQTT communication to a browser-based UI for node management, SmartStart provisioning, and device control.

## Requirements

- **Python 3.10+**
- **ZPC** running and connected to an MQTT broker
- **MQTT broker** (e.g., Mosquitto 2.x) accessible from the host running this server

## Installation

```bash
cd zwave-web
pip install -r requirements.txt
```

## Running

```bash
python server.py
```

The server will attempt to connect to the MQTT broker at `localhost:1883` (configurable via the Setup tab or `config.json`). Open a browser to `http://<host>:8765`.

### Configuration

MQTT broker settings are persisted in `config.json`:

```json
{
  "mqtt_host": "localhost",
  "mqtt_port": 1883
}
```

This file is created automatically on first MQTT reconnect. You can also change the broker address from the Setup tab in the web UI.

## Features

### Nodes Tab
- View commissioned nodes with status, device class, and firmware version
- Add/remove nodes, abort inclusion/exclusion
- Remove failed nodes by ID
- Ping individual nodes or all nodes at once
- Toggle Basic CC (ON/OFF) per node

### SmartStart Tab
- Add DSKs to the provisioning list (Z-Wave or Z-Wave Long Range)
- View and manage pending provisioning entries
- Clear all DSKs
- Automatic device inclusion when a SmartStart device powers on

### Setup Tab
- Change MQTT broker host and port
- Connect/disconnect from the broker
- Factory reset the NCP (resets the Z-Wave network, generates a new Home ID)

## Architecture

The server acts as a bridge between three transports:

| Component | Transport | Role |
|-----------|-----------|------|
| Browser | HTTP + WebSocket | Serves the single-page UI, relays MQTT messages via WebSocket |
| ZPC | MQTT (v5) | Subscribes to `zpc/#`, publishes commands from the UI |
| Server | Python | Connects to both, broadcasts MQTT messages to all WebSocket clients |

### WebSocket Protocol

On connection, the server sends an init message with current state:

```json
{
  "type": "init",
  "home_id": "EFCA4E2C",
  "mqtt_host": "localhost",
  "mqtt_port": 1883,
  "mqtt_connected": true,
  "node_list": [...],
  "node_status": {},
  "basic_values": {},
  "smartstart_list": []
}
```

Subsequent messages are either incoming MQTT messages or client actions:

```json
{ "action": "ping_node", "node_id": 2 }
{ "action": "basic_set", "node_id": 2, "value": 255 }
{ "action": "factory_reset" }
{ "action": "mqtt_reconnect", "mqtt_host": "192.168.1.50", "mqtt_port": 1883 }
```

### Available Actions

| Action | Description |
|--------|-------------|
| `publish` | Publish arbitrary MQTT message (requires `topic` and `payload`) |
| `discovery` | Send ZPC discovery request |
| `node_list` | Request current node list |
| `add_node` | Start node inclusion |
| `abort_add` | Abort inclusion |
| `remove_node` | Start node exclusion |
| `abort_remove` | Abort exclusion |
| `remove_failed_node` | Remove a failed node (requires `node_id`) |
| `factory_reset` | Factory reset the NCP |
| `grant_keys` | Grant S2 keys during inclusion |
| `accept_dsk` | Accept DSK during inclusion |
| `smartstart_list` | Request SmartStart DSK list |
| `smartstart_add` | Add DSK to provisioning list |
| `smartstart_remove` | Remove DSK from provisioning list |
| `smartstart_clear` | Clear all SmartStart DSKs |
| `ping_node` | Ping a node via Basic CC |
| `basic_set` | Set Basic CC value |
| `mqtt_reconnect` | Change MQTT broker and reconnect |