import threading
import asyncio
import json
import logging
import pathlib
import signal
import threading

import websockets
from websockets.datastructures import Headers
from websockets.http11 import Response
import paho.mqtt.client as mqtt

PORT = 8765
MQTT_CLIENT_ID = "zwave-web"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("zwave-web")

# Load saved MQTT config
import os
import socket

CONFIG_FILE = pathlib.Path(__file__).parent / "config.json"


def resolve_display_hostname(host):
    """Return a display name for the MQTT broker host."""
    if host in ("localhost", "127.0.0.1"):
        return socket.gethostname()
    return host


def load_config():
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            pass
    return {"mqtt_host": "localhost", "mqtt_port": 1883}


def save_config(cfg):
    CONFIG_FILE.write_text(json.dumps(cfg))


config = load_config()
MQTT_HOST = config["mqtt_host"]
MQTT_PORT = config["mqtt_port"]
display_hostname = resolve_display_hostname(MQTT_HOST)

state_lock = threading.Lock()
connected_clients = set()
home_id = None
node_status = {}
node_list = []
smartstart_list = []
mqtt_client = None
event_loop = None
pinged_nodes = set()  # Track nodes we've pinged to synthesize status
basic_values = {}  # node_id -> current_value from BasicReport

STATIC_DIR = pathlib.Path(__file__).parent
MIME_TYPES = {
    ".html": "text/html",
    ".css": "text/css",
    ".js": "application/javascript",
    ".json": "application/json",
    ".png": "image/png",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}


mqtt_connected = False

def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    global mqtt_connected
    if rc == 0:
        logger.info("Connected to MQTT broker")
        mqtt_connected = True
        client.subscribe("zpc/#")
        client.publish("zpc/Discovery", "{}")
    else:
        mqtt_connected = False
        logger.error(f"MQTT connection failed with code {rc}")

def on_mqtt_disconnect(client, userdata, disconnect_flags, rc, properties=None):
    global mqtt_connected
    mqtt_connected = False
    if rc != 0:
        logger.warning(f"Disconnected from MQTT broker (rc={rc})")
    else:
        logger.info("Disconnected from MQTT broker")


def broadcast_to_clients(message):
    """Broadcast a JSON string to all connected WebSocket clients (thread-safe)."""
    global connected_clients
    if not connected_clients or event_loop is None:
        return

    async def _broadcast():
        global connected_clients
        dead = set()
        for ws in connected_clients:
            try:
                await ws.send(message)
            except Exception as e:
                logger.debug(f"Broadcast send failed: {e}")
                dead.add(ws)
        connected_clients -= dead

    fut = asyncio.run_coroutine_threadsafe(_broadcast(), event_loop)


def on_mqtt_message(client, userdata, msg):
    global home_id, node_list, node_status, smartstart_list, basic_values
    
    topic = msg.topic
    payload = msg.payload.decode("utf-8", errors="replace")
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        data = payload

    logger.debug(f"MQTT {topic}: {data}")

    with state_lock:
        parts = topic.split("/")
        if len(parts) >= 2 and parts[0] == "zpc" and parts[1] not in ("Discovery", "Network"):
            if home_id is None:
                home_id = parts[1]

        if topic.endswith("Discovery/Report"):
            if isinstance(data, dict) and "home_id" in data:
                home_id = data["home_id"]
                logger.info(f"Discovered home_id: {home_id}")

        # Factory reset report — global topic (no home_id segment), clear all state
        if topic == "zpc/Network/FactoryReset/Report":
            if isinstance(data, dict):
                old_home = home_id
                home_id = data.get("home_id")
                node_status.clear()
                node_list.clear()
                smartstart_list.clear()
                basic_values.clear()
                pinged_nodes.clear()
                logger.info(f"Factory reset complete: {old_home} -> {home_id}")
                # Re-discover
                if mqtt_client:
                    mqtt_client.publish("zpc/Discovery", "{}")

        if topic.endswith("Network/Node/List/Report"):
            node_list = data if isinstance(data, list) else []
            logger.info(f"Node list updated: {len(node_list)} nodes")
            # Clean up stale entries for removed nodes
            active_ids = {
                (n.get("node_information") or {}).get("node_id")
                for n in node_list
            }
            node_status = {nid: s for nid, s in node_status.items() if nid in active_ids}
            basic_values = {nid: v for nid, v in basic_values.items() if nid in active_ids}

        if topic.endswith("Network/Status/Report"):
            if isinstance(data, dict) and "node_id" in data:
                node_status[data["node_id"]] = data.get("status", "unknown")

        if topic.endswith("Network/SmartStart/List/Report"):
            if isinstance(data, dict) and "value" in data:
                smartstart_list = data["value"] if isinstance(data["value"], list) else []
                logger.info(f"SmartStart list updated: {len(smartstart_list)} entries")

        # Track Basic CC value and synthesize status when a pinged node responds
        if topic.endswith("/Basic/Report/BasicReport") and "/" in topic:
            parts = topic.split("/")
            try:
                node_hex = parts[2]  # zpc/<home_id>/<node_id>/ep0/Basic/Report/BasicReport
                node_id = int(node_hex, 16)
                # Track current Basic value
                if isinstance(data, dict):
                    basic_values[node_id] = data.get("current_value", 0)
                # Synthesize status report for pinged nodes
                if node_id in pinged_nodes:
                    pinged_nodes.discard(node_id)
                if node_status.get(node_id) != "online":
                    node_status[node_id] = "online"
                    status_msg = json.dumps({
                        "topic": f"zpc/{home_id}/Network/Status/Report",
                        "payload": {"node_id": node_id, "status": "online"},
                    })
                    broadcast_to_clients(status_msg)
                    logger.info(f"Synthesized status online for node {node_id} (BasicReport)")
            except (IndexError, ValueError):
                pass

        # Synthesize status when a node responds to Supervision (e.g., after BasicSet)
        if topic.endswith("/Supervision/Report/SupervisionReport") and "/" in topic:
            parts = topic.split("/")
            try:
                node_hex = parts[2]  # zpc/<home_id>/<node_id>/ep0/Supervision/Report/SupervisionReport
                node_id = int(node_hex, 16)
                # Node 1 is the controller itself — skip it
                if node_id == 1:
                    pass
                elif node_status.get(node_id) != "online":
                    node_status[node_id] = "online"
                    status_msg = json.dumps({
                        "topic": f"zpc/{home_id}/Network/Status/Report",
                        "payload": {"node_id": node_id, "status": "online"},
                    })
                    broadcast_to_clients(status_msg)
                    logger.info(f"Synthesized status online for node {node_id} (SupervisionReport)")
            except (IndexError, ValueError):
                pass


    message = json.dumps({"topic": topic, "payload": data})
    broadcast_to_clients(message)


def read_file_sync(filepath):
    try:
        return filepath.read_bytes()
    except FileNotFoundError:
        return None


def is_websocket_upgrade(request):
    """Check if the HTTP request is a WebSocket upgrade."""
    upgrade = request.headers.get("Upgrade", "").lower()
    connection = request.headers.get("Connection", "").lower()
    return upgrade == "websocket" and ("upgrade" in connection)


async def process_request(connection, request):
    """Serve static files for plain HTTP, pass WebSocket upgrades through."""
    path = request.path

    # WebSocket upgrade path
    if path == "/ws":
        if is_websocket_upgrade(request):
            return None  # Allow WebSocket upgrade
        # Plain HTTP to /ws -> serve index.html
        return await serve_file(connection, STATIC_DIR / "index.html")

    # Root path
    if path == "/":
        if is_websocket_upgrade(request):
            return None  # Allow WebSocket upgrade at /
        # Plain HTTP -> serve index.html
        return await serve_file(connection, STATIC_DIR / "index.html")

    # Static asset
    static_path = path.lstrip("/")
    filepath = STATIC_DIR / static_path
    if filepath.exists() and filepath.is_file():
        return await serve_file(connection, filepath)

    # Fallback to index.html
    return await serve_file(connection, STATIC_DIR / "index.html")


async def serve_file(connection, filepath):
    suffix = filepath.suffix
    content_type = MIME_TYPES.get(suffix, "application/octet-stream")
    body = read_file_sync(filepath)
    if body is None:
        return Response(
            404, "Not Found",
            Headers([("content-type", "text/plain")]),
            b"Not Found",
        )
    return Response(
        200, "OK",
        Headers([
            ("content-type", content_type),
            ("content-length", str(len(body))),
            # No cache-control headers => the browser heuristically caches the page
            # and keeps serving a stale copy after code changes. Force revalidation
            # so edits (e.g. the hostname in the title) show up on reload.
            ("cache-control", "no-cache, no-store, must-revalidate"),
            ("pragma", "no-cache"),
            ("expires", "0"),
        ]),
        body,
    )


async def ws_handler(websocket):
    connected_clients.add(websocket)
    global home_id, mqtt_client, event_loop, MQTT_HOST, MQTT_PORT, node_status, node_list, smartstart_list, basic_values, pinged_nodes, display_hostname
    try:
        with state_lock:
            init = {
                "type": "init",
                "home_id": home_id,
                "node_list": node_list,
                "node_status": node_status,
                "basic_values": basic_values,
                "smartstart_list": smartstart_list,
                "mqtt_connected": mqtt_connected,
                "mqtt_host": MQTT_HOST,
                "mqtt_port": MQTT_PORT,
                "display_hostname": display_hostname,
            }
        await websocket.send(json.dumps(init))
        logger.info(f"WebSocket client connected ({len(connected_clients)} total)")

        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            action = data.get("action")
            if not action:
                continue

            if action == "publish":
                topic = data.get("topic", "")
                payload = data.get("payload", {})
                if mqtt_client and topic:
                    mqtt_client.publish(topic, json.dumps(payload))

            elif action == "discovery":
                if mqtt_client:
                    mqtt_client.publish("zpc/Discovery", "{}")

            elif action == "node_list":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/Network/Node/List", "{}")

            elif action == "add_node":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/Network/Node/Add", "{}")

            elif action == "remove_node":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/Network/Node/Remove", "{}")

            elif action == "abort_add":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/Network/Node/Add/Abort", "{}")

            elif action == "abort_remove":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/Network/Node/Remove/Abort", "{}")

            elif action == "remove_failed_node":
                nid = data.get("node_id")
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and nid is not None:
                    mqtt_client.publish(
                        f"zpc/{hid}/Network/Node/RemoveFailed",
                        json.dumps({"node_id": nid}),
                    )

            elif action == "factory_reset":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/Network/FactoryReset", "{}")

            elif action == "mqtt_reconnect":
                new_host = data.get("mqtt_host")
                new_port = data.get("mqtt_port")
                if new_host == "" or new_port == 0:
                    # Disconnect
                    if mqtt_client:
                        mqtt_client.loop_stop()
                        mqtt_client.disconnect()
                        mqtt_client = None
                    logger.info("MQTT disconnected by user")
                    continue
                if new_host is not None or new_port is not None:
                    if new_host is not None:
                        MQTT_HOST = new_host
                    if new_port is not None:
                        MQTT_PORT = new_port
                    config["mqtt_host"] = MQTT_HOST
                    config["mqtt_port"] = MQTT_PORT
                    save_config(config)
                    display_hostname = resolve_display_hostname(MQTT_HOST)
                    logger.info(f"MQTT config updated: {MQTT_HOST}:{MQTT_PORT}")
                    if mqtt_client:
                        mqtt_client.loop_stop()
                        mqtt_client.disconnect()
                    with state_lock:
                        home_id = None
                        node_status.clear()
                        node_list.clear()
                        smartstart_list.clear()
                        basic_values.clear()
                    mqtt_client = create_mqtt_client()
                    await asyncio.to_thread(mqtt_client.connect, MQTT_HOST, MQTT_PORT, 60)
                    mqtt_client.loop_start()



            elif action == "grant_keys":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(
                        f"zpc/{hid}/Network/GrantKeys",
                        json.dumps({
                            "Accept": data.get("accept", True),
                            "Keys": data.get("keys", 0),
                            "CSA": data.get("csa", False),
                        }),
                    )

            elif action == "accept_dsk":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(
                        f"zpc/{hid}/Network/DSK/Accept",
                        json.dumps({"dsk": data.get("dsk", "")}),
                    )

            elif action == "smartstart_list":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/Network/SmartStart/List", "{}")

            elif action == "smartstart_add":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    dsk = data.get("dsk", "")
                    protocol = data.get("protocol", "Z-Wave Long Range")
                    payload = {"value": [{"DSK": dsk, "PreferredProtocols": [protocol]}]}
                    mqtt_client.publish(
                        f"zpc/{hid}/Network/SmartStart/Add",
                        json.dumps(payload),
                    )

            elif action == "smartstart_remove":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    dsk = data.get("dsk", "")
                    payload = {"value": [{"DSK": dsk}]}
                    mqtt_client.publish(
                        f"zpc/{hid}/Network/SmartStart/Remove",
                        json.dumps(payload),
                    )

            elif action == "smartstart_clear":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/Network/SmartStart/Clear", "{}")

            elif action == "ping_node":
                nid = data.get("node_id")
                with state_lock:
                    hid = home_id
                    pinged_nodes.add(nid) if nid is not None else None
                if mqtt_client and hid and nid is not None:
                    mqtt_client.publish(
                        f"zpc/{hid}/{nid:04X}/ep0/Basic/Command/BasicGet",
                        "{}",
                    )

            elif action == "basic_set":
                nid = data.get("node_id")
                value = data.get("value", 0)
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and nid is not None:
                    mqtt_client.publish(
                        f"zpc/{hid}/{nid:04X}/ep0/Basic/Command/BasicSet",
                        json.dumps({"value": value}),
                    )

            # Node Properties
            elif action == "node_properties":
                nid = data.get("node_id")
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and nid is not None:
                    mqtt_client.publish(
                        f"zpc/{hid}/Network/Node/Properties",
                        json.dumps({"node_id": nid}),
                    )

            # NLS
            elif action == "nls_enable":
                nid = data.get("node_id")
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and nid is not None:
                    mqtt_client.publish(
                        f"zpc/{hid}/Network/NLS/Enable",
                        json.dumps({"node_id": nid}),
                    )

            elif action == "nls_state":
                nid = data.get("node_id")
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and nid is not None:
                    mqtt_client.publish(
                        f"zpc/{hid}/Network/NLS/State",
                        json.dumps({"node_id": nid}),
                    )

            # OTA
            elif action == "ota_upload_image":
                image_name = data.get("image_name", "")
                image_data = data.get("data", [])
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and image_name:
                    mqtt_client.publish(
                        f"zpc/{hid}/OTA/UploadImage",
                        json.dumps({"image_name": image_name, "data": image_data}),
                    )

            elif action == "ota_list_images":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/OTA/ListImages", "{}")

            elif action == "ota_remove_image":
                image_name = data.get("image_name", "")
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and image_name:
                    mqtt_client.publish(
                        f"zpc/{hid}/OTA/RemoveImage",
                        json.dumps({"image_name": image_name}),
                    )

            elif action == "ota_start_upload":
                nid = data.get("node_id")
                image_name = data.get("image_name", "")
                wait_for_activation = data.get("wait_for_activation", False)
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and nid is not None and image_name:
                    mqtt_client.publish(
                        f"zpc/{hid}/OTA/StartFirmwareUpload",
                        json.dumps({
                            "node_id": nid,
                            "image_name": image_name,
                            "wait_for_activation": wait_for_activation,
                        }),
                    )

            elif action == "ota_progress":
                with state_lock:
                    hid = home_id
                if mqtt_client and hid:
                    mqtt_client.publish(f"zpc/{hid}/OTA/Progress", "{}")

            elif action == "ota_abort":
                nid = data.get("node_id")
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and nid is not None:
                    mqtt_client.publish(
                        f"zpc/{hid}/OTA/Abort",
                        json.dumps({"node_id": nid}),
                    )

            elif action == "ota_activate":
                nid = data.get("node_id")
                with state_lock:
                    hid = home_id
                if mqtt_client and hid and nid is not None:
                    mqtt_client.publish(
                        f"zpc/{hid}/OTA/Activate",
                        json.dumps({"node_id": nid}),
                    )

    except websockets.ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        logger.info(f"WebSocket client disconnected ({len(connected_clients)} remaining)")


def create_mqtt_client():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv5,
    )
    client.on_connect = on_mqtt_connect
    client.on_disconnect = on_mqtt_disconnect
    client.on_message = on_mqtt_message
    return client

def wait_for_mqtt(timeout=30):
    """Wait for MQTT broker to become available, with retries."""
    import time

    connected = [False]

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            connected[0] = True

    mqtt_test = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"{MQTT_CLIENT_ID}-probe",
        protocol=mqtt.MQTTv5,
    )
    mqtt_test.on_connect = on_connect

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            mqtt_test.connect(MQTT_HOST, MQTT_PORT, 10)
            mqtt_test.loop_start()
            for _ in range(50):
                if connected[0]:
                    mqtt_test.loop_stop()
                    mqtt_test.disconnect()
                    return True
                time.sleep(0.1)
            mqtt_test.loop_stop()
        except Exception:
            pass
        finally:
            try:
                mqtt_test.disconnect()
            except Exception:
                pass

        wait_time = min(3, deadline - time.monotonic())
        if wait_time > 0:
            time.sleep(wait_time)

    return False


async def run_server():
    global mqtt_client, event_loop

    logger.info(f"Waiting for MQTT broker at {MQTT_HOST}:{MQTT_PORT} ...")
    if not wait_for_mqtt():
        logger.error(f"Cannot reach MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        logger.error("Please ensure Mosquitto is running and accessible, then restart this server.")
        return

    event_loop = asyncio.get_event_loop()
    mqtt_client = create_mqtt_client()
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    mqtt_client.loop_start()


    stop = event_loop.create_future()

    def handle_signal():
        if not stop.done():
            stop.set_result(True)

    for sig in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(sig, handle_signal)

    try:
        async with websockets.serve(
            ws_handler,
            "0.0.0.0",
            PORT,
            process_request=process_request,
        ):
            logger.info(f"Server running on http://0.0.0.0:{PORT}")
            await stop
    except OSError as e:
        if e.errno == 98:
            logger.error(f"Port {PORT} is already in use. Another instance may be running.")
        else:
            logger.error(f"Failed to start HTTP server: {e}")
        return

    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    logger.info("Server shut down")


if __name__ == "__main__":
    asyncio.run(run_server())
