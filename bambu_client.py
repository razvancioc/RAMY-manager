import json
import ssl
import time
import random
import threading
import logging
from typing import Dict, Any, Optional, Callable
import paho.mqtt.client as mqtt

logger = logging.getLogger("bambu_client")
logging.basicConfig(level=logging.INFO)

class BambuPrinterManager:
    def __init__(self, ip: str = "", access_code: str = "", serial: str = "", port: int = 8883):
        self.ip = ip
        self.access_code = access_code
        self.serial = serial or ""
        self.port = port
        
        self.client: Optional[mqtt.Client] = None
        self.is_connected = False
        self.should_run = True
        self.thread: Optional[threading.Thread] = None
        self.seq_id = random.randint(1000, 9999)
        self.on_state_change: Optional[Callable[[Dict[str, Any]], None]] = None
        
        self.state: Dict[str, Any] = {
            "connected": False,
            "ip": self.ip,
            "serial": self.serial,
            "name": "Bambu Lab A1",
            "gcode_state": "OFFLINE", # IDLE, RUNNING, PAUSE, FINISH, FAILED, PREPARE, OFFLINE
            "nozzle_temper": 0.0,
            "nozzle_target_temper": 0.0,
            "bed_temper": 0.0,
            "bed_target_temper": 0.0,
            "chamber_temper": 0.0,
            "fan_speed": 0,
            "speed_level": 2, # 1: Silent, 2: Standard, 3: Sport, 4: Ludicrous
            "mc_percent": 0,
            "mc_remaining_time": 0, # in minutes
            "layer_num": 0,
            "total_layer_num": 0,
            "subtask_name": "",
            "chamber_light": "off",
            "ams": [
                {"slot": 1, "color": "#1A1A1A", "type": "PLA", "remain": 100, "active": False},
                {"slot": 2, "color": "#E02424", "type": "PLA", "remain": 100, "active": False},
                {"slot": 3, "color": "#FACA15", "type": "PLA", "remain": 100, "active": False},
                {"slot": 4, "color": "#F9FAFB", "type": "PLA", "remain": 100, "active": False},
            ],
            "last_updated": None,
            "error_msg": None
        }

    def update_config(self, ip: str, access_code: str, serial: str = ""):
        needs_restart = (ip != self.ip or access_code != self.access_code or serial != self.serial)
        self.ip = ip
        self.access_code = access_code
        if serial:
            self.serial = serial
        self.state["ip"] = self.ip
        self.state["serial"] = self.serial
        if needs_restart and self.client:
            self._disconnect()

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.should_run = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.should_run = False
        self._disconnect()

    def reconnect(self):
        logger.info("Explicit reconnect requested.")
        self.should_run = True
        self._disconnect()
        self._connect_mqtt()

    def _disconnect(self):
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception as e:
                logger.debug(f"Disconnect exception: {e}")
            finally:
                self.client = None
                self.is_connected = False
                self.state["connected"] = False
                self.state["gcode_state"] = "OFFLINE"
                self._notify()

    def _worker_loop(self):
        while self.should_run:
            if not self.is_connected and self.ip and self.access_code:
                try:
                    self._connect_mqtt()
                except Exception as e:
                    logger.debug(f"Connection attempt failed: {e}")
                    self.is_connected = False
                    self.state["connected"] = False
                    self.state["gcode_state"] = "OFFLINE"
                    self.state["error_msg"] = str(e)
                    self._notify()
            time.sleep(8)

    def _connect_mqtt(self):
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass
            self.client = None

        client_id = f"bambu_mgr_{random.randint(1000, 9999)}"
        try:
            # Compatibility with paho-mqtt 2.x and 1.x - MQTTv311 is REQUIRED for Bambu Lab
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv311)
        except AttributeError:
            self.client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

        self.client.username_pw_set("bblp", self.access_code)
        try:
            self.client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT, cert_reqs=ssl.CERT_NONE)
        except Exception:
            self.client.tls_set(tls_version=ssl.PROTOCOL_TLS, cert_reqs=ssl.CERT_NONE)
        self.client.tls_insecure_set(True)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        logger.info(f"Connecting to Bambu Lab printer at {self.ip}:{self.port}...")
        self.client.connect(self.ip, self.port, keepalive=60)
        self.client.loop_start()

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        rc_code = rc if isinstance(rc, int) else getattr(rc, "value", 0)
        if rc_code == 0:
            logger.info("Successfully connected to Bambu Lab printer!")
            self.is_connected = True
            self.state["connected"] = True
            self.state["error_msg"] = None
            # Subscribe to exact serial report (wildcard '+' causes disconnects on A1)
            serial = self.serial or ""
            if serial:
                client.subscribe(f"device/{serial}/report")
                logger.info(f"Subscribed to device/{serial}/report")
            else:
                client.subscribe("device/+/report")
                logger.info("Subscribed to device/+/report")
            self._notify()
            # Request immediate pushall state
            try:
                pushall = json.dumps({"pushing": {"sequence_id": self._next_seq(), "command": "pushall"}})
                client.publish(f"device/{serial}/request", pushall)
            except Exception as e:
                logger.debug(f"Pushall error: {e}")
        else:
            logger.warning(f"Connection failed with code: {rc_code}")
            self.is_connected = False
            self.state["connected"] = False
            self.state["gcode_state"] = "OFFLINE"
            self.state["error_msg"] = f"Eroare conectare (cod {rc_code})"
            self._notify()

    def _on_disconnect(self, client, userdata, flags=None, rc=0, properties=None):
        logger.info(f"Printer disconnected. rc: {rc}")
        self.is_connected = False
        self.state["connected"] = False
        self.state["gcode_state"] = "OFFLINE"
        self._notify()

    def _on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            # Extract serial from topic: device/<serial>/report
            parts = topic.split("/")
            if len(parts) >= 2 and not self.serial:
                self.serial = parts[1]
                self.state["serial"] = self.serial

            payload = json.loads(msg.payload.decode("utf-8", errors="ignore"))
            self._parse_payload(payload)
        except Exception as e:
            logger.error(f"Error parsing MQTT message: {e}")

    def _parse_payload(self, data: Dict[str, Any]):
        print_data = data.get("print", {})
        if not print_data:
            return

        updated = False

        if "gcode_state" in print_data:
            self.state["gcode_state"] = print_data["gcode_state"]
            updated = True

        if "nozzle_temper" in print_data:
            self.state["nozzle_temper"] = round(float(print_data["nozzle_temper"]), 1)
            updated = True
        if "nozzle_target_temper" in print_data:
            self.state["nozzle_target_temper"] = round(float(print_data["nozzle_target_temper"]), 1)
            updated = True

        if "bed_temper" in print_data:
            self.state["bed_temper"] = round(float(print_data["bed_temper"]), 1)
            updated = True
        if "bed_target_temper" in print_data:
            self.state["bed_target_temper"] = round(float(print_data["bed_target_temper"]), 1)
            updated = True

        if "chamber_temper" in print_data:
            self.state["chamber_temper"] = round(float(print_data["chamber_temper"]), 1)
            updated = True

        if "mc_percent" in print_data:
            self.state["mc_percent"] = int(print_data["mc_percent"])
            updated = True

        if "mc_remaining_time" in print_data:
            self.state["mc_remaining_time"] = int(print_data["mc_remaining_time"])
            updated = True

        if "layer_num" in print_data:
            self.state["layer_num"] = int(print_data["layer_num"])
            updated = True
        if "total_layer_num" in print_data:
            self.state["total_layer_num"] = int(print_data["total_layer_num"])
            updated = True

        if "subtask_name" in print_data:
            self.state["subtask_name"] = str(print_data["subtask_name"] or "")
            updated = True

        if "spd_lvl" in print_data:
            self.state["speed_level"] = int(print_data["spd_lvl"])
            updated = True

        # Parse lights
        if "lights_report" in print_data:
            for l in print_data["lights_report"]:
                if l.get("node") == "chamber_light":
                    self.state["chamber_light"] = l.get("mode", "off")
                    updated = True

        # Parse AMS trays
        ams_info = print_data.get("ams", {})
        if ams_info and "ams" in ams_info and len(ams_info["ams"]) > 0:
            first_ams = ams_info["ams"][0]
            trays = first_ams.get("tray", [])
            tray_tar = ams_info.get("tray_tar")
            ams_list = []
            for idx, tray in enumerate(trays):
                color_raw = tray.get("tray_color", "FFFFFF")
                color_hex = f"#{color_raw[:6]}" if not color_raw.startswith("#") else color_raw
                tray_type = tray.get("tray_type") or "PLA"
                tray_sub_brands = tray.get("tray_sub_brands", "")
                remain = tray.get("remain", 100)
                tag_uid = tray.get("tag_uid", "")
                
                # Detect if the slot is empty
                is_empty = (not tray.get("tray_type") and not tray.get("tray_color")) or (remain == -1 and not tag_uid and not tray.get("tray_type"))
                
                ams_list.append({
                    "slot": idx + 1,
                    "color": color_hex,
                    "type": tray_type,
                    "brand": tray_sub_brands or "Bambu Lab",
                    "remain": remain,
                    "empty": bool(is_empty),
                    "active": (str(idx) == str(tray_tar))
                })
            if ams_list:
                self.state["ams"] = ams_list
                updated = True

        if updated:
            self.state["last_updated"] = time.strftime("%H:%M:%S")
            self._notify()

    def _notify(self):
        if self.on_state_change:
            try:
                self.on_state_change(self.get_state())
            except Exception as e:
                logger.error(f"Error in on_state_change callback: {e}")

    def get_state(self) -> Dict[str, Any]:
        return dict(self.state)

    def _next_seq(self) -> str:
        self.seq_id += 1
        return str(self.seq_id)

    def send_command(self, payload: Dict[str, Any]) -> bool:
        if not self.is_connected or not self.client:
            logger.warning("Cannot send command, printer is offline.")
            return False
        topic = f"device/{self.serial}/request" if self.serial else "device/+/request"
        try:
            msg_str = json.dumps(payload)
            self.client.publish(topic, msg_str)
            logger.info(f"Published command to {topic}: {msg_str}")
            return True
        except Exception as e:
            logger.error(f"Error publishing command: {e}")
            return False

    def pause_print(self) -> bool:
        return self.send_command({
            "print": {
                "sequence_id": self._next_seq(),
                "command": "pause"
            }
        })

    def resume_print(self) -> bool:
        return self.send_command({
            "print": {
                "sequence_id": self._next_seq(),
                "command": "resume"
            }
        })

    def stop_print(self) -> bool:
        return self.send_command({
            "print": {
                "sequence_id": self._next_seq(),
                "command": "stop"
            }
        })

    def set_speed(self, level: int) -> bool:
        # level: 1 (Quiet), 2 (Standard), 3 (Sport), 4 (Ludicrous)
        self.state["speed_level"] = level
        return self.send_command({
            "print": {
                "sequence_id": self._next_seq(),
                "command": "print_speed",
                "param": str(level)
            }
        })

    def toggle_light(self, turn_on: bool) -> bool:
        mode = "on" if turn_on else "off"
        self.state["chamber_light"] = mode
        return self.send_command({
            "system": {
                "sequence_id": self._next_seq(),
                "command": "ledctrl",
                "led_node": "chamber_light",
                "led_mode": mode,
                "led_on_time": 0,
                "led_off_time": 0,
                "loop_times": 0,
                "interval_time": 0
            }
        })

# Global singleton instance
bambu_printer = BambuPrinterManager()
