import struct
import socket
import ssl
import time
import threading
import logging
from typing import Optional, Generator

logger = logging.getLogger("camera_client")
logging.basicConfig(level=logging.INFO)

class BambuCameraManager:
    """
    Manages local TLS video frame acquisition from Bambu Lab A1 / P1 series on port 6000.
    Caches the latest JPEG frame in memory and provides MJPEG multipart stream generators.
    """
    def __init__(self, ip: str = "", access_code: str = "", port: int = 6000):
        self.ip = ip
        self.access_code = access_code
        self.port = port
        
        self.latest_frame: Optional[bytes] = None
        self.last_frame_time: float = 0.0
        self.is_connected: bool = False
        self.should_run: bool = True
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def update_config(self, ip: str, access_code: str):
        with self._lock:
            changed = (ip != self.ip or access_code != self.access_code)
            self.ip = ip
            self.access_code = access_code
        if changed and self.thread and self.thread.is_alive():
            logger.info("Camera config updated, will reconnect in background.")

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.should_run = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.should_run = False

    def get_latest_frame(self) -> Optional[bytes]:
        with self._lock:
            return self.latest_frame

    def get_mjpeg_stream(self) -> Generator[bytes, None, None]:
        """
        Yields multipart MJPEG chunks for FastAPI StreamingResponse.
        """
        last_yielded_time = 0.0
        while self.should_run:
            frame = None
            frame_time = 0.0
            with self._lock:
                frame = self.latest_frame
                frame_time = self.last_frame_time

            if frame and frame_time > last_yielded_time:
                last_yielded_time = frame_time
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Content-Length: " + str(len(frame)).encode() + b"\r\n\r\n" +
                    frame + b"\r\n"
                )
            # Sleep slightly to align with ~2 fps camera stream
            time.sleep(0.2)

    def _recv_exact(self, sock, num_bytes: int) -> Optional[bytes]:
        buf = bytearray()
        while len(buf) < num_bytes and self.should_run:
            try:
                chunk = sock.recv(min(num_bytes - len(buf), 32768))
                if not chunk:
                    return None
                buf.extend(chunk)
            except (socket.timeout, ssl.SSLError):
                continue
            except Exception:
                return None
        return bytes(buf) if len(buf) == num_bytes else None

    def _worker_loop(self):
        username = "bblp"
        jpeg_start = b"\xff\xd8"
        jpeg_end = b"\xff\xd9"

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        while self.should_run:
            if not self.ip or not self.access_code:
                time.sleep(3)
                continue

            auth_data = bytearray()
            auth_data += struct.pack("<I", 0x40)
            auth_data += struct.pack("<I", 0x3000)
            auth_data += struct.pack("<I", 0)
            auth_data += struct.pack("<I", 0)
            auth_data += struct.pack("32s", username.encode("ascii"))
            with self._lock:
                cur_code = self.access_code
                cur_ip = self.ip
            auth_data += struct.pack("32s", cur_code.encode("ascii"))

            sock = None
            ssl_sock = None
            try:
                sock = socket.create_connection((cur_ip, self.port), timeout=6.0)
                ssl_sock = ctx.wrap_socket(sock, server_hostname=cur_ip)
                ssl_sock.write(auth_data)
                ssl_sock.settimeout(5.0)

                self.is_connected = True
                logger.info(f"Bambu Camera connected on {cur_ip}:{self.port}")

                while self.should_run:
                    header = self._recv_exact(ssl_sock, 16)
                    if not header:
                        break

                    payload_size = int.from_bytes(header[0:4], byteorder="little")
                    if payload_size <= 0 or payload_size > 8_000_000:
                        logger.warning(f"Unexpected camera frame size: {payload_size}, reconnecting")
                        break

                    frame = self._recv_exact(ssl_sock, payload_size)
                    if not frame:
                        break

                    if frame.startswith(jpeg_start) and (jpeg_end in frame[-4:]):
                        with self._lock:
                            self.latest_frame = frame
                            self.last_frame_time = time.time()

            except Exception as e:
                logger.debug(f"Camera stream tick: {e}")
            finally:
                self.is_connected = False
                if ssl_sock:
                    try:
                        ssl_sock.close()
                    except Exception:
                        pass
                if sock:
                    try:
                        sock.close()
                    except Exception:
                        pass

            time.sleep(3)

# Global singleton
bambu_camera = BambuCameraManager()
