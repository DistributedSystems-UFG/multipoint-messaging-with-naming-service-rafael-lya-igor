"""
Peer node — multipoint messaging with naming-service discovery.

Startup sequence
----------------
1. Read NAME_SERVICE_ADDRESS (the only static address in the config).
2. Wait for naming service to be reachable.
3. bind(PEER_NAME, PEER_HOST:PEER_PORT)  — register address.
4. register(PEER_NAME, "peer")            — mark process type.
5. Start gRPC server to receive messages.
6. Periodically discover("peer") and send random messages to peers.
7. On SIGTERM: unbind(PEER_NAME) then exit cleanly.
"""

import os
import sys
import time
import random
import signal
import threading
import logging
from concurrent import futures

import grpc

sys.path.insert(0, "/app")
import naming_pb2
import naming_pb2_grpc
import peer_pb2
import peer_pb2_grpc

# ── config (only NAME_SERVICE_ADDRESS is static) ──────────────────────────────

PEER_NAME       = os.environ["PEER_NAME"]
PEER_PORT       = int(os.environ.get("PEER_PORT", "50070"))
PEER_HOST       = os.environ.get("PEER_HOST", PEER_NAME)   # Docker container hostname
NAME_SERVICE    = os.environ["NAME_SERVICE_ADDRESS"]        # the only hardcoded address

MSG_INTERVAL_LO = float(os.environ.get("MSG_INTERVAL_LO", "3.0"))
MSG_INTERVAL_HI = float(os.environ.get("MSG_INTERVAL_HI", "7.0"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{PEER_NAME}] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

_my_address = f"{PEER_HOST}:{PEER_PORT}"

# ── naming-service helpers ────────────────────────────────────────────────────

def _ns_stub() -> naming_pb2_grpc.NamingServiceStub:
    return naming_pb2_grpc.NamingServiceStub(grpc.insecure_channel(NAME_SERVICE))


def _wait_for_naming_service(retries: int = 30, delay: float = 2.0):
    for attempt in range(1, retries + 1):
        try:
            ch = grpc.insecure_channel(NAME_SERVICE)
            grpc.channel_ready_future(ch).result(timeout=3)
            ch.close()
            log.info("Naming service ready at %s", NAME_SERVICE)
            return
        except Exception:
            log.warning("Naming service not ready yet (attempt %d/%d)…", attempt, retries)
            time.sleep(delay)
    raise RuntimeError(f"Naming service at {NAME_SERVICE} never became ready")


def bind_and_register():
    ns = _ns_stub()
    r = ns.Bind(naming_pb2.BindRequest(name=PEER_NAME, address=_my_address))
    if not r.ok:
        raise RuntimeError(f"bind failed: {r.message}")
    log.info("Bound   %s  →  %s", PEER_NAME, _my_address)

    r = ns.Register(naming_pb2.RegisterRequest(name=PEER_NAME, type="peer"))
    if not r.ok:
        raise RuntimeError(f"register failed: {r.message}")
    log.info("Registered %s as type=peer", PEER_NAME)


def unbind():
    try:
        r = _ns_stub().Unbind(naming_pb2.UnbindRequest(name=PEER_NAME))
        if r.ok:
            log.info("Unbound %s from naming service", PEER_NAME)
    except Exception as e:
        log.warning("Unbind failed: %s", e)


def discover_peers() -> list[tuple[str, str]]:
    try:
        r = _ns_stub().Discover(naming_pb2.DiscoverRequest(type="peer"))
        return [(p.name, p.address) for p in r.processes if p.name != PEER_NAME]
    except Exception as e:
        log.warning("Discover failed: %s", e)
        return []

# ── peer-to-peer messaging ────────────────────────────────────────────────────

_peer_channels: dict[str, grpc.Channel] = {}
_peer_channels_mu = threading.Lock()


def _peer_stub(address: str) -> peer_pb2_grpc.PeerServiceStub:
    with _peer_channels_mu:
        if address not in _peer_channels:
            _peer_channels[address] = grpc.insecure_channel(address)
        return peer_pb2_grpc.PeerServiceStub(_peer_channels[address])


def send_message(target_name: str, target_addr: str, content: str):
    try:
        _peer_stub(target_addr).SendMessage(
            peer_pb2.MessageRequest(sender=PEER_NAME, content=content),
            timeout=3.0,
        )
        log.info("→ %-20s  \"%s\"", target_name, content)
    except grpc.RpcError as e:
        log.warning("Could not reach %s @ %s: %s", target_name, target_addr, e.details())


def _message_loop():
    while True:
        time.sleep(random.uniform(MSG_INTERVAL_LO, MSG_INTERVAL_HI))
        peers = discover_peers()
        if not peers:
            log.debug("No peers found in naming service")
            continue
        name, addr = random.choice(peers)
        msg = f"Hello from {PEER_NAME}! (token={random.randint(1000, 9999)})"
        send_message(name, addr, msg)

# ── gRPC server ───────────────────────────────────────────────────────────────

class PeerServicer(peer_pb2_grpc.PeerServiceServicer):
    def SendMessage(self, request, context):
        log.info("← %-20s  \"%s\"", request.sender, request.content)
        return peer_pb2.MessageResponse(ok=True)


def _start_grpc_server() -> grpc.Server:
    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    peer_pb2_grpc.add_PeerServiceServicer_to_server(PeerServicer(), srv)
    srv.add_insecure_port(f"[::]:{PEER_PORT}")
    srv.start()
    log.info("gRPC server listening on port %d", PEER_PORT)
    return srv

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    _wait_for_naming_service()
    bind_and_register()

    srv = _start_grpc_server()

    def _shutdown(signum, frame):
        log.info("Shutting down…")
        unbind()
        srv.stop(grace=3)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    threading.Thread(target=_message_loop, daemon=True).start()

    log.info("Peer %s active  [ns=%s  addr=%s]", PEER_NAME, NAME_SERVICE, _my_address)
    srv.wait_for_termination()


if __name__ == "__main__":
    main()
