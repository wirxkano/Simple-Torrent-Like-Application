import socket
import json
import threading
from urllib.parse import parse_qs


def extract_data(data):
    parsed_params = parse_qs(data)
    info_hash = parsed_params.get("info_hash", [None])[0]
    peer_id = parsed_params.get("peer_id", [None])[0]
    peer_port = parsed_params.get("port", [None])[0]
    uploaded = parsed_params.get("uploaded", [None])[0]
    left = parsed_params.get("left", [None])[0]
    event = parsed_params.get("event", [None])[0]
    compact = parsed_params.get("compact", [None])[0]

    result = {
        "info_hash": info_hash,
        "peer_id": peer_id,
        "peer_port": peer_port,
        "uploaded": uploaded,
        "left": left,
        "event": event,
        "compact": compact
    }

    return result


def handle_peer(peer_sockets, addr, peer_info_hash, bandwidth_limit, result):
    try:
        peer_id = result["peer_id"]
        event = result["event"]

        if peer_id not in bandwidth_limit:
            bandwidth_limit[peer_id] = 40 * 1024

        if event == "started":
            other_peers = [peer for peer in peer_info_hash if peer != peer_info_hash[len(peer_info_hash) - 1]]
            response = {
                "peers": other_peers,
                "bandwidth_limit": bandwidth_limit[peer_id],
            }
            peer_sockets[addr].send(json.dumps(response).encode("utf-8"))
        elif event == "completed":
            bandwidth_limit[peer_id] += 10 * 1024
            response = {"message": "OK", "bandwidth_limit": bandwidth_limit[peer_id]}
            peer_sockets[addr].send(json.dumps(response).encode("utf-8"))
        elif event == "stopped":
            for info_hash, peer_list in list(peers.items()):
                peers[info_hash] = [peer for peer in peer_list if peer[2] != peer_id]
                if not peers[info_hash]:
                    del peers[info_hash]

            response = {"message": "Peer removed"}
            peer_sockets[addr].send(json.dumps(response).encode("utf-8"))
            print(f"{addr[0]} quit.")



    except (ConnectionResetError, BrokenPipeError):
        print(f"Connection with {addr} closed.")


def main():
    peer_connections = {}
    global peers
    peers = {}
    bandwidth_limit = {}
    port = 12345

    s = socket.socket()
    s.bind(("", port))
    s.listen(5)
    print("Server is listening...")

    while True:
        c, addr = s.accept()
        print("Accepted connection request from ", addr[0])
        if addr not in peer_connections:
            peer_connections[addr] = c

        data = c.recv(1024).decode("utf-8")
        result = extract_data(data)

        info_hash = result.get("info_hash")
        peer_port = result.get("peer_port")
        peer_id = result.get("peer_id")
        if info_hash not in peers:
            peers[info_hash] = []
        peers[info_hash].append((addr[0], peer_port, peer_id))
        threading.Thread(
            target=handle_peer, args=(peer_connections, addr, peers[info_hash], bandwidth_limit, result)
        ).start()


if __name__ == '__main__':
    main()