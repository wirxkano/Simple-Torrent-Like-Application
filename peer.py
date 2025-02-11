from random import randint
import bencodepy
import struct
import socket
import sys
import math
import urllib.parse
import json
import hashlib
import threading
import os
import time
from create_torrent import make_torrent
import uuid
from collections import defaultdict
import random

def read_file(filename):
    data = open(filename, 'rb')
    return data.read()


def throttle_bandwidth(data_length, bandwidth_limit):
    time_to_wait = data_length / bandwidth_limit
    time.sleep(time_to_wait)


def parse_handshake_message(message):
    if len(message) != 68:
        return None

    protocol_id = message[0:20]
    info_hash = message[28:48]
    peer_id = message[48:68]

    return {
        'protocol': protocol_id,
        'info_hash': info_hash,
        'peer_id': peer_id
    }


def listen_message(s):
    try:
        while True:
            length_prefix = s.recv(4)
            if length_prefix == 0:
                print("Connection closed")
                break

            if length_prefix == b'0':
                print("Received termination signal")
                break

            message = s.recv(int.from_bytes(length_prefix, sys.byteorder))
            if len(message) == 0:
                print("No message, connection closed")
                break
            return length_prefix + message
    except socket.timeout:
        print("Connection timed out.")


def extract_pieces_hashes(pieces_hashes):
    index, result = 0, []
    while index < len(pieces_hashes):
        result.append(pieces_hashes[index: index + 20].hex())
        index += 20
    return result


def read_piece_from_file(file_path, piece_index, piece_size):
    with open(file_path, 'rb') as f:
        f.seek(piece_index * piece_size)
        return f.read(piece_size)


def get_available_pieces(file_path, total_pieces, piece_size):
    available_pieces = []

    for i in range(total_pieces):
        if os.path.exists(file_path + f"_pieces/piece_{i}"):
            piece_data = read_file(file_path + f"_pieces/piece_{i}")
        elif os.path.exists(file_path):
            piece_data = read_piece_from_file(file_path, i, piece_size)
        else:
            piece_data = None
        if piece_data is not None:
            available_pieces.append(i)

    return sorted(available_pieces)


def create_bitfield(total_pieces, available_pieces):
    num_bytes = (total_pieces + 7) // 8
    bitfield = [0] * num_bytes

    for piece_index in available_pieces:
        byte_index = piece_index // 8
        bit_index = 7 - (piece_index % 8)
        bitfield[byte_index] |= (1 << bit_index)

    bitfield = struct.pack(f'{num_bytes}B', *bitfield)

    return struct.pack('>IB', len(bitfield) + 1, 5) + bitfield


def unpack_bitfield(bitfield_bytes, piece_count):
    pieces = []
    byte_format = f'{len(bitfield_bytes)}B'
    bitfield_integers = struct.unpack(byte_format, bitfield_bytes)

    for byte in bitfield_integers:
        for i in range(7, -1, -1):
            if len(pieces) < piece_count:
                bit = (byte >> i) & 1
                pieces.append(bool(bit))

    return pieces


def download_piece(sock, file_length, piece_length, total_number_of_pieces, piece_index, output_file, download_bandwidth_limit):
    default_piece_length = piece_length
    if piece_index == total_number_of_pieces - 1:
        piece_length = file_length - (default_piece_length * piece_index)
    else:
        piece_length = default_piece_length
    number_of_blocks = math.ceil(piece_length / (16 * 1024))
    data = bytearray()
    for block_index in range(number_of_blocks):
        begin = 2 ** 14 * block_index
        block_length = min(piece_length - begin, 2 ** 14)
        print(f"Requesting piece index {piece_index} and block index {block_index}")
        request_payload = struct.pack(">IBIII", 17, 6, piece_index, begin, block_length)
        sock.sendall(request_payload)
        message = listen_message(sock)
        if int(message[4]) == 0:
            print("You're choked, please wait!")
            while True:
                time.sleep(5)

                interested_message = struct.pack(">IB", 1, 2)
                sock.send(interested_message)
                message = listen_message(sock)
                if int(message[4]) == 1:
                    print("Unchoked! Resuming download...")
                    break
        elif int(message[4]) == 7:
            print(f"Downloading...")
            data.extend(message[13:])

    throttle_bandwidth(len(data), download_bandwidth_limit)

    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_file, "wb") as f:
        f.write(data)

    return output_file


def upload_piece(sock, piece_index, block_offset, block_length, piece_data):
    block = piece_data[block_offset:block_offset + block_length]

    message_length = 9 + len(block)

    piece_message = struct.pack(">IBII", message_length, 7, piece_index, block_offset) + block

    try:
        print(f"Uploading piece index {piece_index}...")
        sock.sendall(piece_message)
        return True
    except Exception as e:
        print("Failed to upload piece:", e)
        return False


def receive_msg_2(conn, info_hash, file_list, peer_handshake, lock):
    global count_upload
    MAX_UPLOAD = 4
    try:
        handshake = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x00\x00\x00" + info_hash + peer_id.encode()
        conn.send(handshake)
        other_peer_id = parse_handshake_message(peer_handshake).get("peer_id")

        if parse_handshake_message(peer_handshake).get("info_hash") != info_hash:
            print("Handshake failed: Info hash mismatch")
            conn.close()
            return

        print("Handshake successful")

        for info_file in file_list:
            file_path = info_file['file_path']
            piece_length = info_file['piece_length']
            pieces = info_file['pieces']
            total_number_of_pieces = len(extract_pieces_hashes(pieces))
            available_pieces = get_available_pieces(file_path, total_number_of_pieces, piece_length)

            bitfield_msg = create_bitfield(total_number_of_pieces, available_pieces)
            conn.send(bitfield_msg)

            while True:
                message = listen_message(conn)
                if not message:
                    with lock:
                        count_upload -= 1
                        peer_downloading.remove(other_peer_id.decode('utf-8'))
                    break
                if message[4] == 2:
                    with lock:
                        count_upload += 1
                        peer_downloading.append(other_peer_id.decode('utf-8'))
                    if count_upload <= MAX_UPLOAD or other_peer_id.decode('utf-8') in downloading_from:
                        unchoke_msg = struct.pack(">IB", 1, 1)
                        conn.send(unchoke_msg)
                    else:
                        with lock:
                            count_upload -= 1
                            peer_downloading.pop()
                        choke_msg = struct.pack(">IB", 1, 0)
                        conn.send(choke_msg)

                if message[4] == 6:
                    if (
                            count_upload > MAX_UPLOAD
                            and other_peer_id.decode('utf-8') not in downloading_from
                            and peer_downloading[0] == other_peer_id.decode('utf-8')
                    ):
                        with lock:
                            peer_downloading.pop(0)
                            count_upload -= 1
                        choke_msg = struct.pack(">IB", 1, 0)
                        conn.send(choke_msg)
                        continue

                    piece_index, begin, block_length = struct.unpack(">III", message[5:])

                    piece_data = None
                    piece_file_path = file_path + f"_pieces/piece_{piece_index}"
                    if os.path.exists(piece_file_path):
                        piece_data = read_file(piece_file_path)
                    elif os.path.exists(file_path):
                        piece_data = read_piece_from_file(file_path, piece_index, piece_length)

                    if piece_data is not None:
                        upload_piece(conn, piece_index, begin, block_length, piece_data)
                    else:
                        print(f"Requested piece {piece_index} not found for file: {file_path}")

        print(f"Finished processing all files")


    except BrokenPipeError:
        print("Broken pipe: Peer closed the connection.")
    except Exception as e:
        print(f"Unexpected error in receive_msg_2: {e}")
    finally:
        conn.close()

def receive_msg(s):
    global count_upload
    count_upload = 0
    global peer_downloading
    peer_downloading = []
    lock = threading.Lock()
    while True:
        conn, addr = s.accept()
        peer_handshake = conn.recv(68)
        info_hash = parse_handshake_message(peer_handshake).get("info_hash")

        if info_hash in files_uploaded:
            threading.Thread(
                target=receive_msg_2,
                args=(
                    conn,
                    info_hash,
                    files_uploaded[info_hash],
                    peer_handshake,
                    lock
                )
            ).start()

def send_msg(ip, port, info_hash, files, piece_length, pieces, requested_piece, lock, download_bandwidth_limit):
    handshake = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x00\x00\x00" + info_hash + peer_id.encode()
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((ip, port))
    print(f"Connected to peer at {ip}:{port}")

    client_socket.send(handshake)
    received_handshake = client_socket.recv(68)

    if parse_handshake_message(received_handshake).get("info_hash") != info_hash:
        print("Handshake failed: Info hash mismatch")
        client_socket.close()
        return

    print("Handshake successful")
    other_peer_id = parse_handshake_message(received_handshake).get("peer_id")
    for file in files:
        destination = file['file_path']
        file_length = file['file_length']
        message = listen_message(client_socket)
        total_number_of_pieces = len(extract_pieces_hashes(pieces))
        # have or bitfield message
        if int(message[4]) == 4 or int(message[4]) == 5:
            has_piece = unpack_bitfield(message[5:], total_number_of_pieces)
            interested_message = struct.pack(">IB", 1, 2)
            client_socket.send(interested_message)
            message = listen_message(client_socket)
            # choke message
            if int(message[4]) == 0:
                print("You're choked, please wait!")
                while True:
                    time.sleep(5)

                    interested_message = struct.pack(">IB", 1, 2)
                    client_socket.send(interested_message)
                    message = listen_message(client_socket)
                    if int(message[4]) == 1:
                        print("Unchoked! Resuming download...")
                        break
            # unchoke message
            if int(message[4]) == 1:
                for piece_index in range(0, total_number_of_pieces):
                    output_file = f"{destination}_pieces/piece_" + str(piece_index)
                    with lock:
                        if requested_piece[destination][piece_index] is None and has_piece[piece_index] == True:
                            requested_piece[destination][piece_index] = port
                    if (os.path.exists(output_file) or has_piece[piece_index] == False
                            or requested_piece[destination][piece_index] != port):
                        continue

                    download_piece(client_socket, file_length, piece_length,
                                   total_number_of_pieces, piece_index, output_file, download_bandwidth_limit)

                print("Download successfully!")
                with open(destination, 'wb') as f:
                    piece_files = [f for f in os.listdir(f"{destination}_pieces") if f.startswith("piece_")]
                    sorted_piece_files = sorted(piece_files, key=lambda x: int(x.split("_")[1]))
                    for piece_file in sorted_piece_files:
                        full_path = os.path.join(f"{destination}_pieces", str(piece_file))
                        with open(full_path, "rb") as pf:
                            f.write(pf.read())
                client_socket.send(b'0')

    downloading_from.discard(other_peer_id)
    client_socket.close()

def connect_tracker(torrent_file_path, peer_id, port, event):
    if event != "stopped":
        if os.path.isfile(torrent_file_path):
            data = bytes(read_file(torrent_file_path))
            file_data = bencodepy.decode(data)
        else:
            print(f"Error: The file '{torrent_file_path}' does not exist.")
            return
        tracker_ip, tracker_port = file_data.get(b'announce').decode().split(':')
        info = file_data.get(b'info')
        info_hash = hashlib.sha1(bencodepy.encode(info)).digest()
        length = 0
        files = []
        # single file
        if b'length' in info:
            length = info.get(b'length')
            files = [{b'path': [info.get(b'name')], b'length': length}]
        # multiple files
        elif b'files' in info:
            files = info.get(b'files')
            files = [file for file in files if file.get(b'attr') != b'p']
            length = sum(file[b'length'] for file in files)

        piece_length = info.get(b'piece length')
        pieces = info.get(b'pieces')
        original_file_name = info.get(b'name').decode()
        uploaded = 0
        downloaded = 0
        left = length
    else:
        info_hash = ""
        uploaded = 0
        downloaded = 0
        left = 0
        tracker_ip = "127.0.0.1"
        tracker_port = "12345"

    params = {
        "info_hash": info_hash,
        "peer_id": peer_id,
        "port": port,
        "uploaded": uploaded,
        "downloaded": downloaded,
        "compact": "1",
        "left": left,
        "event": event
    }

    encoded_params = urllib.parse.urlencode(params)
    request = f"{encoded_params}"
    rcv_data = []

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tracker_socket:
        tracker_socket.connect((tracker_ip, int(tracker_port)))
        tracker_socket.sendall(request.encode())

        response = b""
        while True:
            data = tracker_socket.recv(4096)
            if not data:
                break
            response += data
            parsed_data = json.loads(response)
            if "peers" in parsed_data:
                rcv_data = parsed_data["peers"]
                download_bandwidth_limit = parsed_data.get("bandwidth_limit", 40 * 1024)
            if event == "completed" and "bandwidth_limit" in parsed_data:
                rcv_data = parsed_data["message"]
                download_bandwidth_limit = parsed_data["bandwidth_limit"]
            if event == "stopped" and "message" in parsed_data:
                if parsed_data["message"] == "Peer removed":
                    return 0
            break

    return rcv_data, files, piece_length, pieces, original_file_name, info, info_hash, download_bandwidth_limit


def main():
    host_ip = ""
    global peer_id
    peer_id = "-PC0001-" + ''.join(random.choices("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=12))
    port = randint(1025, 2024)
    global files_uploaded
    files_uploaded = {}
    global downloading_from
    downloading_from = set()

    s = socket.socket()
    s.bind(("", port))
    s.listen(5)

    threading.Thread(target=receive_msg, args=(s,)).start()
    while True:
        user_input = input("Enter command 'upload', 'download' or 'quit': ")

        if user_input == "upload":
            upload_file = input("Enter the file or folder you want to share (e.g. myfile.pdf): ")
            file_path = os.path.abspath(str(upload_file))

            torrent_file_path = os.path.join(os.path.join('uploads', 'torrents'),
                                             f"{upload_file}_{uuid.uuid4()}.torrent")

            make_torrent(file_path, torrent_file_path)

            (rcv_data, files, piece_length, pieces,
             original_file_name, info, info_hash, download_bandwidth_limit) = connect_tracker(
                torrent_file_path, peer_id, port, "completed")
            if rcv_data == "OK":
                print("Upload successful, you're allowing other download from you.")
                print(f"Your bandwidth increased to: {download_bandwidth_limit / 1024}kB/s" )
                for file in files:
                    path = original_file_name
                    if b'files' in info:
                        base_path = original_file_name + "/"
                        path = os.path.join(base_path, *[p.decode() for p in file[b'path']])
                    file_path = os.path.abspath(str(path))

                    if not os.path.exists(file_path):
                        print(f"Error: File not found - {file_path}")
                        continue
                    info_file = {
                        'piece_length': piece_length,
                        'pieces': pieces,
                        'file_path': file_path,
                        'file_length': file[b'length']
                    }

                    if info_hash not in files_uploaded:
                        files_uploaded[info_hash] = []

                    files_uploaded[info_hash].append(info_file)

            else:
                print("Failed. Cannot connect with tracker to register, please try again!")

        if user_input == "download":
            user_file = input("Enter your Torrent file (e.g. myfile.torrent): ")

            rcv_data, files, piece_length, pieces, original_file_name, info, info_hash, download_bandwidth_limit = connect_tracker(
                user_file, peer_id, port, "started")
            peer_list = rcv_data
            print(peer_list)
            print(f"Your download bandwidth is {download_bandwidth_limit / 1024}kB/s, "
                  f"please upload more to increase it.")
            requested_piece = defaultdict(list)
            lock = threading.Lock()
            destination = input("Enter destination folder (e.g. downloads/):")
            os.makedirs(destination, exist_ok=True)

            for file in files:
                file_path = os.path.join(destination, *[p.decode() for p in file[b'path']])
                requested_piece[file_path] = [None] * len(extract_pieces_hashes(pieces))
                os.makedirs(os.path.dirname(file_path), exist_ok=True)

                info_file = {
                    'piece_length': piece_length,
                    'pieces': pieces,
                    'file_path': file_path,
                    'file_length': file[b'length']
                }

                if info_hash not in files_uploaded:
                    files_uploaded[info_hash] = []

                files_uploaded[info_hash].append(info_file)

            for peer in peer_list:
                downloading_from.add(peer[2])
                threading.Thread(target=send_msg, args=(
                    peer[0], int(peer[1]), info_hash,
                    files_uploaded[info_hash], piece_length, pieces, requested_piece, lock, download_bandwidth_limit
                )).start()

        if user_input == "quit":
            connect_tracker("", peer_id, port, "stopped")
            os._exit(1)


if __name__ == '__main__':
    main()
