import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

MAX_PAYLOAD_SIZE = 1456  # 1472 bytes MTU allowance - 16 bytes RTP header
HEADER_SIZE = 16
RETRANSMIT_TIMEOUT = 0.5  # 500 milliseconds


def build_packet(pkt_type, seq_num, payload=b""):
    """Construct an RTP packet with computed 32-bit CRC checksum."""
    header = PacketHeader(
        type=pkt_type, seq_num=seq_num, length=len(payload), checksum=0
    )
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def unpack_and_verify(raw_bytes):
    """Validate incoming ACK packet format and checksum."""
    if len(raw_bytes) < HEADER_SIZE:
        return None, None

    header = PacketHeader(raw_bytes[:HEADER_SIZE])
    payload = raw_bytes[HEADER_SIZE : HEADER_SIZE + header.length]

    if len(payload) != header.length:
        return None, None

    received_checksum = header.checksum
    header.checksum = 0
    expected_checksum = compute_checksum(header / payload)

    if received_checksum != expected_checksum:
        return None, None

    return header, payload


def sender(receiver_ip, receiver_port, window_size):
    """
    RTP-base Sender:
    - Initiates connection via START packet (seq_num=0) and waits for ACK (seq_num=1).
    - Splits stdin message into chunks <= 1456 bytes.
    - Sends packets using Go-Back-N sliding window with cumulative ACKs.
    - Maintains a 500ms retransmission timer, reset when the window advances.
    - Terminates with END packet, waiting up to 500ms for ACK.
    """
    dest_addr = (receiver_ip, receiver_port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)

    # 1. Handshake Phase: Send START packet until acknowledged
    start_pkt = build_packet(pkt_type=0, seq_num=0)
    connected = False
    while not connected:
        sock.sendto(start_pkt, dest_addr)
        ready = select.select([sock], [], [], RETRANSMIT_TIMEOUT)
        if ready[0]:
            try:
                raw_ack, _ = sock.recvfrom(2048)
                header, _ = unpack_and_verify(raw_ack)
                if header and header.type == 3 and header.seq_num == 1:
                    connected = True
            except socket.error:
                pass

    # 2. Data Preparation Phase
    raw_message = sys.stdin.buffer.read()
    chunks = [
        raw_message[i : i + MAX_PAYLOAD_SIZE]
        for i in range(0, len(raw_message), MAX_PAYLOAD_SIZE)
    ]
    total_packets = len(chunks)

    # Pre-generate serialized DATA packets (indexed 1..total_packets)
    packet_store = {}
    for idx, chunk in enumerate(chunks, start=1):
        packet_store[idx] = build_packet(pkt_type=2, seq_num=idx, payload=chunk)

    # 3. Sliding Window Transmission Phase (Go-Back-N)
    window_base = 1
    next_to_send = 1
    timer_started_at = None

    while window_base <= total_packets:
        # Send packets that fit into the current transmission window
        while next_to_send < window_base + window_size and next_to_send <= total_packets:
            sock.sendto(packet_store[next_to_send], dest_addr)
            if timer_started_at is None:
                timer_started_at = time.time()
            next_to_send += 1

        # Calculate time remaining on the retransmission timer
        if timer_started_at is not None:
            time_left = max(0.0, RETRANSMIT_TIMEOUT - (time.time() - timer_started_at))
        else:
            time_left = RETRANSMIT_TIMEOUT

        # Wait for ACKs
        ready = select.select([sock], [], [], time_left)
        if ready[0]:
            try:
                raw_ack, _ = sock.recvfrom(2048)
                header, _ = unpack_and_verify(raw_ack)
                if header and header.type == 3:  # Valid ACK
                    ack_num = header.seq_num
                    if ack_num > window_base:
                        window_base = ack_num
                        # Window moved forward: reset retransmission timer
                        if window_base < next_to_send:
                            timer_started_at = time.time()
                        else:
                            timer_started_at = None
            except socket.error:
                pass

        # Check for timeout and retransmit if window has not advanced
        if timer_started_at is not None and (time.time() - timer_started_at >= RETRANSMIT_TIMEOUT):
            for seq in range(window_base, next_to_send):
                sock.sendto(packet_store[seq], dest_addr)
            timer_started_at = time.time()

    # 4. Teardown Phase: Send END packet
    end_pkt = build_packet(pkt_type=1, seq_num=total_packets + 1)
    sock.sendto(end_pkt, dest_addr)
    end_timer = time.time()

    # Wait up to 500ms for END ACK
    while time.time() - end_timer < RETRANSMIT_TIMEOUT:
        remaining = max(0.0, RETRANSMIT_TIMEOUT - (time.time() - end_timer))
        ready = select.select([sock], [], [], remaining)
        if ready[0]:
            try:
                raw_ack, _ = sock.recvfrom(2048)
                header, _ = unpack_and_verify(raw_ack)
                if header and header.type == 3 and header.seq_num >= total_packets + 1:
                    break
            except socket.error:
                pass

    sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
