import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

MAX_PAYLOAD_SIZE = 1456
HEADER_SIZE = 16
RETRANSMIT_TIMEOUT = 0.5  # 500 milliseconds


def build_packet(pkt_type, seq_num, payload=b""):
    """Construct an RTP packet with 32-bit CRC checksum."""
    header = PacketHeader(
        type=pkt_type, seq_num=seq_num, length=len(payload), checksum=0
    )
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def unpack_and_verify(raw_bytes):
    """Validate incoming ACK packet structure and checksum."""
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
    RTP-opt Sender (Selective Repeat):
    - Handshake with START packet (seq_num=0) awaiting ACK (seq_num=1).
    - Tracks per-packet ACK state within the transmission window.
    - Maintains a single 500ms window timer.
    - On timeout, selectively retransmits ONLY unacknowledged packets in current window.
    - Advances window base as the lowest unACKed packet moves forward.
    - Teardown with END packet awaiting final ACK or 500ms timeout.
    """
    dest_addr = (receiver_ip, receiver_port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)

    # 1. Connection Handshake
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

    # 2. Data Preparation
    raw_message = sys.stdin.buffer.read()
    chunks = [
        raw_message[i : i + MAX_PAYLOAD_SIZE]
        for i in range(0, len(raw_message), MAX_PAYLOAD_SIZE)
    ]
    total_packets = len(chunks)

    packet_store = {}
    for idx, chunk in enumerate(chunks, start=1):
        packet_store[idx] = build_packet(pkt_type=2, seq_num=idx, payload=chunk)

    # 3. Selective Repeat Window Transmission
    window_base = 1
    next_to_send = 1
    acknowledged_seqs = set()
    timer_started_at = None

    while window_base <= total_packets:
        # Transmit new packets allowed by the sliding window
        while next_to_send < window_base + window_size and next_to_send <= total_packets:
            sock.sendto(packet_store[next_to_send], dest_addr)
            if timer_started_at is None:
                timer_started_at = time.time()
            next_to_send += 1

        # Calculate time remaining for the 500ms window timer
        if timer_started_at is not None:
            time_left = max(0.0, RETRANSMIT_TIMEOUT - (time.time() - timer_started_at))
        else:
            time_left = RETRANSMIT_TIMEOUT

        ready = select.select([sock], [], [], time_left)
        if ready[0]:
            try:
                raw_ack, _ = sock.recvfrom(2048)
                header, _ = unpack_and_verify(raw_ack)
                if header and header.type == 3:
                    ack_seq = header.seq_num
                    if ack_seq >= window_base:
                        acknowledged_seqs.add(ack_seq)

                        # Slide window base forward past all consecutively ACKed packets
                        prev_base = window_base
                        while window_base in acknowledged_seqs:
                            window_base += 1

                        if window_base > prev_base:
                            # Window advanced: reset the retransmission timer
                            if window_base < next_to_send:
                                timer_started_at = time.time()
                            else:
                                timer_started_at = None
            except socket.error:
                pass

        # On timeout: retransmit ONLY unacknowledged packets currently in window
        if timer_started_at is not None and (time.time() - timer_started_at >= RETRANSMIT_TIMEOUT):
            for seq in range(window_base, next_to_send):
                if seq not in acknowledged_seqs:
                    sock.sendto(packet_store[seq], dest_addr)
            timer_started_at = time.time()

    # 4. Connection Termination
    end_pkt = build_packet(pkt_type=1, seq_num=total_packets + 1)
    sock.sendto(end_pkt, dest_addr)
    end_timer = time.time()

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
