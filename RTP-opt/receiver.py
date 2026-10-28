import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16


def build_ack_packet(seq_num):
    """Create an individual ACK packet for a specific sequence number."""
    header = PacketHeader(type=3, seq_num=seq_num, length=0, checksum=0)
    header.checksum = compute_checksum(header / b"")
    return bytes(header / b"")


def unpack_and_verify(raw_bytes):
    """Validate incoming packet structure and 32-bit CRC checksum."""
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


def receiver(receiver_ip, receiver_port, window_size):
    """
    RTP-opt Receiver (Selective ACK):
    - Drops corrupted packets (checksum mismatch).
    - Drops packets with seq_num >= expected_seq_num + window_size.
    - Sends individual ACK echoing the exact seq_num of each valid data packet.
    - Buffers out-of-order packets and delivers data sequentially to stdout.
    - Terminates cleanly upon acknowledging the END packet.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((receiver_ip, receiver_port))

    expected_seq_num = 1
    packet_buffer = {}  # seq_num -> payload bytes
    connected = False
    connection_ended = False

    while not connection_ended:
        try:
            raw_pkt, sender_addr = sock.recvfrom(2048)
        except socket.error:
            continue

        header, payload = unpack_and_verify(raw_pkt)
        if header is None:
            # Corrupted packet: drop without sending ACK
            continue

        pkt_type = header.type
        seq_num = header.seq_num

        if pkt_type == 0:  # START packet
            connected = True
            ack_pkt = build_ack_packet(1)
            sock.sendto(ack_pkt, sender_addr)

        elif pkt_type == 2:  # DATA packet
            if not connected:
                continue

            # Drop packets beyond the allowed sliding window
            if seq_num >= expected_seq_num + window_size:
                continue

            # In RTP-opt: send an individual ACK for this exact packet
            ack_pkt = build_ack_packet(seq_num)
            sock.sendto(ack_pkt, sender_addr)

            # Store in buffer if it is a new packet (seq_num >= expected)
            if seq_num >= expected_seq_num and seq_num not in packet_buffer:
                packet_buffer[seq_num] = payload

            # Flush all contiguous in-order packets to stdout
            while expected_seq_num in packet_buffer:
                chunk = packet_buffer.pop(expected_seq_num)
                sys.stdout.buffer.write(chunk)
                sys.stdout.flush()
                expected_seq_num += 1

        elif pkt_type == 1:  # END packet
            while expected_seq_num in packet_buffer:
                chunk = packet_buffer.pop(expected_seq_num)
                sys.stdout.buffer.write(chunk)
                sys.stdout.flush()
                expected_seq_num += 1

            ack_pkt = build_ack_packet(seq_num)
            sock.sendto(ack_pkt, sender_addr)
            connection_ended = True

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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
