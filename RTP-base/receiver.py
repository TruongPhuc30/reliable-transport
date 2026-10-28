import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16


def build_ack_packet(seq_num):
    """Create an ACK packet with the given sequence number."""
    header = PacketHeader(type=3, seq_num=seq_num, length=0, checksum=0)
    header.checksum = compute_checksum(header / b"")
    return bytes(header / b"")


def unpack_and_verify(raw_bytes):
    """
    Validate the packet structure and 32-bit CRC checksum.
    Returns (header, payload) if valid, or (None, None) if corrupted or incomplete.
    """
    if len(raw_bytes) < HEADER_SIZE:
        return None, None

    header = PacketHeader(raw_bytes[:HEADER_SIZE])
    payload = raw_bytes[HEADER_SIZE : HEADER_SIZE + header.length]

    # Verify length consistency
    if len(payload) != header.length:
        return None, None

    # Calculate checksum with checksum field zeroed
    received_checksum = header.checksum
    header.checksum = 0
    expected_checksum = compute_checksum(header / payload)

    if received_checksum != expected_checksum:
        return None, None

    return header, payload


def receiver(receiver_ip, receiver_port, window_size):
    """
    RTP-base Receiver:
    - Verifies checksums for all incoming packets.
    - Buffers out-of-order packets within [expected_seq, expected_seq + window_size).
    - Writes contiguous in-order data to sys.stdout.
    - Sends cumulative ACKs for the next expected packet.
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
            # Corrupted packet or invalid length: drop without sending ACK
            continue

        pkt_type = header.type
        seq_num = header.seq_num

        if pkt_type == 0:  # START packet
            # Sender starts connection with seq_num = 0
            # Next expected data seq_num is 1
            connected = True
            ack_pkt = build_ack_packet(1)
            sock.sendto(ack_pkt, sender_addr)

        elif pkt_type == 2:  # DATA packet
            if not connected:
                # Ignore data packets if handshake hasn't taken place
                continue

            # Drop packets beyond the allowed sliding window
            if seq_num >= expected_seq_num + window_size:
                continue

            if seq_num < expected_seq_num:
                # Duplicate packet already acknowledged: re-send cumulative ACK
                ack_pkt = build_ack_packet(expected_seq_num)
                sock.sendto(ack_pkt, sender_addr)
            else:
                # Buffer the received packet (handles in-order or out-of-order)
                if seq_num not in packet_buffer:
                    packet_buffer[seq_num] = payload

                # Flush all consecutive in-order packets to stdout
                while expected_seq_num in packet_buffer:
                    chunk = packet_buffer.pop(expected_seq_num)
                    sys.stdout.buffer.write(chunk)
                    sys.stdout.flush()
                    expected_seq_num += 1

                # Send cumulative ACK for the next expected sequence number
                ack_pkt = build_ack_packet(expected_seq_num)
                sock.sendto(ack_pkt, sender_addr)

        elif pkt_type == 1:  # END packet
            # Flush any remaining buffer if applicable
            while expected_seq_num in packet_buffer:
                chunk = packet_buffer.pop(expected_seq_num)
                sys.stdout.buffer.write(chunk)
                sys.stdout.flush()
                expected_seq_num += 1

            # Acknowledge the END packet and exit cleanly
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
