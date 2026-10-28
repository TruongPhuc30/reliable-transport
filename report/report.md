# CS 145 Project 4: Reliable Transport Protocol (RTP)

## Author and collaborators
### Author name
- Full Name: Cao Đặng Trường Phúc
- Student ID (MSV): 24020606
- Email: caodangtruongphuc3006@gmail.com

### Collaborators
None.

## Report
### 1. RTP-base Implementation
- **Protocol**: Reliable Transport Protocol running directly on top of UDP datagrams (`SOCK_DGRAM`).
- **Flow Control & Reliability**: Go-Back-N sliding window mechanism with packet-level cumulative ACKs.
- **Checksum Verification**: 32-bit CRC (`crc32`) computed over header with checksum set to 0 concatenated with payload. Corrupted packets failing checksum validation are dropped immediately without sending ACKs.
- **Out-of-order Buffering**: Packets arriving within the allowable window `[expected_seq_num, expected_seq_num + window_size)` are buffered. Consecutive in-order chunks are flushed to stdout in order.
- **Retransmission & Timeout**: A 500ms retransmission timer is maintained. The timer resets whenever the window advances. If the timer fires before the window advances, all unacknowledged packets currently in the window are retransmitted.
- **Handshake & Teardown**: Connection initiation via `START` packet (`seq_num=0`, acknowledged with `seq_num=1`). Clean termination via `END` packet with a 500ms grace period.

### 2. RTP-opt Implementation
- **Selective Acknowledgement (SACK / Individual ACK)**: Receiver acknowledges each received valid packet by echoing its exact sequence number (`seq_num`), avoiding unnecessary cumulative ACK ambiguities.
- **Selective Repeat Retransmission**: Sender tracks per-packet acknowledgement status in the sliding window. On 500ms window timeout, only packets that have not yet been acknowledged within the current window are retransmitted.

## Citations
- Python standard library documentation (`socket`, `select`, `binascii`).
- Scapy documentation for packet crafting (`Packet`, `IntField`).

## Grading notes (if any)
- Tested and verified passing with all four proxy error types (`0123`: bit jam/corruption, delay, packet reordering, packet drops) across various window sizes (1, 5, 64, 128).
- Scaffolding code for network proxy `test_scripts/proxy.py` has been strictly preserved without any modifications.

## Extra credit attempted (if any)
None.
