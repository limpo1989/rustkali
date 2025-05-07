# TcpKali2 - High Performance Load Testing Tool

## Overview

TcpKali2 is a high-performance load testing tool designed for benchmarking TCP and WebSocket servers. Built with Rust
and Tokio, it delivers exceptional performance with minimal overhead, making it ideal for stress testing and performance
analysis.

## Features

* Multi-protocol support: Test both TCP and WebSocket (RFC6455) server
* Precise rate control: Accurate message/connection rate limiting
* Comprehensive metrics: Latency distribution, throughput, error rates
* Resource efficient: Low memory footprint, high connection density
* Flexible configuration: Custom messages, bandwidth limiting, variable duration

## Installation

```bash
cargo install --git https://github.com/limpo1989/tcpkali2
```

## Usage

Basic TCP echo test.

```bash
tcpkali2 -c 1000 -T 30s 127.0.0.1:8080
```

Basic Websocket echo test.

```bash
tcpkali2 --websocket -c 500 -r 10k ws://127.0.0.1:8080
```

```
# tcpkali2 --help
A load testing tool for WebSocket and TCP server

Usage: tcpkali2 [OPTIONS] <host:port>

Arguments:
  <host:port>  Target server in host:port format

Options:
      --websocket                    Use RFC6455 WebSocket transport
  -c, --connections <N>              Connections to keep open to the destinations [default: 1]
      --connect-rate <R>             Limit number of new connections per second [default: 100]
      --connect-timeout <T>          Limit time spent in a connection attempt [default: 1s]
      --channel-lifetime <T>         Shut down each connection after T seconds
  -w, --workers <N>                  Number of Tokio worker threads to use [default: 8]
      --nagle                        Control Nagle algorithm (set TCP_NODELAY)
  -p, --pipeline                     Use pipeline client to send messages
  -T, --duration <T>                 Load test for the specified amount of time [default: 15s]
  -e, --unescape-message-args        Unescape the following {-m|-f|--first-*} arguments
      --first-message <string>       Send this message first, once
      --first-message-file <name>    Read the first message from a file
  -m, --message <string>             Message to repeatedly send to the remote
  -s, --message-size <message-size>  Random message to repeatedly send to the remote [default: 128]
  -f, --message-file <name>          Read message to send from a file
  -r, --message-rate <R>             Messages per second to send in a connection
  -q                                 Suppress real-time output
  -h, --help                         Print help
  -V, --version                      Print version
```

## License

The repository released under version 2.0 of the Apache License.