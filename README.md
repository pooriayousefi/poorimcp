
# PooriMCP

A production-grade, fully asynchronous C++23 implementation of the Model Context Protocol (MCP). It combines a zero-dependency JSON library, a JSON-RPC 2.0 framework, a cross-platform process manager, and a coroutine-based async thread pool into a single, cohesive library — all built from first principles without any external dependencies.

```cpp
#include "poorimcp.hpp"

using namespace pooriayousefi::mcp;

int main() {
    ThreadPool pool{4};

    // --- Server side (HTTP mode) ---
    MCPServer server{9876}; // Use MCPServer{} for STDIO mode
    server.register_tool(
        MCPTool{"echo", "Echoes back input", JSON(nullptr)},
        [](const JSON& args) -> AsyncTask<JSON> {
            co_return args;
        }
    );

    auto server_coro = [&server]() -> DetachedTask {
        co_await server.start();
        co_return;
    };
    auto dt = server_coro();
    auto h = dt.handle;
    dt.detach();
    pool.enqueue_raw([h]() { h.resume(); });

    // --- Client side ---
    MCPClient client(MCPTransport::create_http("127.0.0.1", 9876, "/mcp"));

    auto client_task = [&client]() -> AsyncTask<void> {
        co_await client.connect_async();
        co_await client.initialize();
        auto result = co_await client.call_tool_async("echo", JSON{{"msg", "hi"}});
        std::println("Result: {}", *result);
    };

    auto fut = pool.run(client_task());
    fut.get();
}
```

---

## Table of contents

- [PooriMCP](#poorimcp)
  - [Table of contents](#table-of-contents)
  - [Why PooriMCP?](#why-poorimcp)
  - [Architecture](#architecture)
  - [Features](#features)
    - [JSON Foundation (`poorijson.hpp`)](#json-foundation-poorijsonhpp)
    - [JSON-RPC 2.0 (`poorijsonrpc.hpp`)](#json-rpc-20-poorijsonrpchpp)
    - [Process Management (`process.hpp`)](#process-management-processhpp)
    - [Async I/O (`asyncore.hpp` + `io_thread_pool.hpp`)](#async-io-asyncorehpp--io_thread_poolhpp)
    - [HTTP Client (`poorimcp.hpp` — `AsyncHTTPClient`)](#http-client-poorimcphpp--asynchttpclient)
    - [MCP Layer (`poorimcp.hpp`)](#mcp-layer-poorimcphpp)
  - [Requirements](#requirements)
  - [Project Structure](#project-structure)
  - [Building](#building)
  - [Transport Protocols](#transport-protocols)
    - [1. STDIO](#1-stdio)
    - [2. HTTP (Streamable HTTP)](#2-http-streamable-http)
  - [Core Components](#core-components)
  - [Usage](#usage)
    - [Server: Registering Tools](#server-registering-tools)
    - [Server: Starting](#server-starting)
    - [Client: Connecting and Discovering Tools](#client-connecting-and-discovering-tools)
    - [Client: Calling Tools](#client-calling-tools)
    - [Transport: STDIO](#transport-stdio)
    - [Transport: HTTP](#transport-http)
  - [Comparison with Other C++ MCP Implementations](#comparison-with-other-c-mcp-implementations)
  - [Limitations and Gotchas](#limitations-and-gotchas)
  - [License](#license)

---

## Why PooriMCP?

Most MCP implementations are thin wrappers around existing JSON and HTTP libraries, bolted onto synchronous or callback-based I/O. PooriMCP is different — it is built **from the ground up** as a vertically integrated, coroutine-native C++23 stack:

| Layer | Typical MCP Library | PooriMCP |
|-------|-------------------|----------|
| JSON | `nlohmann/json` (exception-based, heap-heavy) | `poorijson` — `std::expected`-based, transparent hashmap, zero-allocation lookups |
| RPC | Manual `if/else` on JSON fields | `poorijsonrpc` — typed builders, `classify()`, `ErrorCode` enum |
| Process | `popen()` or `boost::process` | `process.hpp` — Cross-platform RAII (POSIX `fork` / Win `CreateProcessA`), no zombies |
| Async I/O | `boost::asio` or callbacks | `pooriasync` — C++23 coroutines, Standard Thread Pool with `run_blocking()` bridging |
| HTTP | `cpp-httplib` or `libcurl` | `AsyncHTTPClient` — built on native cross-platform sockets, no external deps |
| MCP | Glue layer | `poorimcp` — `MCPServer` (STDIO+HTTP), `MCPClient`, `MCPTransport`, `ToolHandler` |

**No `boost`. No `nlohmann`. No `asio`. No `libuv`. No `cpp-httplib`. No exceptions for control flow.**

---

## Architecture

```
┌────────────────────────────────────────────────┐
│                 poorimcp.hpp                   │
│  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ MCPServer   │  │ MCPClient               │  │
│  │ MCPTransport│  │   connect_async()       │  │
│  │ ToolHandler │  │   initialize()          │  │
│  │ AsyncHTTPCli│  │   call_tool_async()     │  │
│  └──────┬──────┘  └────────────┬────────────┘  │
│         │                      │               │
├─────────┼──────────────────────┼───────────────┤
│         ▼                      ▼               │
│  ┌──────────────────────────────────────────┐  │
│  │         io_thread_pool.hpp               │  │
│  │   ThreadPool (Standard C++ Threads)      │  │
│  │   run_blocking (Park coroutines for I/O) │  │
│  └──────────┬──────────────────┬────────────┘  │
│             │                  │               │
│  ┌──────────▼─────┐  ┌────────▼─────────┐      │
│  │  asyncore.hpp  │  │  process.hpp     │      │
│  │  AsyncTask<T>  │  │ Process (RAII)   │      │
│  │  DetachedTask  │  │ fork/CreateProc  │      │
│  │  Cancellation  │  │ read_stdout()    │      │
│  └────────────────┘  └──────────────────┘      │
│             │                                  │
│  ┌──────────▼──────────────────────────────┐   │
│  │          poorijson.hpp                  │   │
│  │  JSON (std::variant, transparent hash)  │   │
│  │  parse() / parse_lenient()              │   │
│  │  ┌─────────────────────────────────┐    │   │
│  │  │      poorijsonrpc.hpp           │    │   │
│  │  │  make_request/response/error    │    │   │
│  │  │  make_notification/batch        │    │   │
│  │  │  classify() / MessageType       │    │   │
│  │  └─────────────────────────────────┘    │   │
│  └─────────────────────────────────────────┘   │
└────────────────────────────────────────────────┘
```

---

## Features

### JSON Foundation (`poorijson.hpp`)
- **`std::expected<T, JSONError>`** — exception-free error handling for all parse operations
- **Transparent hash/eq** — `operator[]`, `at()`, `contains()`, `erase()` on objects use `std::string_view` without allocation
- **`std::to_chars` / `std::from_chars`** — locale-independent, shortest round-trippable number formatting
- **`parse_lenient()`** — repairs LLM-generated JSON (strips Markdown fences, closes unclosed containers via stack-based nesting)
- **`at()` methods** — checked element access returning `std::expected<std::reference_wrapper<JSON>, JSONError>`
- **Direct `std::string` serialization** — no `std::ostringstream` overhead

### JSON-RPC 2.0 (`poorijsonrpc.hpp`)
- **Spec-compliant builders** — `make_request()`, `make_response()`, `make_error()`, `make_notification()`, `make_batch()`
- **`ErrorCode` enum** — reserved error codes (-32700 to -32603), no manual `static_cast`
- **`classify()`** — structural classification of incoming messages (`REQUEST`, `NOTIFICATION`, `RESPONSE_SUCCESS`, `RESPONSE_ERROR`, `BATCH`, `INVALID`)
- **Move-by-value optimization** — `params`, `result`, `data` taken by value and moved (no unnecessary copies)

### Process Management (`process.hpp`)
- **Cross-platform RAII** — POSIX `fork`+`execvp` and Windows `CreateProcessA`
- **No zombies, no orphans** — destructor calls `terminate()` + `wait()` if child is still running
- **Safe `running()`** — POSIX: reaps zombies and updates exit code (no `ECHILD` on subsequent `wait()`)
- **Idempotent `wait()`** — returns cached exit code if already reaped
- **Noexcept `terminate()`** — safe in destructors, signal handlers, cleanup paths
- **Blocking I/O helpers** — `read_stdout()` and `write_stdin()` return `std::expected` and are designed to be wrapped inside `io_bound::run_blocking()`
- **`close_stdin()`** — signals EOF to child so programs like `cat` exit cleanly

### Async I/O (`asyncore.hpp` + `io_thread_pool.hpp`)
- **C++23 coroutines** — `AsyncTask<T>`, `AsyncGenerator<T>`, `FireAndForget`, `DetachedTask`
- **Standard Thread Pool** — Cross-platform `std::thread` and `std::mutex` implementation. Highly portable across Mac, Linux, and Windows.
- **`run_blocking()`** — Coroutine awaitable that safely parks the coroutine, executes a blocking OS call (like `::recv()` or `read_stdout()`) on a worker thread, and resumes the coroutine when data is ready. Eliminates the need for complex OS-specific reactors (epoll/IOCP).
- **`DetachedTask`** — starts suspended, auto-destroys frame on completion; no leaks, no UB
- **`CancellationToken`** — thread-safe, one-way cancel flag with `throw_if_cancelled()`
- **`MoveOnlyFunction`** — type-erased, move-only callable (no `std::function` overhead)

### HTTP Client (`poorimcp.hpp` — `AsyncHTTPClient`)
- **Built on Native Sockets** — no `cpp-httplib`, no `libcurl`, no external HTTP library
- **Coroutine-based** — requests are dispatched via `co_await pool.run_blocking(...)`
- **HTTP/1.1 parsing** — status line, headers, Content-Length, body extraction
- **SSE Streaming Support** — Natively parses `text/event-stream` responses, returning the first valid JSON-RPC payload found in the stream.

### MCP Layer (`poorimcp.hpp`)
- **Official transports only** — STDIO (subprocess + pipes) and HTTP (Streamable HTTP POST)
- **`MCPTransport`** — unified factory: `create_stdio()` / `create_http()`
- **`MCPServer`** — Supports both STDIO mode (default) and HTTP mode (takes port). Features tool registration, multi-request connection persistence, and HTTP request/response parsing.
- **`MCPClient`** — async connect, initialize, tool discovery, tool invocation
- **`ToolHandler`** — move-only, type-erased handler for `AsyncTask<JSON>(const JSON&)`
- **Transparent tool map** — zero-allocation tool lookup by `std::string_view`
- **`rpc::make_*` integration** — uses `poorijsonrpc` helpers for all JSON-RPC message construction
- **Graceful shutdown** — `CancellationToken` in server, `stop()` method

---

## Requirements

- **C++23** (`std::expected`, `std::coroutine`, `std::span`, `std::binary_semaphore`)
- Clang 16+ (macOS/Linux), GCC 13+ (Linux), MSVC 19.34+ (Windows)
- **Cross-Platform**: Mac, Linux, and Windows Native
- No external dependencies

## Project Structure

```text
poorimcp/
├── bin/
├── include/
│   ├── asyncore.hpp
│   ├── io_thread_pool.hpp
│   ├── poorijson.hpp
│   ├── poorijsonrpc.hpp
│   ├── process.hpp
│   └── poorimcp.hpp
├── src/
│   └── main.cpp
└── README.md
```

## Building

**Mac/Linux:**
```bash
mkdir -p bin
clang++ -std=c++23 -O3 -I include src/main.cpp -o bin/poorimcp_test
./bin/poorimcp_test
```

**Windows (PowerShell, MSVC):**
```powershell
if (-not (Test-Path bin)) { New-Item -ItemType Directory bin }
cl /std:c++23 /EHsc /I include src/main.cpp ws2_32.lib /out:bin\poorimcp_test.exe
.\bin\poorimcp_test.exe
```

---

## Transport Protocols

PooriMCP supports the two official MCP transports defined by the
[Model Context Protocol specification](https://modelcontextprotocol.io):

### 1. STDIO

The server runs as a subprocess. Communication uses newline-delimited
JSON-RPC over the child process's stdin/stdout pipes.

```cpp
auto transport = MCPTransport::create_stdio(
    "npx",
    {"-y", "@modelcontextprotocol/server-filesystem", "/tmp"}
);
MCPClient client(std::move(transport));
```

Internally: `Process` spawns the subprocess (`fork`+`execvp` on POSIX, `CreateProcessA` on Windows). `MCPClient` uses `co_await pool.run_blocking()` to safely wrap the blocking `read_stdout()` and `write_stdin()` calls without stalling the event loop.

### 2. HTTP (Streamable HTTP)

The server runs as an HTTP endpoint. Each JSON-RPC request is sent as
an HTTP `POST` and the response body is the JSON-RPC reply.

```cpp
auto transport = MCPTransport::create_http("localhost", 8931, "/mcp");
MCPClient client(std::move(transport));
```

Internally: `AsyncHTTPClient` builds an HTTP/1.1 request, sends it via native cross-platform sockets, and reads the response. It natively supports SSE (`text/event-stream`) responses to prevent connection hangs on streaming servers.

---

## Core Components

| Component | Header | Description |
|-----------|--------|-------------|
| `JSON` | `poorijson.hpp` | `std::variant`-backed JSON value with `std::expected` error handling |
| `parse()` / `parse_lenient()` | `poorijson.hpp` | Strict RFC 8259 parser + LLM-tolerant repair |
| `rpc::make_*` / `rpc::classify()` | `poorijsonrpc.hpp` | JSON-RPC 2.0 builders and message classifier |
| `Process` | `process.hpp` | Cross-platform RAII process lifecycle |
| `AsyncTask<T>` | `asyncore.hpp` | Lazy coroutine returning `T` |
| `DetachedTask` | `asyncore.hpp` | Lazy, auto-destroying coroutine for thread pools |
| `ThreadPool` | `io_thread_pool.hpp` | Standard C++ thread pool with coroutine bridging |
| `run_blocking()` | `io_thread_pool.hpp` | Parks coroutine during blocking I/O |
| `CancellationToken` | `asyncore.hpp` | Thread-safe cancel flag |
| `MoveOnlyFunction` | `asyncore.hpp` | Type-erased move-only callable |
| `AsyncHTTPClient` | `poorimcp.hpp` | HTTP/1.1 client built on native sockets |
| `MCPTransport` | `poorimcp.hpp` | Unified STDIO/HTTP transport |
| `MCPServer` | `poorimcp.hpp` | STDIO/HTTP MCP server with tool registration |
| `MCPClient` | `poorimcp.hpp` | Async MCP client with tool discovery |
| `ToolHandler` | `poorimcp.hpp` | Move-only type-erased tool handler |

---

## Usage

### Server: Registering Tools

```cpp
MCPServer server{9876}; // HTTP mode

server.register_tool(
    MCPTool{"calculator", "Performs arithmetic", JSON({{"type", "object"}})},
    [](const JSON& args) -> AsyncTask<JSON> {
        double a = args["a"].get_number();
        double b = args["b"].get_number();
        std::string op = args["op"].get_string();

        JSON result;
        if (op == "add")            { result["result"] = a + b; }
        else if (op == "subtract")  { result["result"] = a - b; }
        else                       { result["error"] = "unknown operation"; }

        co_return result;
    }
);
```

### Server: Starting

```cpp
ThreadPool pool{4};

auto server_coro = [&server]() -> DetachedTask {
    co_await server.start();
    co_return;
};
auto dt = server_coro();
auto h = dt.handle;
dt.detach();
pool.enqueue_raw([h]() { h.resume(); });
```

### Client: Connecting and Discovering Tools

```cpp
MCPClient client(MCPTransport::create_http("127.0.0.1", 9876, "/mcp"));

auto client_task = [&client]() -> AsyncTask<void> {
    co_await client.connect_async();
    co_await client.initialize();

    for (const auto& tool : client.get_tools()) {
        std::println("Discovered: {} — {}", tool.name, tool.description);
    }
};

auto fut = pool.run(client_task());
fut.get();
```

### Client: Calling Tools

```cpp
auto call_task = [&client]() -> AsyncTask<void> {
    JSON args;
    args["a"] = 10;
    args["b"] = 32;
    args["op"] = "add";

    auto result = co_await client.call_tool_async("calculator", args);
    if (result) {
        std::println("Result: {}", *result);
    }
};

auto fut = pool.run(call_task());
fut.get();
```

### Transport: STDIO

```cpp
auto transport = MCPTransport::create_stdio(
    "/usr/local/bin/node",
    {"/path/to/mcp-server.js", "--stdio"}
);
MCPClient client(std::move(transport));
```

### Transport: HTTP

```cpp
auto transport = MCPTransport::create_http("localhost", 8931, "/mcp");
MCPClient client(std::move(transport));
```

---

## Comparison with Other C++ MCP Implementations

| Feature | PooriMCP | Typical C++ MCP Libs |
|---------|----------|---------------------|
| **Dependencies** | **Zero** — no boost, no nlohmann, no asio, no cpp-httplib | Usually 2–5 deps (boost, nlohmann/json, asio, cpp-httplib, etc.) |
| **Async model** | **C++23 coroutines** — `co_await`, `AsyncTask<T>`, symmetric transfer | Callbacks, futures, or `boost::asio` handler chains |
| **Error handling** | **`std::expected<T, JSONError>`** — no exceptions for control flow | `std::optional`, exceptions, or raw error codes |
| **JSON** | **Custom** — transparent hashmap (zero-alloc lookups), `std::to_chars`, `parse_lenient()` | `nlohmann/json` — heap allocations, exception-based |
| **JSON-RPC** | **Typed builders** — `make_request()`, `classify()`, `ErrorCode` enum | Manual `json["method"] = ...` stringly-typed |
| **Process management** | **RAII Cross-platform** — no zombies, noexcept `terminate()`, `read_stdout()` | `popen()` (leaks, no stderr), or `boost::process` (heavy dep) |
| **HTTP client** | **`AsyncHTTPClient`** — built on native sockets, no external deps | `cpp-httplib` or `libcurl` (external dependency) |
| **Thread pool** | Standard C++ threads + `run_blocking()` coroutine bridge | `boost::asio` (powerful but ~500KB binary bloat) |
| **Coroutine lifetime** | **`DetachedTask`** — auto-destroy on completion, no leaks | Manual `handle.destroy()` or `std::suspend_always` + leaks |
| **Cancellation** | **`CancellationToken`** — thread-safe, `throw_if_cancelled()` | None, or ad-hoc `std::atomic<bool>` |
| **Type-erased handlers** | **`ToolHandler`** — move-only, type-safe | `std::function` (requires copyable, heap-allocates) |
| **Object lookups** | **Transparent hash/eq** — `std::string_view` lookup, zero allocation | `std::string` key allocation on every `obj["key"]` |
| **Buffer safety** | **`std::span<std::byte>`** — bounds-safe, zero-copy | Raw `char*` + size, or `std::vector` copies |
| **Header-only** | **Yes** — drop into `include/` and build | Usually compiled library + headers |
| **Binary size** | **Minimal** — only OS networking code linked | Large (boost, asio, nlohmann templates) |
| **Compile time** | **Fast** — no heavy template instantiations | Slow (boost/asio/nlohmann template explosion) |
| **Cross-platform** | **Mac/Linux/Windows** — unified API, `NOMINMAX`, `WIN32_LEAN_AND_MEAN` | Often POSIX-only or Windows-only |
| **LLM integration** | **`parse_lenient()`** — repairs unclosed containers, strips Markdown | Requires perfect JSON from the model |
| **Transport** | **Official spec only** — STDIO + HTTP (Streamable HTTP) | Often custom TCP or non-spec-compliant |
| **Server** | **STDIO + HTTP server** — parses HTTP requests, builds HTTP responses, or uses stdin/stdout | Often raw TCP or requires external HTTP server |
| **Connection persistence** | **Multi-request HTTP** — one connection, multiple requests | Often one-request-per-connection |
| **Server shutdown** | **`CancellationToken` + `stop()`** — clean, non-blocking | Often `kill` or `SIGTERM` + zombie |
| **Code conventions** | **Allman, single-return, smart pointers, no globals** | Mixed style, raw pointers, multiple returns |

---

## Limitations and Gotchas

1. **No TLS/SSL:** HTTP transport is plaintext. For production MCP over network, add TLS (OpenSSL or platform APIs).
2. **No chunked HTTP encoding:** `AsyncHTTPClient` uses `Connection: close` and reads until EOF. It supports SSE streams, but does not support HTTP/1.1 chunked transfer encoding.
3. **No streaming/SAX JSON:** The whole document must be in memory. For very large JSON payloads, a streaming parser would be needed.
4. **`sync_wait` deadlocks:** Never call `sync_wait()` inside a `ThreadPool` worker thread. It will block the worker, preventing the coroutines scheduled on that pool from ever resuming. Use `ThreadPool::run()` from the main thread instead.
5. **Coroutine frame heap allocation:** Each `co_await` creates a frame on the heap. For ultra-low-latency scenarios, a pooled allocator could be added.
6. **Object key order is unspecified:** `std::unordered_map` does not preserve insertion order. If key order matters for protocol compliance, switch to `std::map`.
7. **`ToolHandler` always heap-allocates:** No small-buffer-optimization (SBO). For high-frequency tool dispatch, consider a pooled allocator.

---

## License

Apache License 2.0

---

**Author:** Pooria Yousefi

---