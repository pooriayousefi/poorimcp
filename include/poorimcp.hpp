// ============================================================================
//  poorimcp.hpp — Async Model Context Protocol
//  Developed by: Pooria Yousefi
//  License: Apache 2.0
// ============================================================================
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <expected>
#include <variant>
#include <span>
#include <system_error>
#include <cstdint>

#include "asyncore.hpp"
#include "io_thread_pool.hpp"
#include "poorijson.hpp"
#include "poorijsonrpc.hpp"
#include "pooriprocess.hpp"

#if defined(_WIN32)
    #ifndef NOMINMAX
        #define NOMINMAX
    #endif
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <winsock2.h>
    #include <ws2tcpip.h>
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <unistd.h>
    #include <arpa/inet.h>
#endif

namespace pooriayousefi::mcp
{
    using namespace core;
    using namespace io_bound;
    using namespace json;
    using namespace process;
    using namespace rpc;

    /// @brief Describes an MCP tool's metadata and schema.
    struct MCPTool
    {
        std::string name{};
        std::string description{};
        JSON parameters_schema{};
    };

    /// @brief Type-erased, move-only handler for MCP tool invocations.
    ///
    /// Uses a Base/Model pattern with std::unique_ptr because AsyncTask<JSON>
    /// is move-only and cannot be stored in std::function.
    class ToolHandler
    {
        struct Base
        {
            virtual ~Base() = default;
            virtual AsyncTask<JSON> invoke(const JSON& args) = 0;
        };

        template <typename F>
        struct Model : Base
        {
            F func;

            explicit Model(F&& f) : func(std::move(f))
            {
            }

            AsyncTask<JSON> invoke(const JSON& args) override
            {
                return func(args);
            }
        };

        std::unique_ptr<Base> impl_;

    public:
        ToolHandler() = default;

        template <typename F>
            requires(!std::is_same_v<std::decay_t<F>, ToolHandler>)
        ToolHandler(F&& f)
            : impl_(std::make_unique<Model<std::decay_t<F>>>(std::forward<F>(f)))
        {
        }

        ToolHandler(ToolHandler&&) = default;
        ToolHandler& operator=(ToolHandler&&) = default;
        ToolHandler(const ToolHandler&) = delete;
        ToolHandler& operator=(const ToolHandler&) = delete;

        AsyncTask<JSON> operator()(const JSON& args)
        {
            if (impl_)
            {
                return impl_->invoke(args);
            }

            // Fallback for empty handler — should never happen in practice.
            auto fallback = []() -> AsyncTask<JSON>
            {
                JSON err;
                err["error"] = "empty tool handler";
                co_return err;
            };
            return fallback();
        }
    };

    /// @brief Awaitable that suspends until a socket becomes readable/writable.
    ///
    /// Public version of the awaitable pattern used by AsyncSocket and
    /// AsyncPipe. Needed by MCPServer which works with raw listen sockets.
    struct SocketAwaitable
    {
        NetworkReactor* reactor;
        socket_t fd;
        NetworkReactor::EventType type;

        bool await_ready() noexcept
        {
            return false;
        }

        void await_suspend(std::coroutine_handle<> h)
        {
            reactor->register_socket(
                fd,
                type,
                [h]()
                {
                    h.resume();
                }
            );
        }

        void await_resume() noexcept
        {
        }
    };

    // ----------------------------------------------------------------
    //  MCPTransport — unified TCP/stdio transport layer
    // ----------------------------------------------------------------

    class MCPTransport
    {
    public:
        enum class Type
        {
            TCP,
            STDIO
        };

    private:
        Type type_{Type::TCP};
        std::string host_{};
        int port_{0};
        std::string executable_{};
        std::vector<std::string> args_{};

        std::unique_ptr<Process> process_{};
        std::optional<AsyncSocket> socket_{};
        std::optional<AsyncPipe> pipe_{};

    public:
        MCPTransport() = default;

        MCPTransport(MCPTransport&& other) noexcept = default;
        MCPTransport& operator=(MCPTransport&& other) noexcept = default;

        MCPTransport(const MCPTransport&) = delete;
        MCPTransport& operator=(const MCPTransport&) = delete;

        static MCPTransport create_tcp(std::string host, int port)
        {
            MCPTransport t;
            t.type_ = Type::TCP;
            t.host_ = std::move(host);
            t.port_ = port;
            return t;
        }

        static MCPTransport create_stdio(std::string executable, std::vector<std::string> args)
        {
            MCPTransport t;
            t.type_ = Type::STDIO;
            t.executable_ = std::move(executable);
            t.args_ = std::move(args);
            return t;
        }

        AsyncTask<std::expected<void, std::error_code>> connect()
        {
            std::expected<void, std::error_code> result{};

            if (type_ == Type::TCP)
            {
                socket_.emplace(NetworkReactor::current);
                auto connect_result = co_await socket_->async_connect(host_, port_);
                if (!connect_result)
                {
                    result = std::unexpected(connect_result.error());
                }
            }
            else if (type_ == Type::STDIO)
            {
                try
                {
                    process_ = std::make_unique<Process>();
                    process_->start(executable_, args_);
                    pipe_.emplace(NetworkReactor::current);
                    pipe_->assign_write(process_->get_stdin_write());
                    pipe_->assign_read(process_->get_stdout_read());
                }
                catch (const std::exception&)
                {
                    result = std::unexpected(std::make_error_code(std::errc::no_such_process));
                }
            }

            co_return result;
        }

        AsyncTask<std::expected<std::size_t, std::error_code>> send(std::span<const std::byte> buf)
        {
            std::expected<std::size_t, std::error_code> result{
                std::unexpected(std::make_error_code(std::errc::operation_not_permitted))
            };

            if (type_ == Type::TCP && socket_)
            {
                result = co_await socket_->send(buf);
            }
            else if (type_ == Type::STDIO && pipe_)
            {
                result = co_await pipe_->send(buf);
            }

            co_return result;
        }

        AsyncTask<std::expected<std::size_t, std::error_code>> recv(std::span<std::byte> buf)
        {
            std::expected<std::size_t, std::error_code> result{
                std::unexpected(std::make_error_code(std::errc::operation_not_permitted))
            };

            if (type_ == Type::TCP && socket_)
            {
                result = co_await socket_->recv(buf);
            }
            else if (type_ == Type::STDIO && pipe_)
            {
                result = co_await pipe_->recv(buf);
            }

            co_return result;
        }
    };

    // ----------------------------------------------------------------
    //  MCPServer — async MCP server with tool registration
    // ----------------------------------------------------------------

    class MCPServer
    {
    public:
        using ToolMap = std::unordered_map<std::string, 
            std::pair<MCPTool, ToolHandler>, 
            JSONStringHash, 
            JSONStringEqual>;

    private:
        int port_{-1};
        ToolMap tools_{};
        socket_t listen_fd_{INVALID_SOCK};
        CancellationToken token_{};

    public:
        explicit MCPServer(int port)
            : port_{port}
        {
        }

        ~MCPServer()
        {
            stop();

            if (listen_fd_ != INVALID_SOCK)
            {
                close_socket_impl(listen_fd_);
                listen_fd_ = INVALID_SOCK;
            }
        }

        MCPServer(const MCPServer&) = delete;
        MCPServer& operator=(const MCPServer&) = delete;
        MCPServer(MCPServer&&) = delete;
        MCPServer& operator=(MCPServer&&) = delete;

        void register_tool(MCPTool tool, ToolHandler handler)
        {
            // Capture the key BEFORE moving — tool.name would be empty after move.
            std::string name = tool.name;
            tools_.try_emplace(std::move(name), std::move(tool), std::move(handler));
        }

        void stop() noexcept
        {
            token_.cancel();
        }

        AsyncTask<void> start()
        {
            NetworkReactor* reactor = NetworkReactor::current;

            if (!reactor)
            {
                std::cerr << "MCPServer started without a reactor context!\n"
                          << "Call start() from inside ThreadPool::run().\n";
            }
            else
            {
                listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
                if (listen_fd_ == INVALID_SOCK)
                {
                    // Cannot create socket — nothing to do.
                }
                else
                {
                    int opt = 1;
                    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR,
                               reinterpret_cast<const char*>(&opt), sizeof(opt));
                    set_nonblocking(listen_fd_);

                    struct sockaddr_in address{};
                    address.sin_family = AF_INET;
                    address.sin_addr.s_addr = INADDR_ANY;
                    address.sin_port = htons(static_cast<uint16_t>(port_));

                    if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&address),
                             sizeof(address)) < 0)
                    {
                        std::cerr << "Failed to bind to port " << port_ << "\n";
                    }
                    else if (listen(listen_fd_, 3) < 0)
                    {
                        std::cerr << "Failed to listen on socket\n";
                    }
                    else
                    {
                        std::cout << "MCP Server listening on port " << port_ << "...\n";

                        while (!token_.is_cancelled())
                        {
                            struct sockaddr_in client_addr{};
#if defined(_WIN32)
                            int addrlen = static_cast<int>(sizeof(client_addr));
#else
                            socklen_t addrlen = sizeof(client_addr);
#endif
                            socket_t client_fd = accept(listen_fd_,
                                reinterpret_cast<struct sockaddr*>(&client_addr), &addrlen);

                            if (client_fd == INVALID_SOCK)
                            {
                                co_await SocketAwaitable{
                                    reactor, listen_fd_,
                                    NetworkReactor::EventType::READABLE
                                };
                                continue;
                            }

                            // Spawn a DetachedTask per client — starts suspended,
                            // auto-destroys on completion. No leaks, no UB.
                            auto handler = [this, client_fd]() -> DetachedTask
                            {
                                co_await handle_client(client_fd);
                                co_return;
                            };

                            auto dt = handler();
                            auto h = dt.handle;
                            dt.detach();
                            h.resume();
                        }
                    }

                    close_socket_impl(listen_fd_);
                    listen_fd_ = INVALID_SOCK;
                }
            }

            co_return;
        }

    private:
        AsyncTask<void> handle_client(socket_t client_fd)
        {
            AsyncSocket sock;
            sock.assign(client_fd);

            bool connected = true;

            while (connected)
            {
                std::string buffer;
                char chunk[4096];

                bool done = false;

                while (!done)
                {
                    auto recv_result = co_await sock.recv(std::as_writable_bytes(std::span{chunk}));
                    if (!recv_result || *recv_result == 0)
                    {
                        done = true;
                        connected = false;
                    }
                    else
                    {
                        buffer.append(chunk, *recv_result);
                        if (buffer.find('\n') != std::string::npos)
                        {
                            done = true;
                        }
                    }
                }

                // Extract the JSON line — trim trailing \r if present.
                auto newline_pos = buffer.find('\n');
                if (newline_pos != std::string::npos)
                {
                    std::string_view json_sv{buffer.data(), newline_pos};
                    if (!json_sv.empty() && json_sv.back() == '\r')
                    {
                        json_sv.remove_suffix(1);
                    }

                    auto parsed_req = parse(json_sv);
                    if (parsed_req)
                    {
                        JSON resp = co_await handle_rpc(*parsed_req);
                        std::string resp_str = resp.dump() + "\n";
                        co_await sock.send(std::as_bytes(std::span{resp_str}));
                    }
                }
            }

            sock.close();
            co_return;
        }

        AsyncTask<JSON> handle_rpc(const JSON& req)
        {
            JSON resp{};

            if (!req.contains("id"))
            {
                resp = make_error(ErrorCode::INVALID_REQUEST,
                                  "Invalid Request: missing id", JSON(nullptr));
            }
            else
            {
                if (!req.contains("method"))
                {
                    resp = make_error(ErrorCode::INVALID_REQUEST,
                                      "Missing method", req["id"]);
                }
                else
                {
                    std::string method = req["method"].get_string();

                    if (method == "initialize")
                    {
                        resp = make_response(JSON({{"status", "ready"}}), req["id"]);
                    }
                    else if (method == "tools/list")
                    {
                        JSONArray tools_arr;
                        for (const auto& [name, pair] : tools_)
                        {
                            JSON tool_json;
                            tool_json["name"] = pair.first.name;
                            tool_json["description"] = pair.first.description;
                            tool_json["inputSchema"] = pair.first.parameters_schema;
                            tools_arr.push_back(tool_json);
                        }
                        JSON result_obj;
                        result_obj["tools"] = std::move(tools_arr);
                        resp = make_response(std::move(result_obj), req["id"]);
                    }
                    else if (method == "tools/call")
                    {
                        if (!req.contains("params") || !req["params"].contains("name"))
                        {
                            resp = make_error(ErrorCode::INVALID_PARAMS,
                                              "Missing tool name", req["id"]);
                        }
                        else
                        {
                            std::string tool_name = req["params"]["name"].get_string();
                            JSON args = req["params"].contains("arguments")
                                          ? req["params"]["arguments"]
                                          : JSON(nullptr);

                            auto it = tools_.find(tool_name);
                            if (it == tools_.end())
                            {
                                resp = make_error(ErrorCode::METHOD_NOT_FOUND,
                                                  "Tool not found", req["id"]);
                            }
                            else
                            {
                                try
                                {
                                    JSON tool_result = co_await it->second.second(args);
                                    JSON text_content;
                                    text_content["type"] = "text";
                                    text_content["text"] = tool_result.dump();
                                    JSONArray content_arr;
                                    content_arr.push_back(text_content);
                                    JSON result_obj;
                                    result_obj["content"] = std::move(content_arr);
                                    resp = make_response(std::move(result_obj), req["id"]);
                                }
                                catch (const std::exception& e)
                                {
                                    resp = make_error(ErrorCode::INTERNAL_ERROR,
                                                      e.what(), req["id"]);
                                }
                            }
                        }
                    }
                    else
                    {
                        resp = make_error(ErrorCode::METHOD_NOT_FOUND,
                                          "Method not found", req["id"]);
                    }
                }
            }

            co_return resp;
        }
    };

    // ----------------------------------------------------------------
    //  MCPClient — async MCP client with tool discovery
    // ----------------------------------------------------------------

    class MCPClient
    {
        MCPTransport transport_{};
        int request_id_{0};
        std::vector<MCPTool> discovered_tools_{};

    public:
        explicit MCPClient(MCPTransport transport)
            : transport_{std::move(transport)}
        {
        }

        MCPClient(const MCPClient&) = delete;
        MCPClient& operator=(const MCPClient&) = delete;
        MCPClient(MCPClient&&) = default;
        MCPClient& operator=(MCPClient&&) = default;

        AsyncTask<std::expected<void, std::error_code>> connect_async()
        {
            auto result = co_await transport_.connect();
            co_return result;
        }

        AsyncTask<std::expected<void, std::string>> initialize()
        {
            std::expected<void, std::string> result{};

            JSON init_params;
            init_params["protocolVersion"] = "2024-11-05";
            JSON init_req = make_request("initialize", init_params, request_id_++);

            auto init_resp = co_await send_rpc_async(init_req);
            if (!init_resp)
            {
                result = std::unexpected(init_resp.error());
            }
            else
            {
                JSON list_req = make_request("tools/list", JSON(nullptr), request_id_++);
                auto list_resp = co_await send_rpc_async(list_req);
                if (!list_resp)
                {
                    result = std::unexpected(list_resp.error());
                }
                else
                {
                    if (list_resp->contains("tools") && (*list_resp)["tools"].is_array())
                    {
                        for (const auto& tool_json : (*list_resp)["tools"].get_array())
                        {
                            MCPTool t;
                            t.name = tool_json["name"].get_string();
                            t.description = tool_json["description"].get_string();
                            t.parameters_schema = tool_json["inputSchema"];
                            discovered_tools_.push_back(t);
                        }
                    }
                    result = {};
                }
            }

            co_return result;
        }

        [[nodiscard]] const std::vector<MCPTool>& get_tools() const noexcept
        {
            return discovered_tools_;
        }

        AsyncTask<std::expected<std::string, std::string>> call_tool_async(
            const std::string& tool_name,
            const JSON& args
        )
        {
            std::expected<std::string, std::string> result{};

            JSON call_params;
            call_params["name"] = tool_name;
            call_params["arguments"] = args;
            JSON req = make_request("tools/call", call_params, request_id_++);

            auto resp = co_await send_rpc_async(req);
            if (!resp)
            {
                result = std::unexpected(resp.error());
            }
            else
            {
                if (resp->contains("content") && (*resp)["content"].is_array())
                {
                    const auto& content = (*resp)["content"].get_array();
                    if (!content.empty() && content[0].contains("text"))
                    {
                        result = content[0]["text"].get_string();
                    }
                    else
                    {
                        result = resp->dump();
                    }
                }
                else
                {
                    result = resp->dump();
                }
            }

            co_return result;
        }

    private:
        AsyncTask<std::expected<JSON, std::string>> send_rpc_async(const JSON& request)
        {
            std::expected<JSON, std::string> result{};

            std::string req_str = request.dump() + "\n";
            auto send_res = co_await transport_.send(std::as_bytes(std::span{req_str}));
            if (!send_res)
            {
                result = std::unexpected("Failed to send RPC request");
            }
            else
            {
                std::string buffer;
                char chunk[4096];
                bool done = false;

                while (!done)
                {
                    auto recv_res = co_await transport_.recv(std::as_writable_bytes(std::span{chunk}));
                    if (!recv_res || *recv_res == 0)
                    {
                        done = true;
                    }
                    else
                    {
                        buffer.append(chunk, *recv_res);
                        if (buffer.find('\n') != std::string::npos)
                        {
                            done = true;
                        }
                    }
                }

                auto newline_pos = buffer.find('\n');
                if (newline_pos == std::string::npos)
                {
                    result = std::unexpected("No newline in response");
                }
                else
                {
                    std::string_view json_sv{buffer.data(), newline_pos};
                    if (!json_sv.empty() && json_sv.back() == '\r')
                    {
                        json_sv.remove_suffix(1);
                    }

                    auto parsed = parse(json_sv);
                    if (!parsed)
                    {
                        result = std::unexpected("Invalid JSON from MCP Server");
                    }
                    else if (parsed->contains("error") && (*parsed)["error"].contains("message"))
                    {
                        result = std::unexpected((*parsed)["error"]["message"].get_string());
                    }
                    else
                    {
                        result = (*parsed)["result"];
                    }
                }
            }

            co_return result;
        }
    };
}