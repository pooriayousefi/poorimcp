#include "poorimcp.hpp"

#include <chrono>
#include <iostream>
#include <print>
#include <string>
#include <thread>

using namespace pooriayousefi::mcp;

void print_test_header(const std::string& test_name)
{
    std::println("\n--- Running Test: {} ---", test_name);
}

bool test_tcp_echo()
{
    bool success = true;
    print_test_header("TCP Echo Tool (client → server → response)");

    ThreadPool pool{4};
    MCPServer server{9876};

    server.register_tool(
        MCPTool{"echo", "Echoes back the input", JSON(nullptr)},
        [](const JSON& args) -> AsyncTask<JSON>
        {
            co_return args;
        }
    );

    auto server_coro = [&server]() -> DetachedTask
    {
        co_await server.start();
        co_return;
    };

    auto dt = server_coro();
    auto h = dt.handle;
    dt.detach();
    pool.enqueue_raw([h]() { h.resume(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    MCPClient client(MCPTransport::create_tcp("127.0.0.1", 9876));

    auto client_task = [&client]() -> AsyncTask<bool>
    {
        bool result = false;

        std::println("  [client] connecting...");
        auto connect_result = co_await client.connect_async();
        std::println("  [client] connect result: {}", connect_result.has_value());

        if (connect_result.has_value())
        {
            std::println("  [client] initializing...");
            auto init_result = co_await client.initialize();
            std::println("  [client] init result: {}", init_result.has_value());
            if (init_result.has_value())
            {
                std::println("  [client] tools count: {}", client.get_tools().size());
            }
            else if (!init_result)
            {
                std::println("  [client] init error: {}", init_result.error());
            }

            if (init_result.has_value() && !client.get_tools().empty())
            {
                JSON args;
                args["message"] = "hello mcp!";
                std::println("  [client] calling echo tool...");
                auto call_result = co_await client.call_tool_async("echo", args);
                std::println("  [client] call result has_value: {}", call_result.has_value());
                if (call_result.has_value())
                {
                    std::println("  [client] response: {}", *call_result);
                    result = (*call_result == args.dump());
                }
            }
        }
        else if (!connect_result)
        {
            std::println("  [client] connect error: {}", connect_result.error().message());
        }

        co_return result;
    };

    auto fut = pool.run(client_task());
    bool result = fut.get();

    std::println("Echo round-trip: {} (expected true)", result);

    if (!result)
    {
        success = false;
    }

    server.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    return success;
}

bool test_tool_discovery()
{
    bool success = true;
    print_test_header("Tool Discovery (tools/list)");

    ThreadPool pool{4};
    MCPServer server{9877};

    server.register_tool(
        MCPTool{"calculator", "A simple calculator", JSON(nullptr)},
        [](const JSON& args) -> AsyncTask<JSON>
        {
            co_return args;
        }
    );

    server.register_tool(
        MCPTool{"greeter", "Greets someone", JSON(nullptr)},
        [](const JSON& args) -> AsyncTask<JSON>
        {
            co_return args;
        }
    );

    auto server_coro = [&server]() -> DetachedTask
    {
        co_await server.start();
        co_return;
    };

    auto dt = server_coro();
    auto h = dt.handle;
    dt.detach();
    pool.enqueue_raw([h]() { h.resume(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    MCPClient client(MCPTransport::create_tcp("127.0.0.1", 9877));

    auto client_task = [&client]() -> AsyncTask<bool>
    {
        bool result = false;

        auto connect_result = co_await client.connect_async();
        if (connect_result.has_value())
        {
            auto init_result = co_await client.initialize();
            if (init_result.has_value())
            {
                result = (client.get_tools().size() == 2);
            }
        }

        co_return result;
    };

    auto fut = pool.run(client_task());
    bool result = fut.get();

    std::println("Discovered {} tools (expected 2)", client.get_tools().size());

    if (!result)
    {
        success = false;
    }

    server.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    return success;
}

bool test_missing_tool_error()
{
    bool success = true;
    print_test_header("Missing Tool (error handling)");

    ThreadPool pool{4};
    MCPServer server{9878};

    server.register_tool(
        MCPTool{"real_tool", "A real tool", JSON(nullptr)},
        [](const JSON& args) -> AsyncTask<JSON>
        {
            co_return args;
        }
    );

    auto server_coro = [&server]() -> DetachedTask
    {
        co_await server.start();
        co_return;
    };

    auto dt = server_coro();
    auto h = dt.handle;
    dt.detach();
    pool.enqueue_raw([h]() { h.resume(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    MCPClient client(MCPTransport::create_tcp("127.0.0.1", 9878));

    auto client_task = [&client]() -> AsyncTask<bool>
    {
        bool result = false;

        auto connect_result = co_await client.connect_async();
        if (connect_result.has_value())
        {
            auto init_result = co_await client.initialize();
            if (init_result.has_value())
            {
                JSON args;
                auto call_result = co_await client.call_tool_async("nonexistent", args);
                // Should fail — tool not found.
                result = !call_result.has_value();
            }
        }

        co_return result;
    };

    auto fut = pool.run(client_task());
    bool result = fut.get();

    std::println("Missing tool error: {} (expected true)", result);

    if (!result)
    {
        success = false;
    }

    server.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    return success;
}

bool test_server_shutdown()
{
    bool success = true;
    print_test_header("Server Shutdown (stop + cleanup)");

    ThreadPool pool{4};
    MCPServer server{9879};

    server.register_tool(
        MCPTool{"test", "Test tool", JSON(nullptr)},
        [](const JSON& args) -> AsyncTask<JSON>
        {
            co_return args;
        }
    );

    auto server_coro = [&server]() -> DetachedTask
    {
        co_await server.start();
        co_return;
    };

    auto dt = server_coro();
    auto h = dt.handle;
    dt.detach();
    pool.enqueue_raw([h]() { h.resume(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Stop — should not crash or hang.
    server.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::println("Server stopped cleanly.");
    success = true;

    return success;
}

int main()
{
    int exit_code = EXIT_SUCCESS;

    try
    {
        bool all_passed = true;

        if (!test_tcp_echo())
        {
            all_passed = false;
        }
        if (!test_tool_discovery())
        {
            all_passed = false;
        }
        if (!test_missing_tool_error())
        {
            all_passed = false;
        }
        if (!test_server_shutdown())
        {
            all_passed = false;
        }

        std::println("\n==============================");
        if (all_passed)
        {
            std::println("ALL POORIMCP TESTS PASSED!");
        }
        else
        {
            std::println("SOME POORIMCP TESTS FAILED!");
            exit_code = EXIT_FAILURE;
        }
        std::println("==============================");
    }
    catch (const std::exception& e)
    {
        std::println("Exception occurred: {}", e.what());
        exit_code = EXIT_FAILURE;
    }

    return exit_code;
}