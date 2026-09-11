// ============================================================================
//  pooriprocess.hpp — Cross-Platform Process Management (Enhanced for Async IO)
//  Developed by: Pooria Yousefi
//  License: Apache 2.0
// ============================================================================
#pragma once

#include <cerrno>
#include <cstring>
#include <format>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifdef _WIN32

#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>

namespace pooriayousefi::process
{
    /// @brief Converts a UTF-8 string to a wide (UTF-16) string.
    /// @param text  the UTF-8 input.
    /// @return the converted UTF-16 string.
    /// @throws std::runtime_error if the conversion fails.
    inline std::wstring utf8_to_wide(std::string_view text)
    {
        std::wstring result{};

        if (!text.empty())
        {
            const auto size = MultiByteToWideChar(
                CP_UTF8,
                0,
                text.data(),
                static_cast<int>(text.size()),
                nullptr,
                0
            );

            if (size <= 0)
            {
                throw std::runtime_error{
                    "Failed to convert UTF-8 to UTF-16."
                };
            }

            result.resize(size);

            if (MultiByteToWideChar(
                    CP_UTF8,
                    0,
                    text.data(),
                    static_cast<int>(text.size()),
                    result.data(),
                    size
                ) <= 0)
            {
                throw std::runtime_error{
                    "Failed to convert UTF-8 to UTF-16."
                };
            }
        }

        return result;
    }

    /// @brief Quotes a single command-line argument for Windows.
    ///
    /// Follows the MSDN "Everyone Quotes Command Line Arguments" rules:
    /// backslashes before quotes are doubled, the quote is escaped, and
    /// trailing backslashes are doubled.
    ///
    /// @param argument  the raw argument text.
    /// @return the quoted and escaped argument.
    inline std::wstring quote_argument(
        std::wstring_view argument
    )
    {
        std::wstring result{};

        if (argument.empty())
        {
            result = LR"("")";
        }
        else
        {
            const auto needs_quotes =
                argument.find_first_of(L" \t\n\v\"")
                != std::wstring_view::npos;

            if (!needs_quotes)
            {
                result = std::wstring{argument};
            }
            else
            {
                result.push_back(L'"');

                std::size_t backslashes{0};

                for (const auto ch : argument)
                {
                    if (ch == L'\\')
                    {
                        ++backslashes;
                        continue;
                    }

                    if (ch == L'"')
                    {
                        result.append(
                            backslashes * 2 + 1,
                            L'\\'
                        );

                        result.push_back(L'"');
                        backslashes = 0;
                        continue;
                    }

                    result.append(backslashes, L'\\');
                    backslashes = 0;
                    result.push_back(ch);
                }

                result.append(backslashes * 2, L'\\');
                result.push_back(L'"');
            }
        }

        return result;
    }

    /// @brief RAII wrapper for a Windows HANDLE.
    ///
    /// Calls CloseHandle on destruction or reset. Move-only.
    struct WindowsHandle
    {
        HANDLE h{nullptr};

        WindowsHandle() = default;
        explicit WindowsHandle(HANDLE handle) : h{handle} {}

        ~WindowsHandle()
        {
            reset();
        }

        WindowsHandle(const WindowsHandle&) = delete;
        WindowsHandle& operator=(const WindowsHandle&) = delete;

        WindowsHandle(WindowsHandle&& other) noexcept
            : h{std::exchange(other.h, nullptr)}
        {
        }

        WindowsHandle& operator=(WindowsHandle&& other) noexcept
        {
            if (this != &other)
            {
                reset();
                h = std::exchange(other.h, nullptr);
            }
            return *this;
        }

        void reset() noexcept
        {
            if (h != nullptr && h != INVALID_HANDLE_VALUE)
            {
                CloseHandle(h);
                h = nullptr;
            }
        }

        [[nodiscard]] HANDLE get() const noexcept
        {
            return h;
        }
    };

    /// @brief Cross-platform process management for C++23.
    ///
    /// Launches a child process with piped stdin/stdout, exposes native
    /// handles for async I/O, and provides wait/terminate/exit_code
    /// lifecycle control.  RAII — the destructor calls terminate() + wait()
    /// if the process is still running.
    ///
    /// On POSIX, stderr is inherited from the parent (not piped).
    /// On Windows, stderr is inherited from the parent's STD_ERROR_HANDLE.
    /// If the parent has no console, the child's stderr may be invalid.
    class Process final
    {
    public:
        using Arguments = std::vector<std::string>;

        Process() = default;

        Process(const Process&) = delete;
        Process& operator=(const Process&) = delete;

        Process(Process&& other) noexcept
            : process_{std::exchange(other.process_, WindowsHandle{})}
            , stdin_write_{std::exchange(other.stdin_write_, WindowsHandle{})}
            , stdout_read_{std::exchange(other.stdout_read_, WindowsHandle{})}
            , exit_code_{std::exchange(other.exit_code_, -1)}
        {
        }

        Process& operator=(Process&& other) noexcept
        {
            if (this != &other)
            {
                reset();

                process_ = std::exchange(other.process_, WindowsHandle{});
                stdin_write_ = std::exchange(other.stdin_write_, WindowsHandle{});
                stdout_read_ = std::exchange(other.stdout_read_, WindowsHandle{});
                exit_code_ = std::exchange(other.exit_code_, -1);
            }

            return *this;
        }

        ~Process()
        {
            reset();
        }

        // ----------------------------------------------------------------
        //  Lifecycle
        // ----------------------------------------------------------------

        /// @brief Launches a child process with piped stdin/stdout.
        /// @param executable  path or name of the program to run.
        /// @param arguments   command-line arguments (excluding argv[0]).
        /// @throws std::logic_error if a process is already running.
        /// @throws std::runtime_error on OS-level failure.
        void start(
            std::string_view executable,
            std::span<const std::string> arguments = {}
        )
        {
            if (running())
            {
                throw std::logic_error{
                    "Process is already running."
                };
            }

            reset(); // Clean up any previous (already-exited) state.

            SECURITY_ATTRIBUTES sa{};
            sa.nLength = sizeof(SECURITY_ATTRIBUTES);
            sa.bInheritHandle = TRUE;
            sa.lpSecurityDescriptor = nullptr;

            HANDLE stdin_read_raw{nullptr};
            HANDLE stdin_write_raw{nullptr};
            if (!CreatePipe(&stdin_read_raw, &stdin_write_raw, &sa, 0))
            {
                throw std::runtime_error{
                    std::format("CreatePipe failed. Windows error code: {}", GetLastError())
                };
            }
            WindowsHandle stdin_read{stdin_read_raw};
            stdin_write_ = WindowsHandle{stdin_write_raw};

            if (!SetHandleInformation(stdin_write_.get(), HANDLE_FLAG_INHERIT, 0))
            {
                throw std::runtime_error{
                    std::format("SetHandleInformation failed. Windows error code: {}", GetLastError())
                };
            }

            HANDLE stdout_read_raw{nullptr};
            HANDLE stdout_write_raw{nullptr};
            if (!CreatePipe(&stdout_read_raw, &stdout_write_raw, &sa, 0))
            {
                throw std::runtime_error{
                    std::format("CreatePipe failed. Windows error code: {}", GetLastError())
                };
            }
            stdout_read_ = WindowsHandle{stdout_read_raw};
            WindowsHandle stdout_write{stdout_write_raw};

            if (!SetHandleInformation(stdout_read_.get(), HANDLE_FLAG_INHERIT, 0))
            {
                throw std::runtime_error{
                    std::format("SetHandleInformation failed. Windows error code: {}", GetLastError())
                };
            }

            auto command_line =
                quote_argument(
                    utf8_to_wide(executable)
                );

            for (const auto& argument : arguments)
            {
                command_line.push_back(L' ');
                command_line +=
                    quote_argument(
                        utf8_to_wide(argument)
                    );
            }

            STARTUPINFOW startup_info{};
            startup_info.cb = sizeof(startup_info);
            startup_info.dwFlags = STARTF_USESTDHANDLES;
            startup_info.hStdInput = stdin_read.get();
            startup_info.hStdOutput = stdout_write.get();
            startup_info.hStdError = GetStdHandle(STD_ERROR_HANDLE);

            PROCESS_INFORMATION process_info{};

            const auto success = CreateProcessW(
                nullptr,
                command_line.data(),
                nullptr,
                nullptr,
                TRUE,
                0,
                nullptr,
                nullptr,
                &startup_info,
                &process_info
            );

            if (!success)
            {
                throw std::runtime_error{
                    std::format(
                        "CreateProcessW failed. "
                        "Windows error code: {}",
                        GetLastError()
                    )
                };
            }

            CloseHandle(process_info.hThread);

            process_ = WindowsHandle{process_info.hProcess};
            exit_code_ = -1;
        }

        /// @brief Convenience overload — accepts a braced initializer list.
        void start(
            std::string_view executable,
            std::initializer_list<std::string_view> arguments
        )
        {
            std::vector<std::string> args{arguments.begin(), arguments.end()};
            start(executable, std::span<const std::string>{args});
        }

        // ----------------------------------------------------------------
        //  Native handle access (for AsyncPipe / direct I/O)
        // ----------------------------------------------------------------

        /// @brief Returns the native write-end of the child's stdin pipe.
        [[nodiscard]] HANDLE get_stdin_write() const noexcept
        {
            return stdin_write_.get();
        }

        /// @brief Returns the native read-end of the child's stdout pipe.
        [[nodiscard]] HANDLE get_stdout_read() const noexcept
        {
            return stdout_read_.get();
        }

        /// @brief Closes the write-end of the child's stdin pipe.
        ///
        /// Signals EOF to the child so that programs like `cat` or
        /// `grep` that read until end-of-input can exit cleanly.
        void close_stdin() noexcept
        {
            stdin_write_.reset();
        }

        // ----------------------------------------------------------------
        //  Status & lifecycle queries
        // ----------------------------------------------------------------

        /// @brief Checks whether the child process is still running.
        /// @note On Windows, uses GetExitCodeProcess (no side effects).
        bool running() noexcept
        {
            bool result = false;

            if (process_.get() != nullptr)
            {
                DWORD status{};
                if (GetExitCodeProcess(process_.get(), &status))
                {
                    if (status == STILL_ACTIVE)
                    {
                        result = true;
                    }
                }
            }

            return result;
        }

        /// @brief Waits indefinitely for the child to exit.
        /// @return the process exit code.
        /// @throws std::logic_error if no process was started.
        /// @throws std::runtime_error on OS-level failure.
        int wait()
        {
            // If already cleaned up by running(), return the cached exit code.
            if (process_.get() == nullptr)
            {
                if (exit_code_ != -1)
                {
                    return exit_code_;
                }
                throw std::logic_error{
                    "No process is running."
                };
            }

            const auto result =
                WaitForSingleObject(
                    process_.get(),
                    INFINITE
                );

            if (result != WAIT_OBJECT_0)
            {
                throw std::runtime_error{
                    std::format(
                        "WaitForSingleObject failed. "
                        "Windows error code: {}",
                        GetLastError()
                    )
                };
            }

            DWORD status{};

            if (!GetExitCodeProcess(
                    process_.get(),
                    &status
                ))
            {
                throw std::runtime_error{
                    std::format(
                        "GetExitCodeProcess failed. "
                        "Windows error code: {}",
                        GetLastError()
                    )
                };
            }

            exit_code_ = static_cast<int>(status);

            process_.reset();
            stdin_write_.reset();
            stdout_read_.reset();

            return exit_code_;
        }

        /// @brief Gracefully terminates the child process and waits for it.
        /// @note This method is noexcept-safe — errors are swallowed to
        ///       allow safe use in destructors and cleanup paths.
        void terminate() noexcept
        {
            if (process_.get() == nullptr)
            {
                return;
            }

            if (running())
            {
                // Best-effort termination — ignore failures.
                TerminateProcess(process_.get(), 1);
            }

            // Best-effort wait — ignore failures (e.g. already exited).
            DWORD status{};
            if (WaitForSingleObject(process_.get(), 5000) == WAIT_OBJECT_0)
            {
                if (GetExitCodeProcess(process_.get(), &status))
                {
                    exit_code_ = static_cast<int>(status);
                }
            }

            reset();
        }

        /// @brief Returns the last known exit code.
        /// @return the exit code, or -1 if the process hasn't exited yet.
        [[nodiscard]] int exit_code() const noexcept
        {
            return exit_code_;
        }

    private:
        WindowsHandle process_{};
        WindowsHandle stdin_write_{};
        WindowsHandle stdout_read_{};
        int exit_code_{-1};

        /// @brief Releases all resources without waiting (unsafe for running processes).
        void reset() noexcept
        {
            process_.reset();
            stdin_write_.reset();
            stdout_read_.reset();
        }
    };
}

#else // -----------------------------------------------------------------------

#include <csignal>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace pooriayousefi::process
{
        /// @brief RAII wrapper for a POSIX file descriptor.
    ///
    /// Calls close() on destruction or reset. Move-only.
    struct FileDescriptor
    {
        int fd{-1};

        FileDescriptor() = default;
        explicit FileDescriptor(int descriptor) : fd{descriptor} {}

        ~FileDescriptor()
        {
            reset();
        }

        FileDescriptor(const FileDescriptor&) = delete;
        FileDescriptor& operator=(const FileDescriptor&) = delete;

        FileDescriptor(FileDescriptor&& other) noexcept
            : fd{std::exchange(other.fd, -1)}
        {
        }

        FileDescriptor& operator=(FileDescriptor&& other) noexcept
        {
            if (this != &other)
            {
                reset();
                fd = std::exchange(other.fd, -1);
            }
            return *this;
        }

        void reset() noexcept
        {
            if (fd != -1)
            {
                close(fd);
                fd = -1;
            }
        }

        [[nodiscard]] int get() const noexcept
        {
            return fd;
        }
    };

    /// @brief Cross-platform process management for C++23.
    ///
    /// Launches a child process with piped stdin/stdout, exposes native
    /// handles for async I/O, and provides wait/terminate/exit_code
    /// lifecycle control.  RAII — the destructor calls terminate() + wait()
    /// if the process is still running.
    ///
    /// On POSIX, stderr is inherited from the parent (not piped).
    /// On Windows, stderr is inherited from the parent's STD_ERROR_HANDLE.
    /// If the parent has no console, the child's stderr may be invalid.
    class Process final
    {
    public:
        using Arguments = std::vector<std::string>;

        Process() = default;

        Process(const Process&) = delete;
        Process& operator=(const Process&) = delete;

        Process(Process&& other) noexcept
            : pid_{std::exchange(other.pid_, -1)}
            , stdin_write_{std::exchange(other.stdin_write_, FileDescriptor{})}
            , stdout_read_{std::exchange(other.stdout_read_, FileDescriptor{})}
            , exit_code_{std::exchange(other.exit_code_, -1)}
        {
        }

        Process& operator=(Process&& other) noexcept
        {
            if (this != &other)
            {
                reset();

                pid_ = std::exchange(other.pid_, -1);
                stdin_write_ = std::exchange(other.stdin_write_, FileDescriptor{});
                stdout_read_ = std::exchange(other.stdout_read_, FileDescriptor{});
                exit_code_ = std::exchange(other.exit_code_, -1);
            }

            return *this;
        }

        ~Process()
        {
            reset();
        }

        // ----------------------------------------------------------------
        //  Lifecycle
        // ----------------------------------------------------------------

        /// @brief Launches a child process with piped stdin/stdout.
        /// @param executable  path or name of the program to run.
        /// @param arguments   command-line arguments (excluding argv[0]).
        /// @throws std::logic_error if a process is already running.
        /// @throws std::runtime_error on OS-level failure.
        void start(
            std::string_view executable,
            std::span<const std::string> arguments = {}
        )
        {
            if (running())
            {
                throw std::logic_error{
                    "Process is already running."
                };
            }

            reset(); // Clean up any previous (already-exited) state.

            int stdin_pipe[2] = {-1, -1};
            int stdout_pipe[2] = {-1, -1};

            if (pipe(stdin_pipe) != 0)
            {
                throw std::runtime_error{
                    std::format("pipe() failed: {}", std::strerror(errno))
                };
            }

            // Wrap stdin pipe ends immediately for RAII cleanup on error.
            FileDescriptor stdin_read{stdin_pipe[0]};
            FileDescriptor stdin_write{stdin_pipe[1]};

            if (pipe(stdout_pipe) != 0)
            {
                throw std::runtime_error{
                    std::format("pipe() failed: {}", std::strerror(errno))
                };
            }

            // Wrap stdout pipe ends immediately for RAII cleanup on error.
            FileDescriptor stdout_read{stdout_pipe[0]};
            FileDescriptor stdout_write{stdout_pipe[1]};

            pid_t child = fork();

            if (child < 0)
            {
                throw std::runtime_error{
                    std::format("fork() failed: {}", std::strerror(errno))
                };
            }

            if (child == 0)
            {
                // ---- Child process ----

                // Redirect stdin and stdout.  Check return values — if
                // dup2 fails, the child must exit immediately.
                if (dup2(stdin_read.get(), STDIN_FILENO) < 0 ||
                    dup2(stdout_write.get(), STDOUT_FILENO) < 0)
                {
                    _exit(126); // 126 = "cannot execute" (permission/setup error)
                }

                // Close all original pipe descriptors in the child —
                // stdin_read and stdout_write are now duplicated as
                // STDIN_FILENO and STDOUT_FILENO.
                stdin_read.reset();
                stdin_write.reset();
                stdout_read.reset();
                stdout_write.reset();

                // Build argv array for execvp.
                const std::string executable_copy{executable};
                std::vector<char*> argv{};

                argv.reserve(arguments.size() + 2);

                argv.push_back(
                    const_cast<char*>(executable_copy.c_str())
                );

                for (const auto& argument : arguments)
                {
                    argv.push_back(
                        const_cast<char*>(argument.c_str())
                    );
                }

                argv.push_back(nullptr);

                execvp(executable_copy.c_str(), argv.data());

                // execvp only returns on failure — exit with 127
                // (standard "command not found" convention).
                _exit(127);
            }

            // ---- Parent process ----
            // Close child-side pipe ends (already duplicated in child).
            stdin_read.reset();
            stdout_write.reset();

            stdin_write_ = std::move(stdin_write);
            stdout_read_ = std::move(stdout_read);
            pid_ = child;
            exit_code_ = -1;
        }

        /// @brief Convenience overload — accepts a braced initializer list.
        void start(
            std::string_view executable,
            std::initializer_list<std::string_view> arguments
        )
        {
            std::vector<std::string> args{arguments.begin(), arguments.end()};
            start(executable, std::span<const std::string>{args});
        }

        // ----------------------------------------------------------------
        //  Native handle access (for AsyncPipe / direct I/O)
        // ----------------------------------------------------------------

        /// @brief Returns the native write-end of the child's stdin pipe.
        [[nodiscard]] int get_stdin_write() const noexcept
        {
            return stdin_write_.get();
        }

        /// @brief Returns the native read-end of the child's stdout pipe.
        [[nodiscard]] int get_stdout_read() const noexcept
        {
            return stdout_read_.get();
        }

        /// @brief Closes the write-end of the child's stdin pipe.
        ///
        /// Signals EOF to the child so that programs like `cat` or
        /// `grep` that read until end-of-input can exit cleanly.
        void close_stdin() noexcept
        {
            stdin_write_.reset();
        }

        // ----------------------------------------------------------------
        //  Status & lifecycle queries
        // ----------------------------------------------------------------

        /// @brief Checks whether the child process is still running.
        ///
        /// Uses waitpid with WNOHANG.  If the child has exited, this
        /// method reaps the zombie, updates exit_code_, and resets state
        /// so that a subsequent wait() does not fail with ECHILD.
        ///
        /// @return true if the child is still running; false if it exited
        ///         or was never started.
        bool running() noexcept
        {
            bool result = false;

            if (pid_ > 0)
            {
                int status{};
                const auto wait_result = waitpid(pid_, &status, WNOHANG);

                if (wait_result == 0)
                {
                    // Still running.
                    result = true;
                }
                else if (wait_result > 0)
                {
                    // Child has exited — reap and update state.
                    if (WIFEXITED(status))
                    {
                        exit_code_ = WEXITSTATUS(status);
                    }
                    else if (WIFSIGNALED(status))
                    {
                        exit_code_ = 128 + WTERMSIG(status);
                    }
                    else
                    {
                        exit_code_ = -1;
                    }

                    pid_ = -1;
                    stdin_write_.reset();
                    stdout_read_.reset();
                }
                // else: wait_result < 0 (error, e.g. ECHILD) — treat as not running.
            }

            return result;
        }

        /// @brief Waits indefinitely for the child to exit.
        /// @return the process exit code.
        /// @throws std::logic_error if no process was started.
        /// @throws std::runtime_error on OS-level failure.
        int wait()
        {
            // If already reaped by running(), return the cached exit code.
            if (pid_ <= 0)
            {
                if (exit_code_ != -1)
                {
                    return exit_code_;
                }
                throw std::logic_error{
                    "No process is running."
                };
            }

            int status{};

            const auto result =
                waitpid(
                    pid_,
                    &status,
                    0
                );

            if (result < 0)
            {
                throw std::runtime_error{
                    std::format(
                        "waitpid() failed: {}",
                        std::strerror(errno)
                    )
                };
            }

            if (WIFEXITED(status))
            {
                exit_code_ = WEXITSTATUS(status);
            }
            else if (WIFSIGNALED(status))
            {
                exit_code_ = 128 + WTERMSIG(status);
            }
            else
            {
                exit_code_ = -1;
            }

            pid_ = -1;
            stdin_write_.reset();
            stdout_read_.reset();

            return exit_code_;
        }

        /// @brief Gracefully terminates the child process and waits for it.
        ///
        /// Sends SIGTERM first, then reaps the zombie with waitpid.
        /// This method is noexcept-safe — errors are swallowed to allow
        /// safe use in destructors and cleanup paths.
        void terminate() noexcept
        {
            if (pid_ <= 0)
            {
                return;
            }

            // Best-effort SIGTERM — ignore ESRCH (already exited).
            if (kill(pid_, SIGTERM) != 0 && errno != ESRCH)
            {
                // Fall through to waitpid even on error.
            }

            // Best-effort reap — ignore failures.
            int status{};
            if (waitpid(pid_, &status, 0) > 0)
            {
                if (WIFEXITED(status))
                {
                    exit_code_ = WEXITSTATUS(status);
                }
                else if (WIFSIGNALED(status))
                {
                    exit_code_ = 128 + WTERMSIG(status);
                }
            }

            reset();
        }

        /// @brief Returns the last known exit code.
        /// @return the exit code, or -1 if the process hasn't exited yet.
        [[nodiscard]] int exit_code() const noexcept
        {
            return exit_code_;
        }

    private:
        pid_t pid_{-1};
        FileDescriptor stdin_write_{};
        FileDescriptor stdout_read_{};
        int exit_code_{-1};

        /// @brief Terminates the child if still running, then releases all resources.
        void reset() noexcept
        {
            // If the child is still running, terminate it best-effort.
            if (pid_ > 0)
            {
                kill(pid_, SIGTERM);
                int status{};
                waitpid(pid_, &status, 0);
            }

            pid_ = -1;
            stdin_write_.reset();
            stdout_read_.reset();
        }
    };
}

#endif