// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT
//
// Binance spot market data over WebSocket (kernel TLS), printed as received.
//
// Shows the parts every exchange connector needs on top of the library:
//   * reconnecting with exponential backoff and jitter when the connection drops (Binance also
//     closes every connection after 24 hours), longer pauses when the exchange rate-limits us;
//   * a watchdog for silent connections (half-open TCP: no data, no error);
//   * re-sending subscriptions after every reconnect (--live-subscribe);
//   * graceful shutdown on SIGINT/SIGTERM (Close frame, then exit).
//
//   binance_market_data --streams btcusdt@trade,ethusdt@bookTicker
//   binance_market_data --streams btcusdt@depth@100ms --live-subscribe --busy-poll --cpu 3
//
// Needs the tls kernel module (modprobe tls); see README.

#include <sched.h>

#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <print>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <cxxopts.hpp>

#include <turboq/reactor/Reactor.h>

namespace {

using namespace turboq::reactor;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

volatile std::sig_atomic_t gStop = 0;

void onSignal(int) {
    gStop = 1;
}

/// "2026-10-07 12:34:56.123456" from CLOCK_REALTIME nanoseconds.
[[nodiscard]] auto formatTime(std::uint64_t ns) -> std::string {
    std::time_t const seconds = static_cast<std::time_t>(ns / 1'000'000'000u);
    std::tm tm;
    ::gmtime_r(&seconds, &tm);
    char buffer[40];
    auto const length = std::strftime(buffer, sizeof(buffer), "%F %T", &tm);
    return std::format("{}.{:06}", std::string_view{buffer, length}, (ns / 1000) % 1'000'000);
}

[[nodiscard]] auto realtimeNs() -> std::uint64_t {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

/// Exponential backoff with +-20% jitter, so that many clients dropped at once (exchange restart)
/// do not reconnect in lockstep.
class Backoff {
private:
    std::chrono::milliseconds initial_;
    std::chrono::milliseconds max_;
    std::chrono::milliseconds current_;
    std::mt19937_64 random_{std::random_device{}()};

public:
    Backoff(std::chrono::milliseconds initial, std::chrono::milliseconds max)
        : initial_{initial}, max_{max}, current_{initial} {}

    [[nodiscard]] auto next() -> std::chrono::milliseconds {
        auto const base = current_;
        current_ = std::min(current_ * 2, max_);
        std::uniform_real_distribution<double> jitter{0.8, 1.2};
        return std::chrono::milliseconds{
            static_cast<std::int64_t>(static_cast<double>(base.count()) * jitter(random_))};
    }

    void reset() {
        current_ = initial_;
    }
};

struct Config {
    std::vector<std::string> streams;
    std::string baseUrl;
    bool liveSubscribe = false;
    /// Nothing received (data or server ping) for this long: reconnect. Binance pings every 20 s.
    std::chrono::milliseconds staleTimeout{30s};
    std::chrono::milliseconds backoffInitial{500ms};
    std::chrono::milliseconds backoffMax{30s};
    /// A connection that stayed up this long counts as healthy: the backoff starts over.
    std::chrono::milliseconds stableAfter{60s};
};

[[nodiscard]] auto joinStreams(std::vector<std::string> const& streams, char separator) -> std::string {
    std::string result;
    for (auto const& stream : streams) {
        if (!result.empty()) {
            result += separator;
        }
        result += stream;
    }
    return result;
}

/// One market data connection with its reconnect policy. Driven by process() from the main loop;
/// never blocks.
class MarketDataFeed {
private:
    Reactor& reactor_;
    Config config_;
    WsConnection ws_;
    Backoff backoff_;

    ConnectionState lastState_{ConnectionState::Idle};
    Clock::time_point readyAt_{};
    std::uint64_t readyAtNs_{0}; // Reactor::now() clock
    Clock::time_point reconnectAt_{};
    bool reconnectPending_{false};
    bool stopping_{false};
    bool kernelTlsHintShown_{false};
    std::uint64_t sessions_{0};

public:
    MarketDataFeed(Reactor& reactor, Config config)
        : reactor_{reactor}, config_{std::move(config)}, ws_{reactor, makeOptions(config_)},
          backoff_{config_.backoffInitial, config_.backoffMax} {}

    void start() {
        this->connect(Clock::now());
    }

    /// Ask for a graceful shutdown: Close frame, then the connection goes Closed.
    void stop() {
        stopping_ = true;
        reconnectPending_ = false;
        ws_.close();
    }

    [[nodiscard]] auto stopped() const -> bool {
        return stopping_ && ws_.state() == ConnectionState::Closed;
    }

    /// Call after every Reactor::poll()/wait(). Returns true if anything was printed.
    auto process(Clock::time_point now) -> bool {
        bool printed = this->drainMessages();

        auto const state = ws_.state();
        if (state != lastState_) {
            this->onStateChange(lastState_, state, now);
            lastState_ = state;
            printed = true;
        }

        if (state == ConnectionState::Ready) {
            // TCP can stay "connected" forever after a network failure: no data, no error. Binance
            // pings every 20 s, so anything received (pings included) proves the link is alive.
            auto const lastReceive = std::max(ws_.lastReceiveTime(), readyAtNs_);
            auto const silence = std::chrono::nanoseconds{reactor_.now() - lastReceive};
            if (reactor_.now() > lastReceive && silence > config_.staleTimeout) {
                std::println(stderr, "{} nothing received for {} ms, reconnecting", formatTime(realtimeNs()),
                    std::chrono::duration_cast<std::chrono::milliseconds>(silence).count());
                ws_.close(); // -> Closed -> reconnect scheduled in onStateChange()
            }
        }

        if (reconnectPending_ && now >= reconnectAt_) {
            reconnectPending_ = false;
            this->connect(now);
        }
        return printed;
    }

private:
    [[nodiscard]] static auto makeOptions(Config const& config) -> WsOptions {
        // Combined streams wrap each event as {"stream":"<name>","data":{...}}. With live
        // subscription we connect to the bare endpoint and subscribe after every connect.
        auto url = config.liveSubscribe ? config.baseUrl + "/ws"
                                        : config.baseUrl + "/stream?streams=" + joinStreams(config.streams, '/');
        return WsOptions{
            .url = std::move(url),
            .rxBufferSize = 4u << 20, // largest message + backlog must fit (depth snapshots are big)
            .maxQueuedMessages = 1u << 16,
            .connectTimeout = 5s,
            .handshakeTimeout = 5s,
        };
    }

    void connect(Clock::time_point now) {
        if (auto result = ws_.connect(); !result) {
            // Synchronous failure (DNS, socket()): same policy as an asynchronous one.
            std::println(stderr, "{} connect failed: {}", formatTime(realtimeNs()), result.error().message());
            this->scheduleReconnect(now);
        }
        lastState_ = ws_.state();
    }

    void scheduleReconnect(Clock::time_point now) {
        auto delay = backoff_.next();
        // 429: too many requests / connection attempts; 418: the IP got banned for ignoring 429.
        if (ws_.httpStatus() == 429 || ws_.httpStatus() == 418) {
            delay = std::max<std::chrono::milliseconds>(delay, 60s);
        }
        std::println(stderr, "{} reconnecting in {} ms", formatTime(realtimeNs()), delay.count());
        reconnectAt_ = now + delay;
        reconnectPending_ = true;
    }

    void onStateChange(ConnectionState from, ConnectionState to, Clock::time_point now) {
        std::println(stderr, "{} state {} -> {}", formatTime(realtimeNs()), toStringView(from), toStringView(to));
        if (to == ConnectionState::Ready) {
            ++sessions_;
            readyAt_ = now;
            readyAtNs_ = reactor_.now();
            std::println(stderr, "{} connected{}{}{}{}, session #{}", formatTime(realtimeNs()),
                ws_.tlsVersion().empty() ? "" : " (", ws_.tlsVersion(), ws_.tlsVersion().empty() ? "" : " ",
                ws_.tlsCipher().empty() ? std::string{} : std::string{ws_.tlsCipher()} + ")", sessions_);
            if (config_.liveSubscribe) {
                this->subscribe();
            }
        } else if (to == ConnectionState::Closed) {
            if (auto const ec = ws_.error()) {
                std::println(stderr, "{} disconnected: {}{}{}", formatTime(realtimeNs()), ec.message(),
                    ws_.closeReason().empty() ? "" : ", reason: ", ws_.closeReason());
                if (isKernelTlsError(ec) && !kernelTlsHintShown_) {
                    // Configuration problem, not a network one: say what is missing, once.
                    std::println(stderr, "{} kernel TLS: {}", formatTime(realtimeNs()), describeKernelTlsSupport());
                    kernelTlsHintShown_ = true;
                }
                if (ws_.httpStatus() != 0 && ws_.httpStatus() != 101) {
                    std::println(
                        stderr, "{} upgrade rejected with HTTP {}", formatTime(realtimeNs()), ws_.httpStatus());
                }
            }
            if (stopping_) {
                return;
            }
            if (readyAt_ != Clock::time_point{} && now - readyAt_ >= config_.stableAfter) {
                backoff_.reset(); // it was a healthy session, not a reconnect loop
            }
            readyAt_ = {};
            this->scheduleReconnect(now);
        }
    }

    void subscribe() {
        // One SUBSCRIBE carries all streams: Binance allows 5 incoming messages per second.
        std::string request = R"({"method":"SUBSCRIBE","params":[)";
        for (std::size_t i = 0; i < config_.streams.size(); ++i) {
            request += i == 0 ? "\"" : ",\"";
            request += config_.streams[i];
            request += '"';
        }
        request += std::format(R"(],"id":{}}})", sessions_);
        if (!ws_.tx.push(request)) {
            std::println(stderr, "{} subscribe request does not fit into the tx ring", formatTime(realtimeNs()));
            ws_.close();
            return;
        }
        ws_.tx.flush();
        std::println(stderr, "{} sent {}", formatTime(realtimeNs()), request);
    }

    auto drainMessages() -> bool {
        // Messages stay readable after a disconnect: this also prints what arrived just before it.
        bool printed = false;
        while (!ws_.rx.empty()) {
            auto const payload = ws_.rx.fetch();
            std::fwrite("[", 1, 1, stdout);
            auto const time = formatTime(ws_.rx.timestamp());
            std::fwrite(time.data(), 1, time.size(), stdout);
            std::fwrite("] ", 1, 2, stdout);
            std::fwrite(payload.data(), 1, payload.size(), stdout);
            std::fputc('\n', stdout);
            ws_.rx.consume();
            printed = true;
        }
        return printed;
    }
};

void pinToCpu(int cpu) {
    if (cpu < 0) {
        return;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
        throw std::runtime_error{"sched_setaffinity failed"};
    }
}

[[nodiscard]] auto splitList(std::string const& value) -> std::vector<std::string> {
    std::vector<std::string> result;
    std::size_t begin = 0;
    while (begin <= value.size()) {
        auto const end = std::min(value.find(',', begin), value.size());
        if (end > begin) {
            result.emplace_back(value.substr(begin, end - begin));
        }
        begin = end + 1;
    }
    return result;
}

} // namespace

auto main(int argc, char** argv) -> int {
    try {
        cxxopts::Options options{"binance_market_data", "Binance spot market data via turboq::reactor"};
        // clang-format off
        options.add_options()
            ("s,streams", "comma separated stream names (lowercase symbols)",
                cxxopts::value<std::string>()->default_value("btcusdt@trade,btcusdt@bookTicker"))
            ("url", "base endpoint; wss://data-stream.binance.vision serves market data only",
                cxxopts::value<std::string>()->default_value("wss://stream.binance.com:9443"))
            ("live-subscribe", "connect to /ws and send SUBSCRIBE after every (re)connect")
            ("stale-timeout", "reconnect when nothing (not even a ping) arrives for this many ms",
                cxxopts::value<unsigned>()->default_value("30000"))
            ("backoff-initial", "first reconnect delay, ms", cxxopts::value<unsigned>()->default_value("500"))
            ("backoff-max", "largest reconnect delay, ms", cxxopts::value<unsigned>()->default_value("30000"))
            ("busy-poll", "spin on Reactor::poll() instead of sleeping in Reactor::wait()")
            ("cpu", "pin to this CPU (-1 = no pinning)", cxxopts::value<int>()->default_value("-1"))
            ("h,help", "print usage");
        // clang-format on
        auto const args = options.parse(argc, argv);
        if (args.count("help")) {
            std::println("{}", options.help());
            return 0;
        }

        Config config{
            .streams = splitList(args["streams"].as<std::string>()),
            .baseUrl = args["url"].as<std::string>(),
            .liveSubscribe = args.count("live-subscribe") > 0,
            .staleTimeout = std::chrono::milliseconds{args["stale-timeout"].as<unsigned>()},
            .backoffInitial = std::chrono::milliseconds{args["backoff-initial"].as<unsigned>()},
            .backoffMax = std::chrono::milliseconds{args["backoff-max"].as<unsigned>()},
        };
        if (config.streams.empty()) {
            throw std::invalid_argument{"no streams given"};
        }
        bool const busyPoll = args.count("busy-poll") > 0;

        pinToCpu(args["cpu"].as<int>());
        std::signal(SIGINT, onSignal);
        std::signal(SIGTERM, onSignal);
        static char stdoutBuffer[1 << 16];
        std::setvbuf(stdout, stdoutBuffer, _IOFBF, sizeof(stdoutBuffer));

        Reactor reactor;
        MarketDataFeed feed{reactor, std::move(config)};
        feed.start();

        auto stopDeadline = Clock::time_point::max();
        while (!feed.stopped()) {
            if (busyPoll) {
                reactor.poll();
            } else {
                // Returns as soon as a completion arrives; the timeout only bounds how late timers
                // (reconnect, watchdog) fire.
                reactor.wait(50ms);
            }
            auto const now = Clock::now();
            if (feed.process(now)) {
                std::fflush(stdout);
            }
            if (gStop && stopDeadline == Clock::time_point::max()) {
                std::println(stderr, "{} stopping", formatTime(realtimeNs()));
                feed.stop();
                stopDeadline = now + 2s;
            }
            if (now > stopDeadline) {
                break; // the peer did not let us close in time
            }
        }
        std::fflush(stdout);
    } catch (std::exception const& e) {
        std::println(stderr, "error: {}", e.what());
        return 1;
    }
    return 0;
}
