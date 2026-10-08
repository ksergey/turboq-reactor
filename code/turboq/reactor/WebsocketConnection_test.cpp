// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <doctest/doctest.h>

#include "Error.h"
#include "Reactor.h"
#include "TestBackend.h"
#include "detail/WebsocketProtocol.h"

namespace turboq::reactor::testing {
namespace {

using namespace std::chrono_literals;

[[maybe_unused]] auto const kIgnoreSigPipe = std::signal(SIGPIPE, SIG_IGN);

template <typename Pred>
[[nodiscard]] auto pollUntil(Reactor& reactor, Pred pred, std::chrono::milliseconds timeout = 3000ms) -> bool {
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        reactor.poll();
    }
    return true;
}

[[nodiscard]] auto asString(std::span<std::byte const> data) -> std::string {
    return {reinterpret_cast<char const*>(data.data()), data.size()};
}

struct Frame {
    bool fin;
    std::uint8_t opcode;
    bool masked;
    std::string payload; // unmasked
};

/// Server side of one WebSocket connection (blocking socket).
struct Peer {
    int fd{-1};
    std::string request;

    [[nodiscard]] auto writeRaw(std::string_view data) const -> bool {
        while (!data.empty()) {
            auto const rc = ::send(fd, data.data(), data.size(), MSG_NOSIGNAL);
            if (rc <= 0) {
                return false;
            }
            data.remove_prefix(static_cast<std::size_t>(rc));
        }
        return true;
    }

    /// Server frame (unmasked unless asked to, which is a protocol violation).
    [[nodiscard]] static auto frame(
        bool fin, std::uint8_t opcode, std::string_view payload, bool mask = false) -> std::string {
        std::string out;
        out += static_cast<char>((fin ? 0x80 : 0) | opcode);
        auto const maskBit = mask ? 0x80 : 0;
        if (payload.size() <= 125) {
            out += static_cast<char>(maskBit | payload.size());
        } else if (payload.size() <= 0xFFFF) {
            out += static_cast<char>(maskBit | 126);
            out += static_cast<char>(payload.size() >> 8);
            out += static_cast<char>(payload.size());
        } else {
            out += static_cast<char>(maskBit | 127);
            for (int i = 7; i >= 0; --i) {
                out += static_cast<char>(static_cast<std::uint64_t>(payload.size()) >> (8 * i));
            }
        }
        if (mask) {
            out += std::string(4, '\0'); // zero key keeps the payload as is
        }
        out += payload;
        return out;
    }

    [[nodiscard]] auto writeFrame(bool fin, std::uint8_t opcode, std::string_view payload) const -> bool {
        return this->writeRaw(frame(fin, opcode, payload));
    }

    [[nodiscard]] auto readExactly(std::size_t size) const -> std::optional<std::string> {
        std::string data(size, '\0');
        std::size_t done = 0;
        while (done < size) {
            auto const rc = ::recv(fd, data.data() + done, size - done, 0);
            if (rc <= 0) {
                return std::nullopt;
            }
            done += static_cast<std::size_t>(rc);
        }
        return data;
    }

    [[nodiscard]] auto readFrame() const -> std::optional<Frame> {
        auto header = this->readExactly(2);
        if (!header) {
            return std::nullopt;
        }
        Frame frame{};
        auto const b0 = static_cast<unsigned char>((*header)[0]);
        auto const b1 = static_cast<unsigned char>((*header)[1]);
        frame.fin = (b0 & 0x80) != 0;
        frame.opcode = b0 & 0x0F;
        frame.masked = (b1 & 0x80) != 0;
        std::uint64_t length = b1 & 0x7F;
        if (length == 126) {
            auto ext = this->readExactly(2);
            length = std::uint64_t{static_cast<unsigned char>((*ext)[0])} << 8 | static_cast<unsigned char>((*ext)[1]);
        } else if (length == 127) {
            auto ext = this->readExactly(8);
            length = 0;
            for (char c : *ext) {
                length = length << 8 | static_cast<unsigned char>(c);
            }
        }
        std::string key = frame.masked ? *this->readExactly(4) : std::string(4, '\0');
        auto payload = this->readExactly(static_cast<std::size_t>(length));
        if (!payload) {
            return std::nullopt;
        }
        for (std::size_t i = 0; i < payload->size(); ++i) {
            (*payload)[i] ^= key[i % 4];
        }
        frame.payload = std::move(*payload);
        return frame;
    }

    /// Wait for the client to go away.
    void drain() const {
        char buffer[4096];
        while (::recv(fd, buffer, sizeof(buffer), 0) > 0) {}
    }
};

/// Loopback WebSocket server running one scripted connection in a thread.
class WsServer {
public:
    using Respond = std::function<std::string(std::string const& key)>;
    using Script = std::function<void(Peer&)>;

    static auto accept101(std::string const& key) -> std::string {
        return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
               "Sec-WebSocket-Accept: " +
               detail::computeWsAccept(key) + "\r\n\r\n";
    }

private:
    int listener_{-1};
    std::uint16_t port_{0};
    std::thread thread_;

public:
    std::atomic<bool> done{false};

    explicit WsServer(Script script, Respond respond = &WsServer::accept101, int connections = 1) {
        listener_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        REQUIRE(listener_ >= 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        REQUIRE_EQ(::bind(listener_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        REQUIRE_EQ(::listen(listener_, 4), 0);
        socklen_t len = sizeof(addr);
        ::getsockname(listener_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);

        thread_ = std::thread{[this, script = std::move(script), respond = std::move(respond), connections] {
            for (int i = 0; i < connections; ++i) {
                this->serveOne(script, respond);
            }
            done = true;
        }};
    }

private:
    void serveOne(Script const& script, Respond const& respond) {
        Peer peer;
        peer.fd = ::accept4(listener_, nullptr, nullptr, SOCK_CLOEXEC);
        if (peer.fd < 0) {
            return;
        }
        // Read the upgrade request.
        char buffer[4096];
        while (peer.request.find("\r\n\r\n") == std::string::npos) {
            auto const rc = ::recv(peer.fd, buffer, sizeof(buffer), 0);
            if (rc <= 0) {
                ::close(peer.fd);
                return;
            }
            peer.request.append(buffer, static_cast<std::size_t>(rc));
        }
        std::string key;
        if (auto const pos = peer.request.find("Sec-WebSocket-Key: "); pos != std::string::npos) {
            key = peer.request.substr(pos + 19, peer.request.find("\r\n", pos) - pos - 19);
        }
        auto const response = respond(key);
        if (!response.empty() && peer.writeRaw(response)) {
            script(peer);
        } else {
            peer.drain();
        }
        ::close(peer.fd);
    }

public:
    ~WsServer() {
        if (thread_.joinable()) {
            thread_.join();
        }
        ::close(listener_);
    }

    void join() {
        thread_.join();
    }

    [[nodiscard]] auto url(std::string_view path = "/stream") const -> std::string {
        return "ws://127.0.0.1:" + std::to_string(port_) + std::string{path};
    }
};

} // namespace

TEST_SUITE("WebsocketConnection") {

    TEST_CASE("URL parsing") {
        auto url = detail::parseWsUrl("wss://stream.example.com/ws/btcusdt@trade");
        REQUIRE(url);
        REQUIRE(url->secure);
        REQUIRE_EQ(url->host, "stream.example.com");
        REQUIRE_EQ(url->port, 443);
        REQUIRE_EQ(url->target, "/ws/btcusdt@trade");
        REQUIRE_EQ(url->hostHeader, "stream.example.com");

        url = detail::parseWsUrl("ws://10.0.0.1:9001?stream=a#frag");
        REQUIRE(url);
        REQUIRE_FALSE(url->secure);
        REQUIRE_EQ(url->port, 9001);
        REQUIRE_EQ(url->target, "/?stream=a");
        REQUIRE_EQ(url->hostHeader, "10.0.0.1:9001");

        url = detail::parseWsUrl("WS://[::1]:8080/x");
        REQUIRE(url);
        REQUIRE_EQ(url->host, "::1");
        REQUIRE_EQ(url->hostHeader, "[::1]:8080");

        for (auto const* bad : {"http://example.com", "ws://", "ws://:80/", "ws://host:0/", "ws://host:99999",
                 "ws://user@host/", "ws://[::1/", "example.com"}) {
            CAPTURE(bad);
            REQUIRE_FALSE(detail::parseWsUrl(bad));
        }
    }

    TEST_CASE("accept key matches RFC 6455 example") {
        REQUIRE_EQ(detail::computeWsAccept("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    TEST_CASE("masking is an involution for any length and key") {
        for (std::size_t size : {0u, 1u, 3u, 4u, 7u, 8u, 9u, 15u, 16u, 17u, 1000u}) {
            std::vector<std::byte> data(size);
            for (std::size_t i = 0; i < size; ++i) {
                data[i] = static_cast<std::byte>(i * 7);
            }
            auto const original = data;
            detail::applyWsMask(data.data(), size, 0xA1B2C3D4);
            std::uint32_t const k = 0xA1B2C3D4;
            auto const* key = reinterpret_cast<unsigned char const*>(&k);
            for (std::size_t i = 0; i < size; ++i) {
                REQUIRE_EQ(data[i], original[i] ^ static_cast<std::byte>(key[i % 4]));
            }
            detail::applyWsMask(data.data(), size, k);
            REQUIRE(data == original);
        }
    }

    TEST_CASE("upgrade and messages in both directions") {
        std::vector<Frame> received;
        std::string request;
        WsServer server{[&](Peer& peer) {
            request = peer.request;
            REQUIRE(peer.writeFrame(true, 0x1, R"({"e":"trade","p":"100.5"})"));
            REQUIRE(peer.writeFrame(true, 0x2, std::string(300, 'b')));   // 16-bit length
            REQUIRE(peer.writeFrame(true, 0x2, std::string(70000, 'c'))); // 64-bit length
            REQUIRE(peer.writeFrame(true, 0x1, ""));                      // empty message
            for (int i = 0; i < 5; ++i) {
                auto frame = peer.readFrame();
                REQUIRE(frame);
                received.push_back(std::move(*frame));
            }
            peer.drain();
        }};

        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url("/ws?streams=btcusdt"),
                                            .headers = {{"X-Api-Key", "secret"}},
                                            .rxBufferSize = 256 * 1024}};
        REQUIRE(ws.connect());
        REQUIRE(ws.tx.prepare(1).empty()); // not Ready yet
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Ready;
        }));
        REQUIRE_EQ(ws.httpStatus(), 101);

        REQUIRE(pollUntil(reactor, [&] {
            return ws.rx.size() == 4;
        }));
        REQUIRE_EQ(ws.rx.opcode(), WsOpcode::Text);
        REQUIRE(ws.rx.timestamp() != Timestamp{});
        REQUIRE_EQ(asString(ws.rx.fetch()), R"({"e":"trade","p":"100.5"})");
        ws.rx.consume();
        REQUIRE_EQ(ws.rx.opcode(), WsOpcode::Binary);
        REQUIRE_EQ(asString(ws.rx.fetch()), std::string(300, 'b'));
        ws.rx.consume();
        REQUIRE_EQ(asString(ws.rx.fetch()), std::string(70000, 'c'));
        ws.rx.consume();
        REQUIRE_FALSE(ws.rx.empty());
        REQUIRE(ws.rx.fetch().empty());
        ws.rx.consume();
        REQUIRE(ws.rx.empty());

        // tx: every length encoding, and commits smaller than the prepared size
        REQUIRE(ws.tx.push(R"({"method":"SUBSCRIBE"})"));
        auto buffer = ws.tx.prepare(200); // 16-bit length class...
        std::memset(buffer.data(), 'x', 100);
        ws.tx.commit(100, WsOpcode::Binary); // ...committed as 7-bit
        buffer = ws.tx.prepare(70000);
        std::memset(buffer.data(), 'y', 70000);
        ws.tx.commit(WsOpcode::Binary);
        buffer = ws.tx.prepare(70000);
        std::memset(buffer.data(), 'z', 300);
        ws.tx.commit(300); // 64-bit class committed as 16-bit
        REQUIRE(ws.tx.push(""));
        ws.tx.flush();

        REQUIRE(pollUntil(reactor, [&] {
            return server.done.load() || received.size() == 5;
        }));
        ws.close();
        server.join();

        REQUIRE(request.starts_with("GET /ws?streams=btcusdt HTTP/1.1\r\n"));
        REQUIRE_NE(request.find("Host: 127.0.0.1:"), std::string::npos);
        REQUIRE_NE(request.find("X-Api-Key: secret\r\n"), std::string::npos);
        REQUIRE_NE(request.find("Sec-WebSocket-Version: 13\r\n"), std::string::npos);

        REQUIRE_EQ(received.size(), 5);
        for (auto const& frame : received) {
            REQUIRE(frame.fin);
            REQUIRE(frame.masked);
        }
        REQUIRE_EQ(received[0].opcode, 0x1);
        REQUIRE_EQ(received[0].payload, R"({"method":"SUBSCRIBE"})");
        REQUIRE_EQ(received[1].opcode, 0x2);
        REQUIRE_EQ(received[1].payload, std::string(100, 'x'));
        REQUIRE_EQ(received[2].payload, std::string(70000, 'y'));
        REQUIRE_EQ(received[3].opcode, 0x1);
        REQUIRE_EQ(received[3].payload, std::string(300, 'z'));
        REQUIRE_EQ(received[4].payload, "");
    }

    TEST_CASE("frames in the same segment as the 101 response") {
        WsServer server{[](Peer& peer) {
                            peer.drain();
                        },
            [](std::string const& key) {
                return WsServer::accept101(key) + Peer::frame(true, 0x1, "early") + Peer::frame(true, 0x1, "bird");
            }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.rx.size() == 2;
        }));
        REQUIRE_EQ(asString(ws.rx.fetch()), "early");
        ws.rx.consume();
        REQUIRE_EQ(asString(ws.rx.fetch()), "bird");
        ws.close();
    }

    TEST_CASE("fragmented message with an interleaved ping") {
        std::optional<Frame> pong;
        WsServer server{[&](Peer& peer) {
            std::string data = Peer::frame(false, 0x1, "Hello, ") + Peer::frame(true, 0x9, "are you there?") +
                               Peer::frame(false, 0x0, "fragmented ") + Peer::frame(true, 0x0, "world") +
                               Peer::frame(true, 0x1, "next");
            // Byte by byte: exercises every partial-header and partial-payload path.
            for (char c : data) {
                REQUIRE(peer.writeRaw(std::string_view{&c, 1}));
            }
            pong = peer.readFrame();
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.rx.size() == 2;
        }));
        REQUIRE_EQ(ws.rx.opcode(), WsOpcode::Text);
        REQUIRE_EQ(asString(ws.rx.fetch()), "Hello, fragmented world");
        ws.rx.consume();
        REQUIRE_EQ(asString(ws.rx.fetch()), "next");
        ws.rx.consume();
        REQUIRE(pollUntil(reactor, [&] {
            return pong.has_value() || server.done.load();
        }));
        ws.close();
        server.join();
        REQUIRE(pong);
        REQUIRE_EQ(pong->opcode, 0xA);
        REQUIRE(pong->masked);
        REQUIRE_EQ(pong->payload, "are you there?");
    }

    TEST_CASE("pings while the user holds an open prepare() are answered after commit") {
        std::optional<Frame> first;
        std::optional<Frame> second;
        std::atomic<bool> prepared{false};
        WsServer server{[&](Peer& peer) {
            while (!prepared) {
                std::this_thread::sleep_for(1ms);
            }
            REQUIRE(peer.writeFrame(true, 0x9, "p1"));
            first = peer.readFrame();
            second = peer.readFrame();
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Ready;
        }));
        auto buffer = ws.tx.prepare(4);
        REQUIRE_EQ(buffer.size(), 4);
        prepared = true;
        // Not supposed to poll here, but the reactor must stay consistent if it happens.
        for (int i = 0; i < 50; ++i) {
            reactor.poll();
            std::this_thread::sleep_for(1ms);
        }
        std::memcpy(buffer.data(), "data", 4);
        ws.tx.commit();
        ws.tx.flush();
        REQUIRE(pollUntil(reactor, [&] {
            return second.has_value() || server.done.load();
        }));
        ws.close();
        server.join();
        REQUIRE(first);
        REQUIRE(second);
        REQUIRE_EQ(first->opcode, 0x1); // the user's message stays intact...
        REQUIRE_EQ(first->payload, "data");
        REQUIRE_EQ(second->opcode, 0xA); // ...and the pong follows it
        REQUIRE_EQ(second->payload, "p1");
    }

    TEST_CASE("close from the server is echoed") {
        std::optional<Frame> echo;
        WsServer server{[&](Peer& peer) {
            REQUIRE(peer.writeFrame(true, 0x1, "bye soon"));
            std::string payload = "\x03\xE9going away"; // 1001
            REQUIRE(peer.writeFrame(true, 0x8, payload));
            echo = peer.readFrame();
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        server.join();
        REQUIRE_EQ(ws.error(), std::error_code(1001, getWsCloseCategory()));
        REQUIRE_EQ(ws.closeReason(), "going away");
        REQUIRE_EQ(asString(ws.rx.fetch()), "bye soon"); // received before the close: still readable
        REQUIRE(echo);
        REQUIRE_EQ(echo->opcode, 0x8);
        REQUIRE_EQ(echo->payload, std::string("\x03\xE9"));
    }

    TEST_CASE("user close sends a Close frame") {
        std::optional<Frame> closeFrame;
        WsServer server{[&](Peer& peer) {
            closeFrame = peer.readFrame();
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Ready;
        }));
        ws.close(4000, "custom");
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        REQUIRE_FALSE(ws.error());
        server.join();
        REQUIRE(closeFrame);
        REQUIRE_EQ(closeFrame->opcode, 0x8);
        REQUIRE_EQ(closeFrame->payload, std::string("\x0F\xA0") + "custom");
    }

    TEST_CASE("client ping") {
        std::optional<Frame> ping;
        WsServer server{[&](Peer& peer) {
            ping = peer.readFrame();
            REQUIRE(peer.writeFrame(true, 0xA, ping ? ping->payload : ""));
            REQUIRE(peer.writeFrame(true, 0x1, "after pong"));
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Ready;
        }));
        std::string_view payload = "keepalive";
        REQUIRE(ws.ping({reinterpret_cast<std::byte const*>(payload.data()), payload.size()}));
        REQUIRE(pollUntil(reactor, [&] {
            return !ws.rx.empty();
        }));
        REQUIRE_EQ(asString(ws.rx.fetch()), "after pong"); // pongs are not queued
        ws.close();
        server.join();
        REQUIRE(ping);
        REQUIRE_EQ(ping->opcode, 0x9);
        REQUIRE_EQ(ping->payload, "keepalive");
    }

    TEST_CASE("rejected upgrade") {
        WsServer server{[](Peer&) {},
            [](std::string const&) {
                return std::string{"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n"};
            }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(ws.error(), makeErrorCode(Error::WsHandshakeFailed));
        REQUIRE_EQ(ws.httpStatus(), 403);
    }

    TEST_CASE("invalid Sec-WebSocket-Accept is rejected") {
        WsServer server{[](Peer& peer) {
                            peer.drain();
                        },
            [](std::string const&) {
                return std::string{"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                                   "Sec-WebSocket-Accept: AAAAAAAAAAAAAAAAAAAAAAAAAAA=\r\n\r\n"};
            }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(ws.error(), makeErrorCode(Error::WsHandshakeFailed));
        REQUIRE_EQ(ws.httpStatus(), 101);
    }

    TEST_CASE("upgrade timeout") {
        WsServer server{[](Peer&) {},
            [](std::string const&) {
                return std::string{};
            }}; // never answers
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url(), .handshakeTimeout = 200ms}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Handshaking;
        }));
        auto const start = std::chrono::steady_clock::now();
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(ws.error(), makeErrorCode(Error::WsHandshakeTimeout));
        REQUIRE_GE(std::chrono::steady_clock::now() - start, 150ms);
    }

    TEST_CASE("masked server frame is a protocol error") {
        std::optional<Frame> closeFrame;
        WsServer server{[&](Peer& peer) {
            REQUIRE(peer.writeRaw(Peer::frame(true, 0x1, "masked", true)));
            closeFrame = peer.readFrame();
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        server.join();
        REQUIRE_EQ(ws.error(), makeErrorCode(Error::WsProtocolError));
        REQUIRE(closeFrame);
        REQUIRE_EQ(closeFrame->payload, std::string("\x03\xEA")); // 1002
    }

    TEST_CASE("message larger than the rx ring") {
        WsServer server{[&](Peer& peer) {
            [[maybe_unused]] bool sent = peer.writeFrame(true, 0x2, std::string(20000, 'x'));
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url(), .rxBufferSize = 4096}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(ws.error(), makeErrorCode(Error::WsMessageTooBig));
    }

    TEST_CASE("maxMessageSize") {
        WsServer server{[&](Peer& peer) {
            REQUIRE(peer.writeFrame(true, 0x1, std::string(100, 'a')));
            REQUIRE(peer.writeFrame(true, 0x1, std::string(101, 'a')));
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url(), .maxMessageSize = 100}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(ws.error(), makeErrorCode(Error::WsMessageTooBig));
        REQUIRE_EQ(ws.rx.size(), 1);
        REQUIRE_EQ(ws.rx.fetch().size(), 100);
    }

    TEST_CASE("many small messages through a small ring and queue keep order") {
        constexpr int kCount = 20000;
        WsServer server{[&](Peer& peer) {
            std::string batch;
            for (int i = 0; i < kCount; ++i) {
                batch += Peer::frame(true, 0x1, std::to_string(i));
                if (i % 7 == 0) {
                    batch += Peer::frame(true, 0x9, ""); // control frames between messages
                }
                if (batch.size() > 8192) {
                    REQUIRE(peer.writeRaw(batch));
                    batch.clear();
                }
            }
            REQUIRE(peer.writeRaw(batch));
            peer.drain();
        }};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url(), .rxBufferSize = 4096, .maxQueuedMessages = 16}};
        REQUIRE(ws.connect());
        int expected = 0;
        REQUIRE(pollUntil(
            reactor,
            [&] {
                // Consume at most 5 per poll so the queue fills and parsing has to resume in consume().
                for (int n = 0; n < 5 && !ws.rx.empty(); ++n) {
                    REQUIRE_EQ(asString(ws.rx.fetch()), std::to_string(expected));
                    ws.rx.consume();
                    ++expected;
                }
                return expected == kCount || ws.state() == ConnectionState::Closed;
            },
            10000ms));
        REQUIRE_EQ(expected, kCount);
        ws.close();
    }

    TEST_CASE("reconnect after close") {
        std::atomic<int> session{0};
        WsServer server{[&](Peer& peer) {
                            REQUIRE(peer.writeFrame(true, 0x1, ++session == 1 ? "one" : "two"));
                            peer.drain();
                        },
            &WsServer::accept101, 2};
        Reactor reactor;
        WebsocketConnection ws{reactor, {.url = server.url()}};
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return !ws.rx.empty();
        }));
        REQUIRE_EQ(asString(ws.rx.fetch()), "one");
        ws.close();
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
        REQUIRE_FALSE(ws.error());
        REQUIRE_EQ(asString(ws.rx.fetch()), "one"); // still readable after close

        REQUIRE(ws.connect());
        REQUIRE(ws.rx.empty()); // previous session dropped
        REQUIRE(pollUntil(reactor, [&] {
            return !ws.rx.empty();
        }));
        REQUIRE_EQ(asString(ws.rx.fetch()), "two");
        ws.close();
        REQUIRE(pollUntil(reactor, [&] {
            return ws.state() == ConnectionState::Closed;
        }));
    }

    TEST_CASE("invalid URL throws") {
        Reactor reactor;
        REQUIRE_THROWS_AS(WebsocketConnection(reactor, {.url = "http://example.com/"}), std::system_error);
    }
}

} // namespace turboq::reactor::testing
