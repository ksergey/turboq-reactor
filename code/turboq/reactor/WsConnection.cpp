// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "WsConnection.h"

#include <algorithm>
#include <charconv>
#include <string>

#include <turboq/Math.h>

#include "Error.h"
#include "Reactor.h"

namespace turboq::reactor {
namespace {

constexpr std::size_t kMaxResponseHeaderSize = 16 * 1024;
constexpr std::size_t kMaxPendingControl = 64 * 1024;

// Close codes we send.
constexpr std::uint16_t kCloseNormal = 1000;
constexpr std::uint16_t kCloseProtocolError = 1002;
constexpr std::uint16_t kCloseNoStatus = 1005;
constexpr std::uint16_t kCloseMessageTooBig = 1009;

[[nodiscard]] auto toLower(std::string_view text) -> std::string {
    std::string result{text};
    for (auto& c : result) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c - 'A' + 'a');
        }
    }
    return result;
}

[[nodiscard]] auto trim(std::string_view text) noexcept -> std::string_view {
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
        text.remove_prefix(1);
    }
    while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) {
        text.remove_suffix(1);
    }
    return text;
}

/// True if the comma separated header value contains `token` (case-insensitive).
[[nodiscard]] auto hasToken(std::string_view value, std::string_view token) -> bool {
    while (!value.empty()) {
        auto const comma = value.find(',');
        if (toLower(trim(value.substr(0, comma))) == token) {
            return true;
        }
        if (comma == std::string_view::npos) {
            break;
        }
        value.remove_prefix(comma + 1);
    }
    return false;
}

} // namespace

namespace detail {

auto WsCore::create(Ring& ring, WsOptions options) -> WsCore* {
    auto url = parseWsUrl(options.url);
    if (!url) {
        throw std::system_error{url.error(), "WsCore"};
    }
    TlsOptions tls = options.tls;
    tls.enabled = url->secure;
    std::unique_ptr<TcpCore> tcp{TcpCore::create(ring, {.host = url->host,
                                                           .port = url->port,
                                                           .rxBufferSize = options.rxBufferSize,
                                                           .txBufferSize = options.txBufferSize,
                                                           .connectTimeout = options.connectTimeout,
                                                           .noDelay = options.noDelay,
                                                           .directSend = options.directSend,
                                                           .tls = std::move(tls)})};
    auto* conn = new WsCore{ring, tcp.get(), std::move(options), std::move(*url)};
    tcp.release(); // owned by conn now
    return conn;
}

WsCore::WsCore(Ring& ring, TcpCore* tcp, WsOptions options, WsUrl url)
    : ring_{ring}, tcpCore_{tcp}, tcp_{*tcp}, options_{std::move(options)}, url_{std::move(url)} {
    tcp_.setObserver(this);
    entries_.resize(upperPow2(std::max<std::size_t>(options_.maxQueuedMessages, 1)));
    entriesMask_ = entries_.size() - 1;
    maxMessageSize_ = options_.maxMessageSize != 0 ? options_.maxMessageSize : tcp_.rx.capacity();
    pendingControl_.reserve(256);
    detail::fillRandom(&prngState_, sizeof(prngState_));
    prngState_ |= 1; // xorshift state must not be zero
}

auto WsCore::connect() -> std::expected<void, std::error_code> {
    if (state_ != ConnectionState::Idle && state_ != ConnectionState::Closed) {
        return std::unexpected(makeErrorCode(Error::InvalidState));
    }
    rxHead_ = rxTail_ = 0;
    streamHead_ = parsePos_ = 0;
    parseBlocked_ = false;
    assembling_ = false;
    assemblySize_ = 0;
    reservation_ = nullptr;
    prepared_ = 0;
    pendingControl_.clear();
    error_.clear();
    httpStatus_ = 0;
    closeReason_.clear();

    if (auto result = tcp_.connect(); !result) {
        state_ = ConnectionState::Closed;
        error_ = result.error();
        return result;
    }
    state_ = ConnectionState::Connecting;
    return {};
}

void WsCore::close(std::uint16_t code, std::string_view reason) noexcept {
    switch (state_) {
    case ConnectionState::Idle: state_ = ConnectionState::Closed; break;
    case ConnectionState::Ready: {
        std::array<std::byte, 125> payload;
        payload[0] = static_cast<std::byte>(code >> 8);
        payload[1] = static_cast<std::byte>(code);
        auto const reasonSize = std::min<std::size_t>(reason.size(), payload.size() - 2);
        if (reasonSize != 0) { // memcpy from a null pointer is UB even for 0 bytes
            std::memcpy(payload.data() + 2, reason.data(), reasonSize);
        }
        this->queueControl(WsOpcode::Close, std::span{payload}.first(2 + reasonSize));
        prepared_ = 0; // an abandoned prepare() must not block the Close frame
        this->flushControl();
        this->beginClose();
        break;
    }
    case ConnectionState::Connecting: [[fallthrough]];
    case ConnectionState::Handshaking: this->beginClose(); break;
    case ConnectionState::Closing: [[fallthrough]];
    case ConnectionState::Closed: break;
    }
}

auto WsCore::ping(std::span<std::byte const> payload) noexcept -> bool {
    if (state_ != ConnectionState::Ready || payload.size() > 125) {
        return false;
    }
    this->queueControl(WsOpcode::Ping, payload);
    this->flushControl();
    return true;
}

// --- events from the TCP connection -------------------------------------------------------------

void WsCore::onStreamReady() noexcept {
    if (state_ != ConnectionState::Connecting) {
        return;
    }
    handshakeKey_ = detail::makeWsKey();

    std::string request;
    request.reserve(512);
    request += "GET ";
    request += url_.target;
    request += " HTTP/1.1\r\nHost: ";
    request += url_.hostHeader;
    request += "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ";
    request += handshakeKey_;
    request += "\r\nSec-WebSocket-Version: 13\r\n";
    if (!options_.subprotocol.empty()) {
        request += "Sec-WebSocket-Protocol: ";
        request += options_.subprotocol;
        request += "\r\n";
    }
    for (auto const& [name, value] : options_.headers) {
        request += name;
        request += ": ";
        request += value;
        request += "\r\n";
    }
    request += "\r\n";

    if (!tcp_.tx.push({reinterpret_cast<std::byte const*>(request.data()), request.size()})) {
        this->fail(makeErrorCode(Error::InvalidOptions), 0); // tx ring smaller than the request
        return;
    }
    tcp_.tx.flush();
    state_ = ConnectionState::Handshaking;
    this->armTimeout();
}

void WsCore::onStreamData() noexcept {
    if (state_ == ConnectionState::Handshaking) {
        this->completeUpgrade();
    } else if (state_ == ConnectionState::Ready) {
        this->parseFrames();
    }
    this->flushControl();
}

void WsCore::onStreamClosed() noexcept {
    if (state_ != ConnectionState::Closing && state_ != ConnectionState::Closed) {
        // Closed underneath us (peer reset, TLS alert, ...).
        if (!error_) {
            error_ = tcp_.error() ? tcp_.error() : makeErrorCode(Error::ClosedByPeer);
        }
        state_ = ConnectionState::Closing;
        this->disarmTimeout();
    }
    this->finishCloseIfDone();
}

void WsCore::onCompletion(detail::OpCode op, std::int32_t res, [[maybe_unused]] std::uint32_t flags) noexcept {
    assert(inflight_ > 0);
    --inflight_;
    if (op == detail::OpCode::ConnectTimeout) {
        timeoutArmed_ = false;
        if (res == -ETIME && state_ == ConnectionState::Handshaking) {
            this->fail(makeErrorCode(Error::WsHandshakeTimeout), 0);
        }
    }
    this->finishCloseIfDone();
}

// --- upgrade ------------------------------------------------------------------------------------

void WsCore::completeUpgrade() noexcept {
    auto const data = tcp_.rx.fetch();
    std::string_view const text{reinterpret_cast<char const*>(data.data()), data.size()};
    auto const headerEnd = text.find("\r\n\r\n");
    if (headerEnd == std::string_view::npos) {
        if (text.size() > kMaxResponseHeaderSize) {
            this->fail(makeErrorCode(Error::WsHandshakeFailed), 0);
        }
        return; // wait for more
    }
    auto const headerSize = headerEnd + 4;
    auto response = text.substr(0, headerEnd);

    // Status line: HTTP/1.1 101 Switching Protocols
    auto const lineEnd = response.find("\r\n");
    auto const statusLine = response.substr(0, lineEnd);
    auto const space = statusLine.find(' ');
    if (!statusLine.starts_with("HTTP/1.") || space == std::string_view::npos) {
        this->fail(makeErrorCode(Error::WsHandshakeFailed), 0);
        return;
    }
    auto const statusText = statusLine.substr(space + 1, 3);
    std::from_chars(statusText.data(), statusText.data() + statusText.size(), httpStatus_);
    if (httpStatus_ != 101) {
        this->fail(makeErrorCode(Error::WsHandshakeFailed), 0);
        return;
    }

    bool upgradeOk = false;
    bool connectionOk = false;
    bool acceptOk = false;
    bool protocolOk = options_.subprotocol.empty();
    bool extensionsOk = true;
    auto const expectedAccept = detail::computeWsAccept(handshakeKey_);
    response = lineEnd == std::string_view::npos ? std::string_view{} : response.substr(lineEnd + 2);
    while (!response.empty()) {
        auto const end = response.find("\r\n");
        auto const line = response.substr(0, end);
        response = end == std::string_view::npos ? std::string_view{} : response.substr(end + 2);
        auto const colon = line.find(':');
        if (colon == std::string_view::npos) {
            continue;
        }
        auto const name = toLower(trim(line.substr(0, colon)));
        auto const value = trim(line.substr(colon + 1));
        if (name == "upgrade") {
            upgradeOk = toLower(value) == "websocket";
        } else if (name == "connection") {
            connectionOk = hasToken(value, "upgrade");
        } else if (name == "sec-websocket-accept") {
            acceptOk = value == expectedAccept;
        } else if (name == "sec-websocket-protocol") {
            protocolOk = value == options_.subprotocol;
        } else if (name == "sec-websocket-extensions") {
            extensionsOk = false; // none requested (no permessage-deflate)
        }
    }
    if (!(upgradeOk && connectionOk && acceptOk && protocolOk && extensionsOk)) {
        this->fail(makeErrorCode(Error::WsHandshakeFailed), 0);
        return;
    }

    // Frames may follow the response in the same segment.
    tcp_.rx.consume(headerSize);
    streamHead_ = parsePos_ = headerSize;
    this->disarmTimeout();
    state_ = ConnectionState::Ready;
    this->parseFrames();
}

// --- rx -----------------------------------------------------------------------------------------

void WsCore::releaseTo(std::uint64_t position) noexcept {
    if (position > streamHead_) {
        auto const size = position - streamHead_;
        streamHead_ = position;
        tcp_.rx.consume(size);
    }
}

void WsCore::consumeFront() noexcept {
    assert(rxHead_ != rxTail_);
    ++rxHead_;
    // Everything before the next held byte can go: the message itself and any control frames
    // that followed it (already handled when they were parsed).
    std::uint64_t const keep = rxHead_ != rxTail_ ? entries_[rxHead_ & entriesMask_].begin
                               : assembling_      ? assemblyBegin_
                                                  : parsePos_;
    this->releaseTo(keep);
    if (parseBlocked_) [[unlikely]] {
        this->parseFrames();
        this->flushControl();
    }
}

void WsCore::parseFrames() noexcept {
    parseBlocked_ = false;
    while (state_ == ConnectionState::Ready) {
        if (rxTail_ - rxHead_ == entries_.size()) {
            parseBlocked_ = true; // message queue full: resume in consume()
            break;
        }

        auto const available = static_cast<std::size_t>(streamHead_ + tcp_.rx.fetch().size() - parsePos_);
        if (available < 2) {
            break;
        }
        auto const* p = reinterpret_cast<unsigned char const*>(this->streamPointer(parsePos_));
        bool const fin = (p[0] & 0x80) != 0;
        auto const opcode = static_cast<WsOpcode>(p[0] & 0x0F);
        if ((p[0] & 0x70) != 0 || (p[1] & 0x80) != 0) {
            // RSV bits without a negotiated extension, or a masked server frame.
            this->fail(makeErrorCode(Error::WsProtocolError), kCloseProtocolError);
            return;
        }
        std::size_t headerSize = 2;
        std::uint64_t length = p[1] & 0x7F;
        if (length == 126) {
            if (available < 4) {
                break;
            }
            length = std::uint64_t{p[2]} << 8 | p[3];
            headerSize = 4;
        } else if (length == 127) {
            if (available < 10) {
                break;
            }
            length = 0;
            for (int i = 0; i < 8; ++i) {
                length = length << 8 | p[2 + i];
            }
            headerSize = 10;
        }

        bool const control = (static_cast<std::uint8_t>(opcode) & 0x08) != 0;
        if (control) {
            if (!fin || length > 125 ||
                (opcode != WsOpcode::Close && opcode != WsOpcode::Ping && opcode != WsOpcode::Pong)) {
                this->fail(makeErrorCode(Error::WsProtocolError), kCloseProtocolError);
                return;
            }
        } else if (opcode != WsOpcode::Continuation && opcode != WsOpcode::Text && opcode != WsOpcode::Binary) {
            this->fail(makeErrorCode(Error::WsProtocolError), kCloseProtocolError);
            return;
        }
        if (!control && (length > maxMessageSize_ || (assembling_ && assemblySize_ + length > maxMessageSize_))) {
            this->fail(makeErrorCode(Error::WsMessageTooBig), kCloseMessageTooBig);
            return;
        }

        std::uint64_t const frameEnd = parsePos_ + headerSize + length;
        if (frameEnd > streamHead_ + tcp_.rx.fetch().size()) {
            // Incomplete frame. If it can't fit even after the user consumed everything queued,
            // it never will.
            std::uint64_t const pinned = assembling_ ? assemblyBegin_ : parsePos_;
            if (frameEnd - pinned > tcp_.rx.capacity()) {
                this->fail(makeErrorCode(Error::WsMessageTooBig), kCloseMessageTooBig);
                return;
            }
            break;
        }

        std::uint64_t const payload = parsePos_ + headerSize;
        auto const size = static_cast<std::size_t>(length);

        switch (opcode) {
        case WsOpcode::Ping: this->queueControl(WsOpcode::Pong, {this->streamPointer(payload), size}); break;
        case WsOpcode::Pong: break;
        case WsOpcode::Close: {
            std::uint16_t code = kCloseNoStatus;
            auto const* body = reinterpret_cast<unsigned char const*>(this->streamPointer(payload));
            if (size == 1) {
                this->fail(makeErrorCode(Error::WsProtocolError), kCloseProtocolError);
                return;
            }
            if (size >= 2) {
                code = static_cast<std::uint16_t>(body[0] << 8 | body[1]);
                closeReason_.assign(reinterpret_cast<char const*>(body) + 2, size - 2);
            }
            parsePos_ = frameEnd;
            // Echo the status code (an empty Close if there was none) and close.
            std::array<std::byte, 2> echo{static_cast<std::byte>(code >> 8), static_cast<std::byte>(code)};
            this->queueControl(WsOpcode::Close, code == kCloseNoStatus ? std::span<std::byte const>{} : echo);
            this->flushControl();
            error_ = {code, getWsCloseCategory()};
            this->beginClose();
            return;
        }
        case WsOpcode::Text: [[fallthrough]];
        case WsOpcode::Binary:
            if (assembling_) {
                this->fail(makeErrorCode(Error::WsProtocolError), kCloseProtocolError);
                return;
            }
            if (fin) {
                entries_[rxTail_ & entriesMask_] =
                    Entry{parsePos_, payload, tcp_.rx.timestamp(), static_cast<std::uint32_t>(size), opcode};
                ++rxTail_;
            } else {
                assembling_ = true;
                assemblyOpcode_ = opcode;
                assemblyBegin_ = parsePos_;
                assemblyPayload_ = payload;
                assemblySize_ = size;
            }
            break;
        case WsOpcode::Continuation:
            if (!assembling_) {
                this->fail(makeErrorCode(Error::WsProtocolError), kCloseProtocolError);
                return;
            }
            // Join in place: move this fragment right behind the previous ones (over the frame
            // headers and any control frames in between, which are already handled).
            std::memmove(this->streamPointer(assemblyPayload_ + assemblySize_), this->streamPointer(payload), size);
            assemblySize_ += size;
            if (fin) {
                entries_[rxTail_ & entriesMask_] = Entry{assemblyBegin_, assemblyPayload_, tcp_.rx.timestamp(),
                    static_cast<std::uint32_t>(assemblySize_), assemblyOpcode_};
                ++rxTail_;
                assembling_ = false;
            }
            break;
        }
        parsePos_ = frameEnd;
    }

    // Nothing held by the user: control frames and headers parsed so far can be released.
    if (rxHead_ == rxTail_ && !assembling_) {
        this->releaseTo(parsePos_);
    }
}

// --- tx -----------------------------------------------------------------------------------------

auto WsCore::nextMaskKey() noexcept -> std::uint32_t {
    if (options_.masking == WsMasking::Zero) {
        return 0;
    }
    // xorshift64*
    prngState_ ^= prngState_ >> 12;
    prngState_ ^= prngState_ << 25;
    prngState_ ^= prngState_ >> 27;
    auto key = static_cast<std::uint32_t>((prngState_ * 0x2545F4914F6CDD1Dull) >> 32);
    return key != 0 ? key : 0x5A5A5A5A;
}

auto WsCore::prepareMessage(std::size_t size) noexcept -> std::span<std::byte> {
    if (state_ != ConnectionState::Ready) [[unlikely]] {
        return {};
    }
    if (!pendingControl_.empty()) [[unlikely]] {
        this->flushControl();
    }
    auto const header = detail::wsClientHeaderSize(size);
    auto buffer = tcp_.tx.prepare(header + size);
    if (buffer.size() != header + size) [[unlikely]] {
        return {};
    }
    reservation_ = buffer.data();
    reservedHeader_ = header;
    prepared_ = size;
    return buffer.subspan(header, size);
}

void WsCore::commitMessage(std::size_t size, WsOpcode opcode) noexcept {
    assert(size <= prepared_ && reservation_ != nullptr);
    auto const header = detail::wsClientHeaderSize(size);
    if (header != reservedHeader_) [[unlikely]] {
        // Committed size fell into a smaller length encoding than reserved: close the gap.
        std::memmove(reservation_ + header, reservation_ + reservedHeader_, size);
    }
    auto const key = this->nextMaskKey();
    detail::writeWsClientHeader(reservation_, true, static_cast<std::uint8_t>(opcode), size, key);
    detail::applyWsMask(reservation_ + header, size, key);
    tcp_.tx.commit(header + size);
    reservation_ = nullptr;
    prepared_ = 0;
    if (!pendingControl_.empty()) [[unlikely]] {
        this->flushControl();
    }
}

void WsCore::flushTx() noexcept {
    this->flushControl();
    tcp_.tx.flush();
}

void WsCore::queueControl(WsOpcode opcode, std::span<std::byte const> payload) noexcept {
    assert(payload.size() <= 125);
    if (pendingControl_.size() + detail::kWsMaxClientHeaderSize + payload.size() > kMaxPendingControl) {
        return; // ping flood while a prepare() is open: drop
    }
    auto const offset = pendingControl_.size();
    pendingControl_.resize(offset + detail::wsClientHeaderSize(payload.size()) + payload.size());
    auto* out = pendingControl_.data() + offset;
    auto const key = this->nextMaskKey();
    auto const header = detail::writeWsClientHeader(out, true, static_cast<std::uint8_t>(opcode), payload.size(), key);
    if (!payload.empty()) {
        std::memcpy(out + header, payload.data(), payload.size());
    }
    detail::applyWsMask(out + header, payload.size(), key);
}

void WsCore::flushControl() noexcept {
    // Control frames go between messages, never inside an open prepare().
    if (pendingControl_.empty() || prepared_ != 0) {
        return;
    }
    if (tcp_.tx.push(pendingControl_)) {
        pendingControl_.clear();
        tcp_.tx.flush();
    }
}

// --- closing ------------------------------------------------------------------------------------

void WsCore::fail(std::error_code ec, std::uint16_t closeCode) noexcept {
    if (state_ == ConnectionState::Closing || state_ == ConnectionState::Closed) {
        return;
    }
    error_ = ec;
    if (closeCode != 0 && state_ == ConnectionState::Ready) {
        std::array<std::byte, 2> payload{static_cast<std::byte>(closeCode >> 8), static_cast<std::byte>(closeCode)};
        this->queueControl(WsOpcode::Close, payload);
        prepared_ = 0;
        this->flushControl();
    }
    this->beginClose();
}

void WsCore::beginClose() noexcept {
    state_ = ConnectionState::Closing;
    reservation_ = nullptr;
    prepared_ = 0;
    this->disarmTimeout();
    tcp_.close(); // may call onStreamClosed() right away
    this->finishCloseIfDone();
}

void WsCore::finishCloseIfDone() noexcept {
    if (state_ == ConnectionState::Closing && inflight_ == 0 && tcp_.state() == ConnectionState::Closed) {
        state_ = ConnectionState::Closed;
    }
}

void WsCore::armTimeout() noexcept {
    auto const seconds = std::chrono::duration_cast<std::chrono::seconds>(options_.handshakeTimeout);
    timeout_.tv_sec = seconds.count();
    timeout_.tv_nsec =
        std::chrono::duration_cast<std::chrono::nanoseconds>(options_.handshakeTimeout - seconds).count();
    auto* sqe = ring_.getSqe();
    if (!sqe) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull), 0);
        return;
    }
    ::io_uring_prep_timeout(sqe, &timeout_, 0, 0);
    ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::ConnectTimeout));
    timeoutArmed_ = true;
    ++inflight_;
}

void WsCore::disarmTimeout() noexcept {
    if (!timeoutArmed_) {
        return;
    }
    if (auto* sqe = ring_.getSqe(); sqe) {
        ::io_uring_prep_timeout_remove(sqe, detail::encodeUserData(this, detail::OpCode::ConnectTimeout), 0);
        ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::Cancel));
        ++inflight_;
        timeoutArmed_ = false; // its CQE (-ECANCELED) still arrives and is counted in inflight_
    }
}

} // namespace detail

WsConnection::WsConnection(Reactor& reactor, WsOptions options)
    : core_{detail::WsCore::create(reactor.ring(), std::move(options))}, rx{core_->rx}, tx{core_->tx} {
    reactor.ring().attach(core_);
}

} // namespace turboq::reactor
