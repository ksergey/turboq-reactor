// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

namespace turboq::reactor::detail {

/// Internal hook for protocol layers built on top of a TCPConnection (WebSocket). Called by the
/// connection from within Reactor::poll(); never exposed to user code.
class StreamObserver {
public:
    virtual ~StreamObserver() = default;

    /// The stream became Ready (TCP connected, TLS handshake done).
    virtual void onStreamReady() noexcept = 0;

    /// New bytes were appended to the stream's rx ring.
    virtual void onStreamData() noexcept = 0;

    /// The stream reached Closed.
    virtual void onStreamClosed() noexcept = 0;
};

} // namespace turboq::reactor::detail
