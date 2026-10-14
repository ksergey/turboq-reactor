// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

// Everything of turboq-reactor that is linked: the headers of a module are reachable only when its
// target (turboq-reactor::uring, ::ssl, ::websocket, ::xdp) is linked.

#pragma once

#include <turboq/reactor/Address.h>
#include <turboq/reactor/Error.h>
#include <turboq/reactor/Reactor.h>
#include <turboq/reactor/TCPConnection.h>
#include <turboq/reactor/UDPConnection.h>

#if __has_include(<turboq/reactor/TLSConnection.h>)
#include <turboq/reactor/TLSConnection.h>
#endif
#if __has_include(<turboq/reactor/WebsocketConnection.h>)
#include <turboq/reactor/WebsocketConnection.h>
#endif
#if __has_include(<turboq/reactor/XDPConnection.h>)
#include <turboq/reactor/XDPConnection.h>
#endif
