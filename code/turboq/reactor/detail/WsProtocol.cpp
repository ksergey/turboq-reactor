// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "WsProtocol.h"

#include <sys/random.h>

#include <openssl/evp.h>

#include <array>
#include <cerrno>
#include <charconv>

#include "../Error.h"

namespace turboq::reactor::detail {
namespace {

[[nodiscard]] auto base64(unsigned char const* data, std::size_t size) -> std::string {
    std::string result(4 * ((size + 2) / 3), '\0');
    int const length = ::EVP_EncodeBlock(reinterpret_cast<unsigned char*>(result.data()), data, static_cast<int>(size));
    result.resize(static_cast<std::size_t>(length));
    return result;
}

[[nodiscard]] auto equalsIgnoreCase(std::string_view a, std::string_view b) noexcept -> bool {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        auto lower = [](char c) {
            return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
        };
        if (lower(a[i]) != lower(b[i])) {
            return false;
        }
    }
    return true;
}

} // namespace

auto parseWsUrl(std::string_view url) -> std::expected<WsUrl, std::error_code> {
    auto const invalid = std::unexpected(makeErrorCode(Error::WsInvalidUrl));
    WsUrl result;

    auto const schemeEnd = url.find("://");
    if (schemeEnd == std::string_view::npos) {
        return invalid;
    }
    auto const scheme = url.substr(0, schemeEnd);
    if (equalsIgnoreCase(scheme, "wss")) {
        result.secure = true;
    } else if (!equalsIgnoreCase(scheme, "ws")) {
        return invalid;
    }
    url.remove_prefix(schemeEnd + 3);

    auto const authorityEnd = url.find_first_of("/?#");
    auto authority = url.substr(0, authorityEnd);
    auto rest = authorityEnd == std::string_view::npos ? std::string_view{} : url.substr(authorityEnd);
    if (authority.empty() || authority.find('@') != std::string_view::npos) {
        return invalid; // userinfo is not supported
    }

    std::string_view portText;
    if (authority.front() == '[') {
        auto const close = authority.find(']');
        if (close == std::string_view::npos) {
            return invalid;
        }
        result.host = std::string{authority.substr(1, close - 1)};
        auto const after = authority.substr(close + 1);
        if (!after.empty()) {
            if (after.front() != ':') {
                return invalid;
            }
            portText = after.substr(1);
        }
    } else {
        auto const colon = authority.rfind(':');
        if (colon != std::string_view::npos) {
            result.host = std::string{authority.substr(0, colon)};
            portText = authority.substr(colon + 1);
        } else {
            result.host = std::string{authority};
        }
    }
    if (result.host.empty()) {
        return invalid;
    }

    std::uint16_t const defaultPort = result.secure ? 443 : 80;
    result.port = defaultPort;
    if (!portText.empty()) {
        auto const [ptr, ec] = std::from_chars(portText.data(), portText.data() + portText.size(), result.port);
        if (ec != std::errc{} || ptr != portText.data() + portText.size() || result.port == 0) {
            return invalid;
        }
    }

    if (auto const fragment = rest.find('#'); fragment != std::string_view::npos) {
        rest = rest.substr(0, fragment);
    }
    result.target = rest.empty() || rest.front() != '/' ? "/" + std::string{rest} : std::string{rest};

    bool const ipv6 = result.host.find(':') != std::string::npos;
    result.hostHeader = ipv6 ? "[" + result.host + "]" : result.host;
    if (result.port != defaultPort) {
        result.hostHeader += ":" + std::to_string(result.port);
    }
    return result;
}

auto computeWsAccept(std::string_view key) -> std::string {
    constexpr std::string_view kGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    std::string input{key};
    input += kGuid;
    std::array<unsigned char, 20> digest{};
    unsigned int digestSize = 0;
    ::EVP_Digest(input.data(), input.size(), digest.data(), &digestSize, ::EVP_sha1(), nullptr);
    return base64(digest.data(), digestSize);
}

auto makeWsKey() -> std::string {
    std::array<unsigned char, 16> nonce{};
    fillRandom(nonce.data(), nonce.size());
    return base64(nonce.data(), nonce.size());
}

void fillRandom(void* data, std::size_t size) noexcept {
    auto* out = static_cast<unsigned char*>(data);
    while (size > 0) {
        auto const rc = ::getrandom(out, size, 0);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return; // not reachable on Linux >= 3.17 with a sane size
        }
        out += rc;
        size -= static_cast<std::size_t>(rc);
    }
}

} // namespace turboq::reactor::detail
