// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "Tls.h"

#include <linux/capability.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/kdf.h>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>

#include <array>
#include <cstring>
#include <fstream>
#include <string>

#include "../Error.h"
#include "../TcpConnection.h"

namespace turboq::reactor {
namespace {

// Ciphers implemented by the kernel TLS module. ChaCha20-Poly1305 needs Linux 5.11.
constexpr char const* kTls13CipherSuites = "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256";
constexpr char const* kTls12CipherList = "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
                                         "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
                                         "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305";

struct TlsErrorCategory final : public std::error_category {
    [[nodiscard]] auto name() const noexcept -> char const* override {
        return "turboq::reactor::TlsError";
    }

    [[nodiscard]] auto message(int value) const -> std::string override {
        auto const lib = static_cast<unsigned>(value) >> 23;
        auto const reason = static_cast<unsigned>(value) & 0x7FFFFF;
        auto const packed = ERR_PACK(lib, 0, reason);
        char const* libString = ::ERR_lib_error_string(packed);
        char const* reasonString = ::ERR_reason_error_string(packed);
        std::string result = libString ? libString : "OpenSSL";
        result += ": ";
        result += reasonString ? reasonString : ("reason " + std::to_string(reason));
        return result;
    }
};

struct KernelTlsRxErrorCategory final : public std::error_category {
    [[nodiscard]] auto name() const noexcept -> char const* override {
        return "turboq::reactor::KernelTlsRxError";
    }

    [[nodiscard]] auto message(int value) const -> std::string override {
        return "the kernel refused the TLS receive key: " + std::generic_category().message(value);
    }
};

struct X509ErrorCategory final : public std::error_category {
    [[nodiscard]] auto name() const noexcept -> char const* override {
        return "turboq::reactor::X509Error";
    }

    [[nodiscard]] auto message(int value) const -> std::string override {
        return std::string{"certificate verification failed: "} + ::X509_verify_cert_error_string(value);
    }
};

struct TlsAlertCategory final : public std::error_category {
    [[nodiscard]] auto name() const noexcept -> char const* override {
        return "turboq::reactor::TlsAlert";
    }

    [[nodiscard]] auto message(int value) const -> std::string override {
        return std::string{"TLS alert received: "} + ::SSL_alert_desc_string_long(value);
    }
};

/// HKDF-Expand-Label(secret, label, "", length) from RFC 8446 section 7.1.
[[nodiscard]] auto hkdfExpandLabel(char const* digest, std::span<std::uint8_t const> secret, std::string_view label,
    std::span<std::uint8_t> out) noexcept -> bool {
    // HkdfLabel: uint16 length | opaque label<7..255> = "tls13 " + label | opaque context<0..255> = ""
    std::array<std::uint8_t, 2 + 1 + 255 + 1> info{};
    std::size_t infoSize = 0;
    info[infoSize++] = static_cast<std::uint8_t>(out.size() >> 8);
    info[infoSize++] = static_cast<std::uint8_t>(out.size());
    constexpr std::string_view kPrefix = "tls13 ";
    info[infoSize++] = static_cast<std::uint8_t>(kPrefix.size() + label.size());
    std::memcpy(info.data() + infoSize, kPrefix.data(), kPrefix.size());
    infoSize += kPrefix.size();
    std::memcpy(info.data() + infoSize, label.data(), label.size());
    infoSize += label.size();
    info[infoSize++] = 0;

    EVP_KDF* kdf = ::EVP_KDF_fetch(nullptr, "HKDF", nullptr);
    if (!kdf) {
        return false;
    }
    EVP_KDF_CTX* ctx = ::EVP_KDF_CTX_new(kdf);
    ::EVP_KDF_free(kdf);
    if (!ctx) {
        return false;
    }
    int mode = EVP_KDF_HKDF_MODE_EXPAND_ONLY;
    OSSL_PARAM params[] = {
        ::OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, const_cast<char*>(digest), 0),
        ::OSSL_PARAM_construct_int(OSSL_KDF_PARAM_MODE, &mode),
        ::OSSL_PARAM_construct_octet_string(
            OSSL_KDF_PARAM_KEY, const_cast<std::uint8_t*>(secret.data()), secret.size()),
        ::OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_INFO, info.data(), infoSize),
        ::OSSL_PARAM_construct_end(),
    };
    bool const ok = ::EVP_KDF_derive(ctx, out.data(), out.size(), params) == 1;
    ::EVP_KDF_CTX_free(ctx);
    return ok;
}

void storeBigEndian(std::uint64_t value, unsigned char* out) noexcept {
    for (int i = 7; i >= 0; --i) {
        out[i] = static_cast<unsigned char>(value);
        value >>= 8;
    }
}

} // namespace

auto getTlsErrorCategory() noexcept -> std::error_category const& {
    static TlsErrorCategory category;
    return category;
}

auto getKernelTlsRxErrorCategory() noexcept -> std::error_category const& {
    static KernelTlsRxErrorCategory category;
    return category;
}

auto isKernelTlsError(std::error_code ec) noexcept -> bool {
    if (ec.category() == getKernelTlsRxErrorCategory()) {
        return true;
    }
    if (ec.category() != getErrorCategory()) {
        return false;
    }
    switch (static_cast<Error>(ec.value())) {
    case Error::KernelTlsModuleMissing:
    case Error::OpenSslWithoutKtls:
    case Error::KernelTlsSendUnavailable:
    case Error::KernelTlsReceiveUnavailable:
    case Error::KernelTlsCipherUnsupported: return true;
    default: return false;
    }
}

auto describeKernelTlsSupport() -> std::string {
    auto const support = detail::probeKernelTlsSupport();
    std::string result = ::OpenSSL_version(OPENSSL_VERSION);
    result += support.opensslKtls ? " (built with ktls)" : " (built WITHOUT ktls)";
    result += "; tls kernel module: ";
    if (support.moduleLoaded) {
        result += "loaded";
    } else if (support.canLoadModule) {
        result += "not loaded (this process may load it on first use: CAP_NET_ADMIN)";
    } else {
        result += "NOT loaded (sudo modprobe tls)";
    }
    return result;
}

auto getX509ErrorCategory() noexcept -> std::error_category const& {
    static X509ErrorCategory category;
    return category;
}

auto getTlsAlertCategory() noexcept -> std::error_category const& {
    static TlsAlertCategory category;
    return category;
}

namespace detail {

auto probeKernelTlsSupport() noexcept -> KernelTlsSupport {
    KernelTlsSupport support{};
#ifndef OPENSSL_NO_KTLS
    support.opensslKtls = true;
#endif
    try {
        std::ifstream file{"/proc/sys/net/ipv4/tcp_available_ulp"};
        std::string ulp;
        while (file >> ulp) {
            support.moduleLoaded = support.moduleLoaded || ulp == "tls";
        }
    } catch (...) {}
    __user_cap_header_struct header{};
    header.version = _LINUX_CAPABILITY_VERSION_3;
    __user_cap_data_struct data[_LINUX_CAPABILITY_U32S_3]{};
    if (::syscall(SYS_capget, &header, data) == 0) {
        support.canLoadModule = (data[CAP_TO_INDEX(CAP_NET_ADMIN)].effective & CAP_TO_MASK(CAP_NET_ADMIN)) != 0;
    }
    return support;
}

auto popTlsError(std::error_code fallback) noexcept -> std::error_code {
    // The first error in the queue is the root cause, later ones add context.
    unsigned long const error = ::ERR_get_error();
    ::ERR_clear_error();
    if (error == 0) {
        return fallback;
    }
    auto const value = static_cast<int>(
        (static_cast<unsigned>(ERR_GET_LIB(error)) << 23) | (static_cast<unsigned>(ERR_GET_REASON(error)) & 0x7FFFFF));
    return {value, getTlsErrorCategory()};
}

auto createClientContext(TlsOptions const& options) noexcept -> std::expected<ssl_ctx_st*, std::error_code> {
    SSL_CTX* ctx = ::SSL_CTX_new(::TLS_client_method());
    if (!ctx) {
        return std::unexpected(popTlsError(makeErrorCode(Error::TlsHandshakeFailed)));
    }
    auto fail = [&]() {
        ::SSL_CTX_free(ctx);
        return std::unexpected(popTlsError(makeErrorCode(Error::InvalidOptions)));
    };

    int const minVersion = options.minVersion == TlsVersion::Tls13 ? TLS1_3_VERSION : TLS1_2_VERSION;
    int const maxVersion = options.maxVersion == TlsVersion::Tls12 ? TLS1_2_VERSION : TLS1_3_VERSION;
    if (minVersion > maxVersion || ::SSL_CTX_set_min_proto_version(ctx, minVersion) != 1 ||
        ::SSL_CTX_set_max_proto_version(ctx, maxVersion) != 1) {
        return fail();
    }
    if (::SSL_CTX_set_ciphersuites(ctx, kTls13CipherSuites) != 1 ||
        ::SSL_CTX_set_cipher_list(ctx, kTls12CipherList) != 1) {
        return fail();
    }

    ::SSL_CTX_set_options(ctx, SSL_OP_ENABLE_KTLS | SSL_OP_NO_RENEGOTIATION | SSL_OP_NO_COMPRESSION);
    // No session resumption: tickets would only arrive after the kernel took over the read side.
    ::SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);

    if (options.verifyPeer) {
        ::SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
        if (options.caFile.empty() && options.caPath.empty()) {
            if (::SSL_CTX_set_default_verify_paths(ctx) != 1) {
                return fail();
            }
        } else if (::SSL_CTX_load_verify_locations(ctx, options.caFile.empty() ? nullptr : options.caFile.c_str(),
                       options.caPath.empty() ? nullptr : options.caPath.c_str()) != 1) {
            return fail();
        }
    } else {
        ::SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    }
    return ctx;
}

void freeContext(ssl_ctx_st* ctx) noexcept {
    ::SSL_CTX_free(ctx);
}

void freeSsl(ssl_st* ssl) noexcept {
    ::SSL_free(ssl);
}

auto makeTls13CryptoInfo(std::uint16_t cipherSuite, std::span<std::uint8_t const> trafficSecret,
    std::uint64_t sequence) noexcept -> std::expected<KernelTlsCryptoInfo, std::error_code> {
    KernelTlsCryptoInfo result;

    char const* digest = nullptr;
    std::size_t keySize = 0;
    switch (cipherSuite) {
    case 0x1301:
        digest = "SHA256";
        keySize = TLS_CIPHER_AES_GCM_128_KEY_SIZE;
        break;
    case 0x1302:
        digest = "SHA384";
        keySize = TLS_CIPHER_AES_GCM_256_KEY_SIZE;
        break;
    case 0x1303:
        digest = "SHA256";
        keySize = TLS_CIPHER_CHACHA20_POLY1305_KEY_SIZE;
        break;
    default: return std::unexpected(makeErrorCode(Error::KernelTlsCipherUnsupported));
    }

    std::array<std::uint8_t, 32> key{};
    std::array<std::uint8_t, 12> iv{}; // TLS 1.3 per-record nonce base, 12 bytes for all three AEADs
    if (!hkdfExpandLabel(digest, trafficSecret, "key", std::span{key}.first(keySize)) ||
        !hkdfExpandLabel(digest, trafficSecret, "iv", iv)) {
        ::OPENSSL_cleanse(key.data(), key.size());
        return std::unexpected(popTlsError(makeErrorCode(Error::KernelTlsReceiveUnavailable)));
    }

    // The kernel splits the nonce base into a 4-byte salt and an 8-byte "iv" for the GCM ciphers,
    // and takes all 12 bytes as iv for ChaCha20-Poly1305 (no salt).
    auto store = [&result](auto const& info) {
        static_assert(sizeof(info) <= sizeof(result.bytes));
        std::memcpy(result.bytes.data(), &info, sizeof(info));
        result.size = sizeof(info);
    };
    switch (cipherSuite) {
    case 0x1301: {
        tls12_crypto_info_aes_gcm_128 info{};
        info.info.version = TLS_1_3_VERSION;
        info.info.cipher_type = TLS_CIPHER_AES_GCM_128;
        std::memcpy(info.key, key.data(), sizeof(info.key));
        std::memcpy(info.salt, iv.data(), sizeof(info.salt));
        std::memcpy(info.iv, iv.data() + sizeof(info.salt), sizeof(info.iv));
        storeBigEndian(sequence, info.rec_seq);
        store(info);
        ::OPENSSL_cleanse(&info, sizeof(info));
        break;
    }
    case 0x1302: {
        tls12_crypto_info_aes_gcm_256 info{};
        info.info.version = TLS_1_3_VERSION;
        info.info.cipher_type = TLS_CIPHER_AES_GCM_256;
        std::memcpy(info.key, key.data(), sizeof(info.key));
        std::memcpy(info.salt, iv.data(), sizeof(info.salt));
        std::memcpy(info.iv, iv.data() + sizeof(info.salt), sizeof(info.iv));
        storeBigEndian(sequence, info.rec_seq);
        store(info);
        ::OPENSSL_cleanse(&info, sizeof(info));
        break;
    }
    case 0x1303: {
        tls12_crypto_info_chacha20_poly1305 info{};
        info.info.version = TLS_1_3_VERSION;
        info.info.cipher_type = TLS_CIPHER_CHACHA20_POLY1305;
        std::memcpy(info.key, key.data(), sizeof(info.key));
        std::memcpy(info.iv, iv.data(), sizeof(info.iv));
        storeBigEndian(sequence, info.rec_seq);
        store(info);
        ::OPENSSL_cleanse(&info, sizeof(info));
        break;
    }
    }
    ::OPENSSL_cleanse(key.data(), key.size());
    ::OPENSSL_cleanse(iv.data(), iv.size());
    return result;
}

} // namespace detail
} // namespace turboq::reactor
