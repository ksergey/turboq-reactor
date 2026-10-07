// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <linux/tls.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include <doctest/doctest.h>

#include "Error.h"
#include "Reactor.h"
#include "detail/Tls.h"

namespace turboq::reactor::testing {
namespace {

using namespace std::chrono_literals;

// The OpenSSL test server writes with write(): don't let a client that went away kill the test.
[[maybe_unused]] auto const kIgnoreSigPipe = std::signal(SIGPIPE, SIG_IGN);

[[nodiscard]] auto kernelTlsAvailable() -> bool {
    std::ifstream file{"/proc/sys/net/ipv4/tcp_available_ulp"};
    std::string ulp;
    while (file >> ulp) {
        if (ulp == "tls") {
            return true;
        }
    }
    return false;
}

template <typename Pred>
[[nodiscard]] auto pollUntil(Reactor& reactor, Pred pred, std::chrono::milliseconds timeout = 5000ms) -> bool {
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

[[nodiscard]] auto asBytes(std::string_view data) -> std::span<std::byte const> {
    return {reinterpret_cast<std::byte const*>(data.data()), data.size()};
}

/// Self-signed P-256 certificate for "localhost" / 127.0.0.1, written to a temporary PEM file so it
/// can be passed to the client as caFile.
class TestCertificate {
private:
    EVP_PKEY* key_{nullptr};
    X509* cert_{nullptr};
    std::filesystem::path pemPath_;

public:
    TestCertificate() {
        key_ = ::EVP_EC_gen("P-256");
        REQUIRE(key_ != nullptr);
        cert_ = ::X509_new();
        ::X509_set_version(cert_, 2);
        ::ASN1_INTEGER_set(::X509_get_serialNumber(cert_), 1);
        ::X509_gmtime_adj(::X509_getm_notBefore(cert_), -3600);
        ::X509_gmtime_adj(::X509_getm_notAfter(cert_), 24 * 3600);
        ::X509_set_pubkey(cert_, key_);
        X509_NAME* name = ::X509_get_subject_name(cert_);
        ::X509_NAME_add_entry_by_txt(
            name, "CN", MBSTRING_ASC, reinterpret_cast<unsigned char const*>("localhost"), -1, -1, 0);
        ::X509_set_issuer_name(cert_, name);

        X509V3_CTX ctx;
        X509V3_set_ctx_nodb(&ctx);
        ::X509V3_set_ctx(&ctx, cert_, cert_, nullptr, nullptr, 0);
        for (auto [nid, value] : {std::pair{NID_subject_alt_name, "DNS:localhost,IP:127.0.0.1"},
                 std::pair{NID_basic_constraints, "critical,CA:TRUE"}}) {
            X509_EXTENSION* ext = ::X509V3_EXT_conf_nid(nullptr, &ctx, nid, value);
            REQUIRE(ext != nullptr);
            ::X509_add_ext(cert_, ext, -1);
            ::X509_EXTENSION_free(ext);
        }
        REQUIRE(::X509_sign(cert_, key_, ::EVP_sha256()) > 0);

        char path[] = "/tmp/turboq-reactor-ca-XXXXXX";
        int const fd = ::mkstemp(path);
        REQUIRE(fd >= 0);
        FILE* file = ::fdopen(fd, "w");
        REQUIRE(::PEM_write_X509(file, cert_) == 1);
        ::fclose(file);
        pemPath_ = path;
    }

    ~TestCertificate() {
        std::error_code ec;
        std::filesystem::remove(pemPath_, ec);
        ::X509_free(cert_);
        ::EVP_PKEY_free(key_);
    }

    [[nodiscard]] auto pemPath() const -> std::string {
        return pemPath_.string();
    }

    /// Server context; `cipherSuites` restricts TLS 1.3 suites, maxVersion caps the protocol.
    [[nodiscard]] auto serverContext(
        char const* cipherSuites = nullptr, int maxVersion = TLS1_3_VERSION) const -> SSL_CTX* {
        SSL_CTX* ctx = ::SSL_CTX_new(::TLS_server_method());
        REQUIRE(ctx != nullptr);
        REQUIRE(::SSL_CTX_use_certificate(ctx, cert_) == 1);
        REQUIRE(::SSL_CTX_use_PrivateKey(ctx, key_) == 1);
        ::SSL_CTX_set_max_proto_version(ctx, maxVersion);
        if (cipherSuites) {
            REQUIRE(::SSL_CTX_set_ciphersuites(ctx, cipherSuites) == 1);
        }
        return ctx;
    }
};

/// Loopback TCP listener; with a server context it runs a blocking OpenSSL server in a thread:
/// accept, SSL_accept, then `script`.
class TlsServer {
private:
    int listener_{-1};
    std::uint16_t port_{0};
    std::thread thread_;
    SSL_CTX* ctx_{nullptr};

public:
    std::atomic<bool> handshakeOk{false};
    std::atomic<bool> scriptOk{false};

    using Script = std::function<bool(SSL*)>;

    explicit TlsServer(SSL_CTX* ctx = nullptr, Script script = {}) : ctx_{ctx} {
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

        if (ctx_) {
            thread_ = std::thread{[this, script = std::move(script)] {
                int const fd = ::accept4(listener_, nullptr, nullptr, SOCK_CLOEXEC);
                if (fd < 0) {
                    return;
                }
                SSL* ssl = ::SSL_new(ctx_);
                ::SSL_set_fd(ssl, fd);
                if (::SSL_accept(ssl) == 1) {
                    handshakeOk = true;
                    if (script) {
                        scriptOk = script(ssl);
                    }
                }
                ::SSL_free(ssl);
                ::close(fd);
                ::ERR_clear_error();
            }};
        }
    }

    ~TlsServer() {
        this->join();
        ::close(listener_);
        ::SSL_CTX_free(ctx_);
    }

    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return port_;
    }

    /// Accept and drop a TCP connection (for "peer closes during handshake").
    void acceptAndClose() const {
        int const fd = ::accept4(listener_, nullptr, nullptr, SOCK_CLOEXEC);
        REQUIRE(fd >= 0);
        ::close(fd);
    }
};

[[nodiscard]] auto readExactly(SSL* ssl, std::size_t size) -> std::string {
    std::string result(size, '\0');
    std::size_t done = 0;
    while (done < size) {
        int const rc = ::SSL_read(ssl, result.data() + done, static_cast<int>(size - done));
        if (rc <= 0) {
            return {};
        }
        done += static_cast<std::size_t>(rc);
    }
    return result;
}

} // namespace

TEST_SUITE("TlsConnection") {

    TEST_CASE("handshake, then kernel TLS takes over (or reports it can't)") {
        TestCertificate certificate;
        bool const ktls = kernelTlsAvailable();
        if (!ktls) {
            MESSAGE("kernel TLS is not available here: checking the handshake and the failure report only");
        }

        struct Variant {
            char const* name;
            int serverMaxVersion;
            char const* serverCipherSuites;
            std::string_view expectedVersion;
        };
        for (auto const& variant : {Variant{"tls1.3 aes128", TLS1_3_VERSION, "TLS_AES_128_GCM_SHA256", "TLSv1.3"},
                 Variant{"tls1.3 aes256", TLS1_3_VERSION, "TLS_AES_256_GCM_SHA384", "TLSv1.3"},
                 Variant{"tls1.3 chacha", TLS1_3_VERSION, "TLS_CHACHA20_POLY1305_SHA256", "TLSv1.3"},
                 Variant{"tls1.2", TLS1_2_VERSION, nullptr, "TLSv1.2"}}) {
            CAPTURE(variant.name);
            TlsServer server{
                certificate.serverContext(variant.serverCipherSuites, variant.serverMaxVersion), [](SSL* ssl) {
                    // Session tickets (TLS 1.3) go out before this: the client must skip them.
                    if (::SSL_write(ssl, "hello from server", 17) != 17) {
                        return false;
                    }
                    if (readExactly(ssl, 17) != "hello from client") {
                        return false;
                    }
                    ::SSL_shutdown(ssl); // close_notify
                    char byte;
                    return ::SSL_read(ssl, &byte, 1) <= 0;
                }};

            Reactor reactor;
            TcpConnection conn{
                reactor, {.host = "127.0.0.1",
                             .port = server.port(),
                             .tls = {.enabled = true, .serverName = "localhost", .caFile = certificate.pemPath()}}};
            REQUIRE(conn.connect());

            // Written while connecting/handshaking: must go out once the connection is Ready.
            REQUIRE(conn.tx.push(asBytes("hello from client")));

            REQUIRE(pollUntil(reactor, [&] {
                return conn.state() == ConnectionState::Ready || conn.state() == ConnectionState::Closed;
            }));
            REQUIRE_EQ(conn.tlsVersion(), variant.expectedVersion);
            REQUIRE_FALSE(conn.tlsCipher().empty());

            if (!ktls) {
                REQUIRE_EQ(conn.state(), ConnectionState::Closed);
                REQUIRE_EQ(conn.error(), makeErrorCode(Error::KernelTlsUnavailable));
                server.join();
                REQUIRE(server.handshakeOk);
                continue;
            }

            REQUIRE_EQ(conn.state(), ConnectionState::Ready);
            REQUIRE(pollUntil(reactor, [&] {
                return conn.rx.fetch().size() >= 17;
            }));
            REQUIRE_EQ(asString(conn.rx.fetch()), "hello from server");
            conn.rx.consume();

            // The server answers our message with close_notify.
            REQUIRE(pollUntil(reactor, [&] {
                return conn.state() == ConnectionState::Closed;
            }));
            REQUIRE_EQ(conn.error(), makeErrorCode(Error::ClosedByPeer));
            server.join();
            REQUIRE(server.scriptOk);
        }
    }

    TEST_CASE("user close sends close_notify") {
        if (!kernelTlsAvailable()) {
            MESSAGE("kernel TLS is not available here, skipping");
            return;
        }
        TestCertificate certificate;
        TlsServer server{certificate.serverContext(), [](SSL* ssl) {
                             char byte;
                             int const rc = ::SSL_read(ssl, &byte, 1);
                             return rc <= 0 && ::SSL_get_error(ssl, rc) == SSL_ERROR_ZERO_RETURN;
                         }};
        Reactor reactor;
        TcpConnection conn{reactor,
            {.host = "127.0.0.1", .port = server.port(), .tls = {.enabled = true, .caFile = certificate.pemPath()}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Ready;
        }));
        conn.close();
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        server.join();
        REQUIRE(server.scriptOk);
    }

    TEST_CASE("untrusted certificate is rejected") {
        TestCertificate certificate;
        TlsServer server{certificate.serverContext()};
        Reactor reactor;
        // Default trust store: the self-signed test certificate is not in it.
        TcpConnection conn{reactor, {.host = "127.0.0.1", .port = server.port(), .tls = {.enabled = true}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(conn.error().category(), getX509ErrorCategory());
        REQUIRE_EQ(conn.error().value(), X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT);
    }

    TEST_CASE("certificate name mismatch is rejected") {
        TestCertificate certificate;
        TlsServer server{certificate.serverContext()};
        Reactor reactor;
        TcpConnection conn{
            reactor, {.host = "127.0.0.1",
                         .port = server.port(),
                         .tls = {.enabled = true, .serverName = "wrong.example", .caFile = certificate.pemPath()}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(conn.error(), std::error_code(X509_V_ERR_HOSTNAME_MISMATCH, getX509ErrorCategory()));
    }

    TEST_CASE("IP address is verified against the certificate's IP SAN") {
        TestCertificate certificate;
        TlsServer server{certificate.serverContext()};
        Reactor reactor;
        TcpConnection conn{reactor,
            {.host = "127.0.0.1", .port = server.port(), .tls = {.enabled = true, .caFile = certificate.pemPath()}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Ready || conn.state() == ConnectionState::Closed;
        }));
        server.join();
        REQUIRE(server.handshakeOk);
        if (conn.state() == ConnectionState::Closed) {
            REQUIRE_EQ(conn.error(), makeErrorCode(Error::KernelTlsUnavailable));
        }
    }

    TEST_CASE("verification can be disabled") {
        TestCertificate certificate;
        TlsServer server{certificate.serverContext()};
        Reactor reactor;
        TcpConnection conn{
            reactor, {.host = "127.0.0.1", .port = server.port(), .tls = {.enabled = true, .verifyPeer = false}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Ready || conn.state() == ConnectionState::Closed;
        }));
        server.join();
        REQUIRE(server.handshakeOk);
    }

    TEST_CASE("handshake timeout") {
        TlsServer silent; // accepts TCP (kernel backlog), never speaks TLS
        Reactor reactor;
        TcpConnection conn{
            reactor, {.host = "127.0.0.1", .port = silent.port(), .tls = {.enabled = true, .handshakeTimeout = 200ms}}};
        REQUIRE(conn.connect());
        auto const start = std::chrono::steady_clock::now();
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(conn.error(), makeErrorCode(Error::TlsHandshakeTimeout));
        REQUIRE_GE(std::chrono::steady_clock::now() - start, 190ms);
    }

    TEST_CASE("peer closing during the handshake fails the connection") {
        TlsServer server;
        Reactor reactor;
        TcpConnection conn{reactor, {.host = "127.0.0.1", .port = server.port(), .tls = {.enabled = true}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Handshaking;
        }));
        server.acceptAndClose();
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE(conn.error());
    }

    TEST_CASE("user close during the handshake") {
        TlsServer silent;
        Reactor reactor;
        TcpConnection conn{reactor, {.host = "127.0.0.1", .port = silent.port(), .tls = {.enabled = true}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Handshaking;
        }));
        conn.close();
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE_FALSE(conn.error());
    }

    // The kernel receive side for TLS 1.3 is installed by us (OpenSSL < 3.2 does not do it). Check
    // the derived parameters by decrypting what a real OpenSSL server sends after the handshake.
    TEST_CASE("TLS 1.3 kernel crypto info decrypts the server's records") {
        TestCertificate certificate;

        struct Suite {
            char const* name;
            std::uint16_t id;
            EVP_CIPHER const* (*cipher)();
        };
        for (auto const& suite : {Suite{"TLS_AES_128_GCM_SHA256", 0x1301, &::EVP_aes_128_gcm},
                 Suite{"TLS_AES_256_GCM_SHA384", 0x1302, &::EVP_aes_256_gcm},
                 Suite{"TLS_CHACHA20_POLY1305_SHA256", 0x1303, &::EVP_chacha20_poly1305}}) {
            CAPTURE(suite.name);

            // In-memory handshake between two OpenSSL endpoints.
            std::vector<std::uint8_t> serverSecret;
            SSL_CTX* clientCtx = ::SSL_CTX_new(::TLS_client_method());
            ::SSL_CTX_set_min_proto_version(clientCtx, TLS1_3_VERSION);
            ::SSL_CTX_set_ciphersuites(clientCtx, suite.name);
            ::SSL_CTX_set_keylog_callback(clientCtx, [](SSL const* ssl, char const* line) {
                std::string_view text{line};
                constexpr std::string_view kLabel = "SERVER_TRAFFIC_SECRET_0 ";
                if (!text.starts_with(kLabel)) {
                    return;
                }
                text = text.substr(text.find(' ', kLabel.size()) + 1);
                auto* out = static_cast<std::vector<std::uint8_t>*>(SSL_get_app_data(ssl));
                for (std::size_t i = 0; i + 1 < text.size(); i += 2) {
                    out->push_back(static_cast<std::uint8_t>(std::stoi(std::string{text.substr(i, 2)}, nullptr, 16)));
                }
            });
            SSL_CTX* serverCtx = certificate.serverContext(suite.name);

            SSL* client = ::SSL_new(clientCtx);
            SSL* server = ::SSL_new(serverCtx);
            SSL_set_app_data(client, &serverSecret);
            BIO* clientIn = ::BIO_new(::BIO_s_mem());
            BIO* clientOut = ::BIO_new(::BIO_s_mem());
            BIO* serverIn = ::BIO_new(::BIO_s_mem());
            BIO* serverOut = ::BIO_new(::BIO_s_mem());
            ::SSL_set_bio(client, clientIn, clientOut);
            ::SSL_set_bio(server, serverIn, serverOut);
            ::SSL_set_connect_state(client);
            ::SSL_set_accept_state(server);

            auto drain = [](BIO* from) {
                std::vector<std::uint8_t> bytes(static_cast<std::size_t>(BIO_pending(from)));
                if (!bytes.empty()) {
                    ::BIO_read(from, bytes.data(), static_cast<int>(bytes.size()));
                }
                return bytes;
            };

            // Everything the server sends after the client finished its handshake is protected with
            // the server application traffic secret: exactly what the kernel will have to decrypt.
            std::vector<std::uint8_t> afterHandshake;
            bool clientDone = false;
            bool serverDone = false;
            for (int round = 0; round < 20 && !(clientDone && serverDone); ++round) {
                if (!clientDone) {
                    clientDone = ::SSL_do_handshake(client) == 1;
                }
                auto toServer = drain(clientOut);
                ::BIO_write(serverIn, toServer.data(), static_cast<int>(toServer.size()));
                if (!serverDone) {
                    serverDone = ::SSL_do_handshake(server) == 1;
                }
                auto toClient = drain(serverOut);
                if (clientDone) {
                    afterHandshake.insert(afterHandshake.end(), toClient.begin(), toClient.end());
                } else {
                    ::BIO_write(clientIn, toClient.data(), static_cast<int>(toClient.size()));
                }
            }
            REQUIRE(clientDone);
            REQUIRE(serverDone);
            REQUIRE_EQ(::SSL_write(server, "ping", 4), 4);
            auto tail = drain(serverOut);
            afterHandshake.insert(afterHandshake.end(), tail.begin(), tail.end());
            REQUIRE_FALSE(serverSecret.empty());

            auto info = detail::makeTls13CryptoInfo(suite.id, serverSecret, 0);
            REQUIRE(info);

            // Key and 12-byte nonce base as the kernel will see them.
            std::vector<std::uint8_t> key;
            std::array<std::uint8_t, 12> nonceBase{};
            if (suite.id == 0x1303) {
                tls12_crypto_info_chacha20_poly1305 ci;
                REQUIRE_EQ(info->size, sizeof(ci));
                std::memcpy(&ci, info->bytes.data(), sizeof(ci));
                REQUIRE_EQ(ci.info.version, TLS_1_3_VERSION);
                REQUIRE_EQ(ci.info.cipher_type, TLS_CIPHER_CHACHA20_POLY1305);
                key.assign(ci.key, ci.key + sizeof(ci.key));
                std::memcpy(nonceBase.data(), ci.iv, 12);
                REQUIRE(std::all_of(ci.rec_seq, ci.rec_seq + 8, [](auto b) {
                    return b == 0;
                }));
            } else if (suite.id == 0x1302) {
                tls12_crypto_info_aes_gcm_256 ci;
                REQUIRE_EQ(info->size, sizeof(ci));
                std::memcpy(&ci, info->bytes.data(), sizeof(ci));
                REQUIRE_EQ(ci.info.cipher_type, TLS_CIPHER_AES_GCM_256);
                key.assign(ci.key, ci.key + sizeof(ci.key));
                std::memcpy(nonceBase.data(), ci.salt, 4);
                std::memcpy(nonceBase.data() + 4, ci.iv, 8);
            } else {
                tls12_crypto_info_aes_gcm_128 ci;
                REQUIRE_EQ(info->size, sizeof(ci));
                std::memcpy(&ci, info->bytes.data(), sizeof(ci));
                REQUIRE_EQ(ci.info.cipher_type, TLS_CIPHER_AES_GCM_128);
                key.assign(ci.key, ci.key + sizeof(ci.key));
                std::memcpy(nonceBase.data(), ci.salt, 4);
                std::memcpy(nonceBase.data() + 4, ci.iv, 8);
            }

            // Decrypt every record (RFC 8446 5.2/5.3): nonce = base XOR seq, AAD = record header.
            std::vector<std::pair<int, std::string>> records;
            std::size_t offset = 0;
            for (std::uint64_t seq = 0; offset + 5 <= afterHandshake.size(); ++seq) {
                auto const* header = afterHandshake.data() + offset;
                std::size_t const length = std::size_t{header[3]} << 8 | header[4];
                REQUIRE_EQ(header[0], 23); // outer type of every protected record
                REQUIRE_LE(offset + 5 + length, afterHandshake.size());
                REQUIRE_GT(length, 16);
                auto const* body = header + 5;

                auto nonce = nonceBase;
                for (int i = 0; i < 8; ++i) {
                    nonce[4 + i] ^= static_cast<std::uint8_t>(seq >> (56 - 8 * i));
                }
                std::vector<std::uint8_t> plain(length - 16);
                EVP_CIPHER_CTX* ctx = ::EVP_CIPHER_CTX_new();
                int outLength = 0;
                REQUIRE(::EVP_DecryptInit_ex(ctx, suite.cipher(), nullptr, nullptr, nullptr) == 1);
                REQUIRE(::EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_IVLEN, 12, nullptr) == 1);
                REQUIRE(::EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.data(), nonce.data()) == 1);
                REQUIRE(::EVP_DecryptUpdate(ctx, nullptr, &outLength, header, 5) == 1);
                REQUIRE(::EVP_DecryptUpdate(ctx, plain.data(), &outLength, body, static_cast<int>(length - 16)) == 1);
                REQUIRE(::EVP_CIPHER_CTX_ctrl(
                            ctx, EVP_CTRL_AEAD_SET_TAG, 16, const_cast<std::uint8_t*>(body + length - 16)) == 1);
                REQUIRE(::EVP_DecryptFinal_ex(ctx, plain.data() + outLength, &outLength) == 1); // tag verified
                ::EVP_CIPHER_CTX_free(ctx);

                while (!plain.empty() && plain.back() == 0) {
                    plain.pop_back(); // padding
                }
                REQUIRE_FALSE(plain.empty());
                int const innerType = plain.back();
                plain.pop_back();
                records.emplace_back(innerType, std::string{plain.begin(), plain.end()});
                offset += 5 + length;
            }

            // Session tickets (handshake type 4) first, then our data.
            REQUIRE_GE(records.size(), 2);
            for (std::size_t i = 0; i + 1 < records.size(); ++i) {
                REQUIRE_EQ(records[i].first, 22);
                REQUIRE_EQ(static_cast<unsigned char>(records[i].second[0]), 4);
            }
            REQUIRE_EQ(records.back().first, 23);
            REQUIRE_EQ(records.back().second, "ping");

            ::SSL_free(client);
            ::SSL_free(server);
            ::SSL_CTX_free(clientCtx);
            ::SSL_CTX_free(serverCtx);
        }
    }

    TEST_CASE("TLS 1.3 crypto info encodes the record sequence big-endian") {
        std::array<std::uint8_t, 32> secret{};
        auto info = detail::makeTls13CryptoInfo(0x1301, secret, 0x0102030405060708ull);
        REQUIRE(info);
        tls12_crypto_info_aes_gcm_128 ci;
        std::memcpy(&ci, info->bytes.data(), sizeof(ci));
        for (int i = 0; i < 8; ++i) {
            REQUIRE_EQ(ci.rec_seq[i], i + 1);
        }
        REQUIRE_FALSE(detail::makeTls13CryptoInfo(0x1304, secret, 0)); // TLS_AES_128_CCM_SHA256: no kTLS
    }
}

} // namespace turboq::reactor::testing
