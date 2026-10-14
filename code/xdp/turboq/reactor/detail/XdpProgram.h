// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <system_error>
#include <utility>

namespace turboq::reactor::detail {

/// An owned file descriptor.
class UniqueFd {
private:
    int fd_{-1};

public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept : fd_{fd} {}

    UniqueFd(UniqueFd const&) = delete;
    UniqueFd& operator=(UniqueFd const&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_{std::exchange(other.fd_, -1)} {}

    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            this->reset();
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    ~UniqueFd() noexcept {
        this->reset();
    }

    void reset() noexcept;

    [[nodiscard]] auto get() const noexcept -> int {
        return fd_;
    }

    explicit operator bool() const noexcept {
        return fd_ >= 0;
    }
};

struct XdpProgramOptions {
    unsigned ifindex = 0;
    /// Highest receive queue an AF_XDP socket may be registered for.
    unsigned maxQueue = 0;
    /// Redirect IPv4/IPv6 UDP datagrams to these destination ports, pass the rest to the kernel.
    /// Empty: redirect every frame.
    std::span<std::uint16_t const> udpPorts{};
    bool allowNative = true;
    bool allowGeneric = true;
    /// Try a device-bound program that stores the NIC receive timestamp in front of each frame
    /// (8 bytes of XDP metadata, 0 if the NIC has none); falls back to a plain program.
    bool timestamps = false;
};

/// An XDP program attached to an interface (through a BPF link: detached when this object goes
/// away, also if the process dies) that sends frames to the AF_XDP sockets of an XSKMAP, one per
/// receive queue. Built from raw BPF instructions and loaded with the bpf() syscall: no libbpf.
///
/// Needs CAP_NET_ADMIN and CAP_BPF (or root). Only one XDP program can be attached to an
/// interface: attaching fails with EBUSY if another one is already there.
class XdpProgram {
private:
    UniqueFd xskMap_;
    UniqueFd portMap_;
    UniqueFd program_;
    UniqueFd link_;
    bool native_{false};
    bool timestamps_{false};

    XdpProgram() = default;

public:
    XdpProgram(XdpProgram&&) noexcept = default;
    XdpProgram& operator=(XdpProgram&&) noexcept = default;

    /// Load and attach. On failure `diagnostic` explains (verifier log, attach error).
    [[nodiscard]] static auto attach(
        XdpProgramOptions const& options, std::string& diagnostic) -> std::expected<XdpProgram, std::error_code>;

    /// Deliver the redirected frames of receive queue `queue` to the AF_XDP socket `fd` (bound).
    [[nodiscard]] auto addSocket(unsigned queue, int fd) noexcept -> std::error_code;

    /// Attached in native (driver) mode; generic (skb) mode otherwise.
    [[nodiscard]] auto native() const noexcept -> bool {
        return native_;
    }

    /// The program writes NIC receive timestamps in front of the frames.
    [[nodiscard]] auto timestamps() const noexcept -> bool {
        return timestamps_;
    }
};

/// Bytes of XDP metadata in front of every frame when XdpProgram::timestamps() (the timestamp,
/// nanoseconds of the NIC clock, 0 if not available).
inline constexpr std::size_t kXdpMetadataSize = 8;

} // namespace turboq::reactor::detail
