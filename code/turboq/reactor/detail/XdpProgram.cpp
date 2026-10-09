// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "XdpProgram.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <linux/bpf.h>
#include <linux/btf.h>
#include <linux/if_link.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <bit>
#include <cerrno>
#include <cstring>
#include <format>
#include <optional>
#include <string_view>
#include <vector>

#include "../Error.h"

namespace turboq::reactor::detail {
namespace {

static_assert(std::endian::native == std::endian::little,
    "the XDP program compares packet fields loaded as little-endian integers");

// ---------------------------------------------------------------------------------------------
// bpf() syscall

[[nodiscard]] auto bpf(int command, bpf_attr& attr) noexcept -> int {
    return static_cast<int>(::syscall(__NR_bpf, command, &attr, sizeof(attr)));
}

[[nodiscard]] auto toPointer(void const* pointer) noexcept -> std::uint64_t {
    return std::bit_cast<std::uintptr_t>(pointer);
}

[[nodiscard]] auto createMap(bpf_map_type type, unsigned keySize, unsigned valueSize, unsigned maxEntries,
    char const* name) noexcept -> std::expected<UniqueFd, std::error_code> {
    bpf_attr attr{};
    attr.map_type = type;
    attr.key_size = keySize;
    attr.value_size = valueSize;
    attr.max_entries = maxEntries;
    std::strncpy(attr.map_name, name, sizeof(attr.map_name) - 1);
    int const fd = bpf(BPF_MAP_CREATE, attr);
    if (fd < 0) {
        return std::unexpected(makePosixErrorCode(errno));
    }
    return UniqueFd{fd};
}

[[nodiscard]] auto updateMap(int map, void const* key, void const* value) noexcept -> std::error_code {
    bpf_attr attr{};
    attr.map_fd = static_cast<std::uint32_t>(map);
    attr.key = toPointer(key);
    attr.value = toPointer(value);
    attr.flags = BPF_ANY;
    if (bpf(BPF_MAP_UPDATE_ELEM, attr) != 0) {
        return makePosixErrorCode(errno);
    }
    return {};
}

// ---------------------------------------------------------------------------------------------
// BTF: the id of a kernel function, needed to call a kfunc.

[[nodiscard]] auto findKernelFunction(std::string_view name) -> std::optional<std::int32_t> {
    UniqueFd file{::open("/sys/kernel/btf/vmlinux", O_RDONLY | O_CLOEXEC)};
    if (!file) {
        return std::nullopt;
    }
    std::vector<std::byte> data;
    std::byte chunk[65536];
    while (true) {
        auto const n = ::read(file.get(), chunk, sizeof(chunk));
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            break;
        }
        data.insert(data.end(), chunk, chunk + n);
    }

    btf_header header;
    if (data.size() < sizeof(header)) {
        return std::nullopt;
    }
    std::memcpy(&header, data.data(), sizeof(header));
    if (header.magic != BTF_MAGIC || std::size_t{header.hdr_len} + header.type_off + header.type_len > data.size() ||
        std::size_t{header.hdr_len} + header.str_off + header.str_len > data.size()) {
        return std::nullopt;
    }
    auto const* const types = data.data() + header.hdr_len + header.type_off;
    auto const* const strings = data.data() + header.hdr_len + header.str_off;
    auto const stringAt = [&](std::uint32_t offset) -> std::string_view {
        if (offset >= header.str_len) {
            return {};
        }
        auto const* const begin = std::bit_cast<char const*>(strings + offset);
        return {begin, ::strnlen(begin, header.str_len - offset)};
    };

    std::size_t offset = 0;
    std::int32_t id = 1;
    while (offset + sizeof(btf_type) <= header.type_len) {
        btf_type type;
        std::memcpy(&type, types + offset, sizeof(type));
        offset += sizeof(type);
        auto const kind = BTF_INFO_KIND(type.info);
        auto const vlen = std::size_t{BTF_INFO_VLEN(type.info)};
        if (kind == BTF_KIND_FUNC && stringAt(type.name_off) == name) {
            return id;
        }
        // Skip the kind specific data that follows btf_type.
        switch (kind) {
        case BTF_KIND_INT: offset += sizeof(std::uint32_t); break;
        case BTF_KIND_ARRAY: offset += sizeof(btf_array); break;
        case BTF_KIND_STRUCT: [[fallthrough]];
        case BTF_KIND_UNION: offset += vlen * sizeof(btf_member); break;
        case BTF_KIND_ENUM: offset += vlen * sizeof(btf_enum); break;
        case BTF_KIND_FUNC_PROTO: offset += vlen * sizeof(btf_param); break;
        case BTF_KIND_VAR: offset += sizeof(btf_var); break;
        case BTF_KIND_DATASEC: offset += vlen * sizeof(btf_var_secinfo); break;
        case BTF_KIND_DECL_TAG: offset += sizeof(btf_decl_tag); break;
        case BTF_KIND_ENUM64: offset += vlen * sizeof(btf_enum64); break;
        case BTF_KIND_PTR: [[fallthrough]];
        case BTF_KIND_FWD: [[fallthrough]];
        case BTF_KIND_TYPEDEF: [[fallthrough]];
        case BTF_KIND_VOLATILE: [[fallthrough]];
        case BTF_KIND_CONST: [[fallthrough]];
        case BTF_KIND_RESTRICT: [[fallthrough]];
        case BTF_KIND_FUNC: [[fallthrough]];
        case BTF_KIND_FLOAT: [[fallthrough]];
        case BTF_KIND_TYPE_TAG: break;
        default: return std::nullopt; // a kind this parser does not know: can't walk further
        }
        ++id;
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------------------------
// A minimal BPF assembler: instructions plus forward jumps to labels.

enum Reg : std::uint8_t { R0 = 0, R1, R2, R3, R4, R5, R6, R7, R8, R9, R10 };

class Assembler {
private:
    struct Fixup {
        std::size_t at;
        int label;
    };

    std::vector<bpf_insn> code_;
    std::vector<std::optional<std::size_t>> labels_;
    std::vector<Fixup> fixups_;

    void emit(std::uint8_t code, std::uint8_t dst, std::uint8_t src, std::int16_t off, std::int32_t imm) {
        bpf_insn insn{};
        insn.code = code;
        insn.dst_reg = dst & 0xf;
        insn.src_reg = src & 0xf;
        insn.off = off;
        insn.imm = imm;
        code_.push_back(insn);
    }

    void jump(std::uint8_t code, std::uint8_t dst, std::uint8_t src, std::int32_t imm, int label) {
        fixups_.push_back({code_.size(), label});
        this->emit(code, dst, src, 0, imm);
    }

public:
    [[nodiscard]] auto newLabel() -> int {
        labels_.emplace_back();
        return static_cast<int>(labels_.size() - 1);
    }

    void bind(int label) {
        labels_[static_cast<std::size_t>(label)] = code_.size();
    }

    void movReg(Reg dst, Reg src) {
        this->emit(BPF_ALU64 | BPF_MOV | BPF_X, dst, src, 0, 0);
    }
    void movImm(Reg dst, std::int32_t imm) {
        this->emit(BPF_ALU64 | BPF_MOV | BPF_K, dst, 0, 0, imm);
    }
    void addImm(Reg dst, std::int32_t imm) {
        this->emit(BPF_ALU64 | BPF_ADD | BPF_K, dst, 0, 0, imm);
    }
    void addReg(Reg dst, Reg src) {
        this->emit(BPF_ALU64 | BPF_ADD | BPF_X, dst, src, 0, 0);
    }
    void andImm(Reg dst, std::int32_t imm) {
        this->emit(BPF_ALU64 | BPF_AND | BPF_K, dst, 0, 0, imm);
    }
    void lshImm(Reg dst, std::int32_t imm) {
        this->emit(BPF_ALU64 | BPF_LSH | BPF_K, dst, 0, 0, imm);
    }
    /// dst = *(size*)(src + off)
    void load(std::uint8_t size, Reg dst, Reg src, std::int16_t off) {
        this->emit(BPF_LDX | size | BPF_MEM, dst, src, off, 0);
    }
    /// *(size*)(dst + off) = src
    void store(std::uint8_t size, Reg dst, std::int16_t off, Reg src) {
        this->emit(BPF_STX | size | BPF_MEM, dst, src, off, 0);
    }
    /// *(size*)(dst + off) = imm
    void storeImm(std::uint8_t size, Reg dst, std::int16_t off, std::int32_t imm) {
        this->emit(BPF_ST | size | BPF_MEM, dst, 0, off, imm);
    }
    /// dst = map (file descriptor relocated by the kernel)
    void loadMap(Reg dst, int mapFd) {
        this->emit(BPF_LD | BPF_DW | BPF_IMM, dst, BPF_PSEUDO_MAP_FD, 0, mapFd);
        this->emit(0, 0, 0, 0, 0);
    }
    void jumpIfImm(std::uint8_t op, Reg dst, std::int32_t imm, int label) {
        this->jump(BPF_JMP | op | BPF_K, dst, 0, imm, label);
    }
    void jumpIfReg(std::uint8_t op, Reg dst, Reg src, int label) {
        this->jump(BPF_JMP | op | BPF_X, dst, src, 0, label);
    }
    void jumpTo(int label) {
        this->jump(BPF_JMP | BPF_JA, 0, 0, 0, label);
    }
    void call(std::int32_t helper) {
        this->emit(BPF_JMP | BPF_CALL, 0, 0, 0, helper);
    }
    void callKernelFunction(std::int32_t btfId) {
        this->emit(BPF_JMP | BPF_CALL, 0, BPF_PSEUDO_KFUNC_CALL, 0, btfId);
    }
    void exit() {
        this->emit(BPF_JMP | BPF_EXIT, 0, 0, 0, 0);
    }

    [[nodiscard]] auto finish() -> std::vector<bpf_insn> {
        for (auto const& fixup : fixups_) {
            auto const target = *labels_[static_cast<std::size_t>(fixup.label)];
            code_[fixup.at].off = static_cast<std::int16_t>(
                static_cast<std::ptrdiff_t>(target) - static_cast<std::ptrdiff_t>(fixup.at) - 1);
        }
        return std::move(code_);
    }
};

// struct xdp_md field offsets.
constexpr std::int16_t kXdpData = 0;
constexpr std::int16_t kXdpDataEnd = 4;
constexpr std::int16_t kXdpDataMeta = 8;
constexpr std::int16_t kXdpRxQueueIndex = 16;

// Frame layout (no VLAN tag: the NIC strips it with rx-vlan-offload, which is the default).
constexpr std::int32_t kEthernetHeader = 14;
constexpr std::int16_t kEtherTypeOffset = 12;
constexpr std::int32_t kEtherTypeIPv4 = 0x0008; // 0x0800 loaded as a little-endian u16
constexpr std::int32_t kEtherTypeIPv6 = 0xDD86; // 0x86DD
constexpr std::int32_t kUdp = 17;

/// The program, in C:
///
///   int xdp(struct xdp_md* ctx) {
///       if (timestamps && bpf_xdp_adjust_meta(ctx, -8) == 0) {
///           __u64* meta = (void*)(long)ctx->data_meta;
///           if (meta + 1 <= (void*)(long)ctx->data) {
///               *meta = 0;
///               bpf_xdp_metadata_rx_timestamp(ctx, meta);
///           }
///       }
///       if (filter) {
///           port = UDP destination port of an IPv4 (first fragment) or IPv6 datagram, else pass;
///           if (!bpf_map_lookup_elem(&ports, &port)) return XDP_PASS;
///       }
///       return bpf_redirect_map(&xsks, ctx->rx_queue_index, XDP_PASS);
///   }
///
/// bpf_redirect_map() falls back to XDP_PASS for queues without a socket.
[[nodiscard]] auto buildProgram(
    int xskMap, std::optional<int> portMap, std::optional<std::int32_t> timestampFunction) -> std::vector<bpf_insn> {
    Assembler a;
    auto const parse = a.newLabel();
    auto const ipv4 = a.newLabel();
    auto const ipv6 = a.newLabel();
    auto const lookup = a.newLabel();
    auto const redirect = a.newLabel();
    auto const pass = a.newLabel();

    a.movReg(R6, R1); // ctx

    if (timestampFunction) {
        a.movReg(R1, R6);
        a.movImm(R2, -static_cast<std::int32_t>(kXdpMetadataSize));
        a.call(BPF_FUNC_xdp_adjust_meta);
        a.jumpIfImm(BPF_JNE, R0, 0, parse);
        a.load(BPF_W, R2, R6, kXdpDataMeta);
        a.load(BPF_W, R3, R6, kXdpData);
        a.movReg(R4, R2);
        a.addImm(R4, static_cast<std::int32_t>(kXdpMetadataSize));
        a.jumpIfReg(BPF_JGT, R4, R3, parse);
        a.storeImm(BPF_DW, R2, 0, 0);
        a.movReg(R1, R6);
        a.callKernelFunction(*timestampFunction); // (ctx, meta): leaves 0 if there is no timestamp
    }

    a.bind(parse);
    if (portMap) {
        a.load(BPF_W, R2, R6, kXdpData);
        a.load(BPF_W, R3, R6, kXdpDataEnd);
        a.movReg(R4, R2);
        a.addImm(R4, kEthernetHeader);
        a.jumpIfReg(BPF_JGT, R4, R3, pass);
        a.load(BPF_H, R5, R2, kEtherTypeOffset);
        a.jumpIfImm(BPF_JEQ, R5, kEtherTypeIPv4, ipv4);
        a.jumpIfImm(BPF_JEQ, R5, kEtherTypeIPv6, ipv6);
        a.jumpTo(pass);

        a.bind(ipv4);
        a.movReg(R4, R2);
        a.addImm(R4, kEthernetHeader + 20);
        a.jumpIfReg(BPF_JGT, R4, R3, pass);
        a.load(BPF_B, R5, R2, kEthernetHeader + 9); // protocol
        a.jumpIfImm(BPF_JNE, R5, kUdp, pass);
        a.load(BPF_H, R5, R2, kEthernetHeader + 6); // flags + fragment offset
        a.andImm(R5, 0xff1f);                       // fragment offset (13 bits, big-endian)
        a.jumpIfImm(BPF_JNE, R5, 0, pass);          // not the first fragment: no UDP header
        a.load(BPF_B, R5, R2, kEthernetHeader);     // version + IHL
        a.andImm(R5, 0x0f);
        a.lshImm(R5, 2);
        a.jumpIfImm(BPF_JLT, R5, 20, pass);
        a.movReg(R4, R2);
        a.addImm(R4, kEthernetHeader);
        a.addReg(R4, R5); // UDP header
        a.movReg(R5, R4);
        a.addImm(R5, 8);
        a.jumpIfReg(BPF_JGT, R5, R3, pass);
        a.load(BPF_H, R5, R4, 2); // destination port
        a.jumpTo(lookup);

        a.bind(ipv6);
        a.movReg(R4, R2);
        a.addImm(R4, kEthernetHeader + 40 + 8);
        a.jumpIfReg(BPF_JGT, R4, R3, pass);
        a.load(BPF_B, R5, R2, kEthernetHeader + 6); // next header (extension headers are not followed)
        a.jumpIfImm(BPF_JNE, R5, kUdp, pass);
        a.load(BPF_H, R5, R2, kEthernetHeader + 40 + 2);

        a.bind(lookup);
        a.store(BPF_W, R10, -4, R5);
        a.movReg(R2, R10);
        a.addImm(R2, -4);
        a.loadMap(R1, *portMap);
        a.call(BPF_FUNC_map_lookup_elem);
        a.jumpIfImm(BPF_JEQ, R0, 0, pass);
    }

    a.bind(redirect);
    a.load(BPF_W, R2, R6, kXdpRxQueueIndex);
    a.loadMap(R1, xskMap);
    a.movImm(R3, XDP_PASS);
    a.call(BPF_FUNC_redirect_map);
    a.exit();

    if (portMap) { // the verifier rejects unreachable code
        a.bind(pass);
        a.movImm(R0, XDP_PASS);
        a.exit();
    }
    return a.finish();
}

[[nodiscard]] auto loadProgram(std::span<bpf_insn const> code, unsigned ifindex, bool deviceBound,
    std::string& diagnostic) -> std::expected<UniqueFd, std::error_code> {
    // GPL compatible: kfuncs and some helpers are only available to such programs.
    static constexpr char kLicense[] = "Dual MIT/GPL";
    bpf_attr attr{};
    attr.prog_type = BPF_PROG_TYPE_XDP;
    attr.expected_attach_type = BPF_XDP;
    attr.insns = toPointer(code.data());
    attr.insn_cnt = static_cast<std::uint32_t>(code.size());
    attr.license = toPointer(kLicense);
    std::strncpy(attr.prog_name, "turboq_xsk", sizeof(attr.prog_name) - 1);
    if (deviceBound) {
        attr.prog_flags = BPF_F_XDP_DEV_BOUND_ONLY;
        attr.prog_ifindex = ifindex;
    }
    int fd = bpf(BPF_PROG_LOAD, attr);
    if (fd >= 0) {
        return UniqueFd{fd};
    }
    int const error = errno;
    if (error != EACCES && error != EINVAL) {
        diagnostic = std::format("BPF_PROG_LOAD: {}", std::strerror(error));
        return std::unexpected(makePosixErrorCode(error));
    }
    // Rejected by the verifier: load again with the log on, for the diagnostic.
    std::string log(1 << 16, '\0');
    attr.log_level = 1;
    attr.log_buf = toPointer(log.data());
    attr.log_size = static_cast<std::uint32_t>(log.size());
    fd = bpf(BPF_PROG_LOAD, attr);
    if (fd >= 0) {
        return UniqueFd{fd}; // passed the second time (can happen after a transient failure)
    }
    log.resize(::strnlen(log.data(), log.size()));
    diagnostic = std::format("XDP program rejected ({}):\n{}", std::strerror(error), log);
    return std::unexpected(makeErrorCode(Error::XdpProgramRejected));
}

[[nodiscard]] auto attachLink(int program, unsigned ifindex, bool native) noexcept -> std::expected<UniqueFd, int> {
    bpf_attr attr{};
    attr.link_create.prog_fd = static_cast<std::uint32_t>(program);
    attr.link_create.target_ifindex = ifindex;
    attr.link_create.attach_type = BPF_XDP;
    attr.link_create.flags = native ? XDP_FLAGS_DRV_MODE : XDP_FLAGS_SKB_MODE;
    int const fd = bpf(BPF_LINK_CREATE, attr);
    if (fd < 0) {
        return std::unexpected(errno);
    }
    return UniqueFd{fd};
}

} // namespace

void UniqueFd::reset() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

auto XdpProgram::attach(
    XdpProgramOptions const& options, std::string& diagnostic) -> std::expected<XdpProgram, std::error_code> {
    XdpProgram result;

    auto xskMap = createMap(
        BPF_MAP_TYPE_XSKMAP, sizeof(std::uint32_t), sizeof(std::uint32_t), options.maxQueue + 1, "turboq_xsks");
    if (!xskMap) {
        diagnostic = std::format("BPF_MAP_CREATE (XSKMAP): {}", xskMap.error().message());
        return std::unexpected(xskMap.error());
    }
    result.xskMap_ = std::move(*xskMap);

    std::optional<int> portMap;
    if (!options.udpPorts.empty()) {
        auto ports = createMap(BPF_MAP_TYPE_HASH, sizeof(std::uint32_t), sizeof(std::uint8_t),
            static_cast<unsigned>(options.udpPorts.size()), "turboq_ports");
        if (!ports) {
            diagnostic = std::format("BPF_MAP_CREATE (ports): {}", ports.error().message());
            return std::unexpected(ports.error());
        }
        result.portMap_ = std::move(*ports);
        for (auto const port : options.udpPorts) {
            std::uint32_t const key = htons(port); // as the program loads it from the packet
            std::uint8_t const value = 1;
            if (auto ec = updateMap(result.portMap_.get(), &key, &value)) {
                diagnostic = std::format("BPF_MAP_UPDATE_ELEM (ports): {}", ec.message());
                return std::unexpected(ec);
            }
        }
        portMap = result.portMap_.get();
    }

    // Candidates, best first: with timestamps (device-bound, native only), then without.
    struct Attempt {
        bool timestamps;
        bool native;
    };
    std::vector<Attempt> attempts;
    std::optional<std::int32_t> timestampFunction;
    if (options.timestamps && options.allowNative) {
        timestampFunction = findKernelFunction("bpf_xdp_metadata_rx_timestamp");
        if (timestampFunction) {
            attempts.push_back({true, true});
        } else {
            diagnostic = "no bpf_xdp_metadata_rx_timestamp in kernel BTF: hardware timestamps off; ";
        }
    }
    if (options.allowNative) {
        attempts.push_back({false, true});
    }
    if (options.allowGeneric) {
        attempts.push_back({false, false});
    }

    std::error_code lastError = makeErrorCode(Error::InvalidOptions);
    for (auto const& attempt : attempts) {
        auto const code =
            buildProgram(result.xskMap_.get(), portMap, attempt.timestamps ? timestampFunction : std::nullopt);
        std::string loadDiagnostic;
        auto program = loadProgram(code, options.ifindex, attempt.timestamps, loadDiagnostic);
        if (!program) {
            diagnostic += loadDiagnostic + "; ";
            lastError = program.error();
            continue;
        }
        auto link = attachLink(program->get(), options.ifindex, attempt.native);
        if (!link) {
            diagnostic += std::format("attach ({}{}): {}; ", attempt.native ? "native" : "generic",
                attempt.timestamps ? ", timestamps" : "", std::strerror(link.error()));
            lastError = link.error() == EBUSY || link.error() == EEXIST ? makePosixErrorCode(link.error())
                                                                        : makeErrorCode(Error::XdpProgramAttachFailed);
            if (link.error() == EBUSY || link.error() == EEXIST) {
                break; // another XDP program owns the interface: no point in trying other modes
            }
            continue;
        }
        result.program_ = std::move(*program);
        result.link_ = std::move(*link);
        result.native_ = attempt.native;
        result.timestamps_ = attempt.timestamps;
        return result;
    }
    return std::unexpected(lastError);
}

auto XdpProgram::addSocket(unsigned queue, int fd) noexcept -> std::error_code {
    std::uint32_t const key = queue;
    std::uint32_t const value = static_cast<std::uint32_t>(fd);
    return updateMap(xskMap_.get(), &key, &value);
}

} // namespace turboq::reactor::detail
