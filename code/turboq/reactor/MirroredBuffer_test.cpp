// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <unistd.h>

#include <cstring>
#include <numeric>
#include <vector>

#include <doctest/doctest.h>

#include "MirroredBuffer.h"

namespace turboq::reactor::testing {

TEST_SUITE("MirroredBuffer") {

    TEST_CASE("capacity is rounded up to a power of two and at least one page") {
        auto const pageSize = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));

        auto small = MirroredBuffer::create({.capacityHint = 1});
        REQUIRE(small);
        REQUIRE_EQ(small->capacity(), pageSize);

        auto odd = MirroredBuffer::create({.capacityHint = pageSize * 3});
        REQUIRE(odd);
        REQUIRE_EQ(odd->capacity(), pageSize * 4);
    }

    TEST_CASE("both halves alias the same memory") {
        auto buffer = MirroredBuffer::create({.capacityHint = 4096});
        REQUIRE(buffer);
        auto const capacity = buffer->capacity();

        auto writable = buffer->writable();
        REQUIRE_EQ(writable.size(), capacity);
        writable[0] = std::byte{0xAB};
        writable[capacity - 1] = std::byte{0xCD};

        // Peek past the end of the first half through a span of 2 * capacity.
        auto const* raw = writable.data();
        REQUIRE_EQ(raw[capacity], std::byte{0xAB});
        REQUIRE_EQ(raw[2 * capacity - 1], std::byte{0xCD});
    }

    TEST_CASE("readable and writable stay contiguous across the wrap point") {
        auto buffer = MirroredBuffer::create({.capacityHint = 4096});
        REQUIRE(buffer);
        auto const capacity = buffer->capacity();

        // Move positions close to the end of the ring.
        buffer->produce(capacity - 10);
        buffer->consume(capacity - 10);
        REQUIRE(buffer->empty());

        std::vector<std::byte> pattern(100);
        for (std::size_t i = 0; i < pattern.size(); ++i) {
            pattern[i] = static_cast<std::byte>(i);
        }

        auto writable = buffer->writable();
        REQUIRE_EQ(writable.size(), capacity);
        std::memcpy(writable.data(), pattern.data(), pattern.size()); // crosses the wrap point
        buffer->produce(pattern.size());

        auto readable = buffer->readable();
        REQUIRE_EQ(readable.size(), pattern.size());
        REQUIRE_EQ(std::memcmp(readable.data(), pattern.data(), pattern.size()), 0);

        buffer->consume(40);
        readable = buffer->readable();
        REQUIRE_EQ(readable.size(), 60);
        REQUIRE_EQ(std::memcmp(readable.data(), pattern.data() + 40, 60), 0);
    }

    TEST_CASE("full and empty") {
        auto buffer = MirroredBuffer::create({.capacityHint = 4096, .populate = false});
        REQUIRE(buffer);

        REQUIRE(buffer->empty());
        buffer->produce(buffer->capacity());
        REQUIRE(buffer->full());
        REQUIRE(buffer->writable().empty());
        REQUIRE_EQ(buffer->readable().size(), buffer->capacity());

        buffer->clear();
        REQUIRE(buffer->empty());
        REQUIRE_EQ(buffer->writable().size(), buffer->capacity());
    }

    TEST_CASE("streaming many messages keeps data intact") {
        auto buffer = MirroredBuffer::create({.capacityHint = 4096});
        REQUIRE(buffer);

        std::uint64_t writeSeq = 0;
        std::uint64_t readSeq = 0;
        for (int round = 0; round < 10000; ++round) {
            // Write a few odd-sized records.
            for (int i = 0; i < 3; ++i) {
                auto writable = buffer->writable();
                if (writable.size() < sizeof(writeSeq) + 3) {
                    break;
                }
                std::memcpy(writable.data(), &writeSeq, sizeof(writeSeq));
                std::memset(writable.data() + sizeof(writeSeq), 0x5A, 3);
                buffer->produce(sizeof(writeSeq) + 3);
                ++writeSeq;
            }
            // Read them back.
            while (buffer->size() >= sizeof(readSeq) + 3) {
                std::uint64_t value;
                std::memcpy(&value, buffer->readable().data(), sizeof(value));
                REQUIRE_EQ(value, readSeq);
                buffer->consume(sizeof(value) + 3);
                ++readSeq;
            }
        }
        REQUIRE_EQ(readSeq, writeSeq);
    }

    TEST_CASE("move") {
        auto created = MirroredBuffer::create({.capacityHint = 4096});
        REQUIRE(created);
        MirroredBuffer a = std::move(*created);
        a.produce(5);

        MirroredBuffer b;
        REQUIRE_FALSE(b);
        b = std::move(a);
        REQUIRE(b);
        REQUIRE_FALSE(a);
        REQUIRE_EQ(b.size(), 5);
    }
}

} // namespace turboq::reactor::testing
