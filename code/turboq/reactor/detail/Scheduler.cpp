// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "Scheduler.h"

namespace turboq::reactor::detail {

Scheduler::~Scheduler() noexcept {
    // Connections that outlive the reactor are handed to their handles, which delete them (their
    // sockets get closed then). Destroying such a connection is the only valid operation left.
    for (auto* core = live_; core != nullptr; core = core->liveNext_) {
        core->orphaned_ = true;
        core->owner_ = nullptr;
    }
    // retired_ deletes the retired cores.
}

void Scheduler::flushPendingTx() noexcept {
    if (pendingTx_.empty()) {
        return;
    }
    for (auto* handler : pendingTx_) {
        if (handler->txDirty_) {
            handler->txDirty_ = false;
            handler->onTxReady();
        }
    }
    pendingTx_.clear();
}

void Scheduler::attach(IoHandler* core) noexcept {
    core->owner_ = this;
    core->livePrev_ = nullptr;
    core->liveNext_ = live_;
    if (live_) {
        live_->livePrev_ = core;
    }
    live_ = core;
}

void Scheduler::retire(std::unique_ptr<IoHandler> core) noexcept {
    if (core->livePrev_) {
        core->livePrev_->liveNext_ = core->liveNext_;
    } else {
        live_ = core->liveNext_;
    }
    if (core->liveNext_) {
        core->liveNext_->livePrev_ = core->livePrev_;
    }
    core->livePrev_ = core->liveNext_ = nullptr;

    core->beginRetire();
    // pendingTx_ may hold the core (or a core it owns) even with txDirty_ cleared.
    core->unlinkPendingTx(pendingTx_);
    if (core->retirable()) {
        core->onRetired(); // and deleted on return
    } else {
        retired_.push_back(std::move(core));
    }
}

void Scheduler::collectRetired() noexcept {
    if (retired_.empty()) [[likely]] {
        return;
    }
    std::erase_if(retired_, [this](std::unique_ptr<IoHandler> const& core) {
        if (!core->retirable()) {
            return false;
        }
        core->unlinkPendingTx(pendingTx_);
        core->onRetired();
        return true; // erased: deleted
    });
}

void releaseCore(std::unique_ptr<IoHandler> core) noexcept {
    if (!core || core->orphaned_ || !core->owner_) {
        // The reactor is gone (and with it everything in flight), or the core never got attached:
        // nothing references it. Deleted on return.
        return;
    }
    auto* const owner = core->owner_;
    owner->retire(std::move(core));
}

} // namespace turboq::reactor::detail
