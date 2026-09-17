// Distributed Search Engine - Portable SharedMutex wrapper.
//
// MinGW/MSYS2 GCC's std::shared_mutex has a known bug:
//   std::__shared_mutex_pthread::lock(): Assertion '__ret == 0' failed.
//
// Under heavy exclusive lock contention, pthread_mutex_lock() sometimes
// returns EBUSY/EDEADLK, and MinGW's libstdc++ asserts instead of
// retrying. This causes spurious crashes in concurrent tests.
//
// This wrapper provides the same shared/exclusive semantics using plain
// std::mutex + std::condition_variable internally.  std::unique_lock and
// std::shared_lock both work with this type because they are duck-typed
// via SFINAE (they only need lock(), unlock(), try_lock(),
// lock_shared(), unlock_shared()).
//
// Writer-priority: when a writer is waiting, new readers queue behind
// the writer to prevent writer starvation.
//
// Usage:
//   SharedMutex mtx;
//   std::unique_lock lock(mtx);           // exclusive
//   std::shared_lock  lock(mtx);           // shared

#pragma once

#include <condition_variable>
#include <mutex>

namespace dse {

class SharedMutex {
public:
    SharedMutex() = default;

    SharedMutex(const SharedMutex&) = delete;
    SharedMutex& operator=(const SharedMutex&) = delete;
    SharedMutex(SharedMutex&&) = delete;
    SharedMutex& operator=(SharedMutex&&) = delete;

    // Exclusive lock: signal intent first (writer_waiting_ = true), then
    // wait until no active readers AND no other active writer.
    void lock() {
        std::unique_lock lk(internal_mutex_);
        writer_waiting_ = true;
        cv_.wait(lk, [this] {
            return readers_ == 0 && !writer_active_;
        });
        writer_waiting_ = false;
        writer_active_ = true;
    }

    // Exclusive unlock: release and wake all waiters.
    void unlock() {
        {
            std::lock_guard lk(internal_mutex_);
            writer_active_ = false;
        }
        cv_.notify_all();
    }

    // Try exclusive lock (non-blocking).
    bool try_lock() {
        std::lock_guard lk(internal_mutex_);
        if (readers_ == 0 && !writer_active_ && !writer_waiting_) {
            writer_active_ = true;
            return true;
        }
        return false;
    }

    // Shared lock: wait until no active writer AND no writer is waiting.
    void lock_shared() {
        std::unique_lock lk(internal_mutex_);
        cv_.wait(lk, [this] {
            return !writer_active_ && !writer_waiting_;
        });
        ++readers_;
    }

    // Shared unlock: decrement readers and notify_all() so that a blocked
    // writer or reader can re-evaluate its condition.  notify_all()
    // (rather than a targeted notify_one) is used intentionally:
    //   - When readers_ hits 0, the writer can now acquire the lock.
    //   - When writer_waiting_ clears, blocked readers can enter.
    //   - notify_all() is cheap here (few threads) and eliminates
    //     complex ordering requirements between notification and state.
    void unlock_shared() {
        {
            std::lock_guard lk(internal_mutex_);
            --readers_;
        }
        cv_.notify_all();
    }

    // Try shared lock (non-blocking).
    bool try_lock_shared() {
        std::lock_guard lk(internal_mutex_);
        if (!writer_active_ && !writer_waiting_) {
            ++readers_;
            return true;
        }
        return false;
    }

private:
    std::mutex internal_mutex_;
    std::condition_variable cv_;
    unsigned readers_ = 0;
    bool writer_active_ = false;
    bool writer_waiting_ = false;
};

} // namespace dse
