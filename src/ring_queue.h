#ifndef ULTRATAB_RING_QUEUE_H
#define ULTRATAB_RING_QUEUE_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <vector>

namespace ultratab {

/// Bounded ring-buffer queue (mutex + condvar). Fixed capacity; push blocks when full,
/// pop blocks when empty. Cancel unblocks all waiters.
template <typename T>
class RingQueue {
 public:
  explicit RingQueue(std::size_t capacity)
      : capacity_(capacity > 0 ? capacity : 1),
        slots_(capacity_),
        head_(0),
        tail_(0),
        size_(0) {}

  /// Push item; blocks if full. Returns false if cancelled.
  bool push(T item) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [this] { return cancelled_.load() || size_.load() < capacity_; });
    if (cancelled_.load()) return false;
    slots_[tail_] = std::move(item);
    tail_ = (tail_ + 1) % capacity_;
    size_.fetch_add(1);
    not_empty_.notify_one();
    return true;
  }

  /// Pop into out; blocks until available. Returns false if cancelled.
  bool pop(T& out) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] { return cancelled_.load() || size_.load() > 0; });
    if (cancelled_.load()) return false;
    out = std::move(slots_[head_]);
    head_ = (head_ + 1) % capacity_;
    size_.fetch_sub(1);
    not_full_.notify_one();
    return true;
  }

  void cancel() {
    cancelled_.store(true);
    not_full_.notify_all();
    not_empty_.notify_all();
  }

  bool is_cancelled() const { return cancelled_.load(); }
  std::size_t size() const { return size_.load(); }
  std::size_t capacity() const { return capacity_; }

 private:
  const std::size_t capacity_;
  std::vector<T> slots_;
  std::size_t head_;
  std::size_t tail_;
  std::atomic<std::size_t> size_;
  std::atomic<bool> cancelled_{false};
  mutable std::mutex mutex_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
};

}  // namespace ultratab

#endif  // ULTRATAB_RING_QUEUE_H
