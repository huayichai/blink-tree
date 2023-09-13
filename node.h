#ifndef NODE_H_
#define NODE_H_

#include <immintrin.h>
#include <limits.h>
#include <unistd.h>

#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <utility>

#define PAGE_SIZE (512)

namespace BLINK_TREE {

class Node {
 public:
  std::atomic<uint64_t> lock;  // latch
  Node* sibling_ptr;           // right sibling pointer
  int cnt;                     // entries number in node
  uint32_t level;              // cur node level in blink-tree

 public:
  Node() : lock(0), sibling_ptr(nullptr), cnt(0), level(0) {}

  Node(Node* sibling, uint32_t count, uint32_t _level)
      : lock(0), sibling_ptr(sibling), cnt(count), level(_level) {}

  /**
   * @brief Check whether this node is locked.
   *        if @p version last two bits is 0b10, means locked.
   * @param version lock value
   */
  bool is_locked(uint64_t version) { return ((version & 0b10) == 0b10); }

  bool is_obsolete(uint64_t version) { return (version & 1) == 1; }

  uint64_t get_version(bool& restart) {
    uint64_t version = lock.load();
    need_restart(version, restart);
    return version;
  }

  /**
   * @brief check whether it is locked
   *        if current node is locked by other thread, means current thread
   * should be restart
   * @param[out] restart
   */
  uint64_t try_readlock(bool& restart) {
    uint64_t version = (uint64_t)lock.load();
    need_restart(version, restart);
    return version;
  }

  /**
   * @brief exclusive lock
   *        current thread does not hold a lock
   */
  bool try_writelock() {
    uint64_t version = lock.load();
    auto restart = false;
    need_restart(version, restart);
    if (!restart) return false;

    if (lock.compare_exchange_strong(version, version + 0b10)) {  // lock
      return true;
    } else {
      _mm_pause();
      return false;
    }
  }

  /**
   * @brief upgrade to exclusive lock
   */
  void try_upgrade_writelock(uint64_t version, bool& restart) {
    uint64_t _version = lock.load();
    if (version != _version) {
      _mm_pause();
      restart = true;
      return;
    }

    if (!lock.compare_exchange_strong(_version, _version + 0b10)) {
      _mm_pause();
      restart = true;
    }
  }

  void write_unlock() { lock.fetch_add(0b10); }

  void write_unlock_obsolete() { lock.fetch_add(0b11); }

  int get_cnt() { return cnt; }

 private:
  /**
   * @brief need_restart
   *        if locked or obsoleted, need restart
   * @param version lock value
   * @param[out] restart true for need restart, false for not need restart
   */
  void need_restart(uint64_t version, bool& restart) {
    if (is_locked(version) || is_obsolete(version)) {
      _mm_pause();
      restart = true;
    }
  }

};  // class Node

template <typename key_t, typename value_t>
struct Entry {
  key_t key;
  value_t value;
};  // class Entry

/**
 * InternalNode store next level node info.
 * p1'high_key less than k1, p2'high_key greater than k1.
 * | k1 | k2 | k3 | k4 |    |
 * | p1 | p2 | p3 | p4 | p5 |
 */
template <typename key_t>
class InternalNode : public Node {
 public:
  static constexpr size_t cardinality =
      (PAGE_SIZE - sizeof(Node) - sizeof(key_t)) / sizeof(Entry<key_t, Node*>);
  key_t high_key;

 private:
  Entry<key_t, Node*> entry[cardinality];

 public:
  InternalNode() {}

  /**
   * @brief constructor when InternalNode needs to split
   */
  InternalNode(Node* sibling, int count, Node* left, uint32_t _level,
               key_t _high_key)
      : Node(sibling, count, _level), high_key(_high_key) {
    entry[0].value = left;
  }

  /**
   * @brief constructor when tree height grows
   */
  InternalNode(key_t split_key, Node* left, Node* right, Node* sibling,
               uint32_t _level, key_t _high_key)
      : Node(sibling, 1, _level) {
    high_key = _high_key;
    entry[0].key = split_key;
    entry[0].value = left;
    entry[1].value = right;
  }

  bool is_full() { return (cnt == cardinality - 1); }

  /**
   * @brief find the first key in entry that greater than @p key .
   * @return position of the first key that greater than @p key .
   */
  int find_lowerbound(key_t& key) { return lowerbound_linear(key); }

  /**
   * @brief Find the first child node that @p key greater than node's high_key.
   *        First try sibling node, Second use find_lowerbound() find child.
   * @return node
   */
  Node* scan_node(key_t key) {
    if (sibling_ptr && (high_key < key)) {
      return sibling_ptr;
    } else {
      return entry[find_lowerbound(key)].value;
    }
  }

  Node* leftmost_ptr() { return entry[0].value; }

  /**
   * @brief Insert key, pointer in sorted entry.
   */
  void insert(key_t key, Node* value) {
    int pos = find_lowerbound(key);
    memmove(entry + pos + 1, entry + pos,
            sizeof(Entry<key_t, Node*>) * (cnt - pos + 1));
    entry[pos].key = key;
    entry[pos].value = value;
    std::swap(entry[pos].value, entry[pos + 1].value);
    cnt++;
    if (key > high_key) {
      high_key = key;
    }
  }

  /**
   * @brief Split half entries to new node, and rearrange sibling ptr.
   *        original: prev_node -> cur_node -> next_node,
   *             now: prev_node -> cur_node -> new_node -> next_node
   *
   *                      | k1 | k2 | k3 | k4 |    |
   *                      | p1 | p2 | p3 | p4 | p5 |
   *                      /                         \
   *                     /                           \
   *   | k1 | k2 |    |                              | k4 |    |
   *   | p1 | p2 | p3 |                              | p4 | p5 |
   *
   *
   * @param[out] split_key
   * @return new allocated internal node
   */
  InternalNode<key_t>* split(key_t& split_key) {
    int half = cnt - cnt / 2;
    split_key = entry[half].key;

    int new_cnt = cnt - half - 1;
    auto new_node = new InternalNode<key_t>(sibling_ptr, new_cnt,
                                            entry[half].value, level, high_key);
    memcpy(new_node->entry, entry + half + 1,
           sizeof(Entry<key_t, Node*>) * (new_cnt + 1));

    sibling_ptr = static_cast<Node*>(new_node);
    high_key = entry[half].key;
    cnt = half;
    return new_node;
  }

 private:
  /**
   * @brief find the first key in entry that greater than @p key
   * @return position of the first key that greater than @p key
   */
  int lowerbound_linear(key_t key) {
    for (int i = 0; i < cnt; i++) {
      if (entry[i].key >= key) {
        return i;
      }
    }
    return cnt;
  }

};  // class InternalNode

/**
 * LeafNode store key-value pair.
 * | k1 | k2 | k3 | k4 | k5 |
 * | v1 | v2 | v3 | v4 | v5 |
 */
template <typename key_t>
class LeafNode : public Node {
 public:
  static constexpr size_t cardinality =
      (PAGE_SIZE - sizeof(Node) - sizeof(key_t)) /
      sizeof(Entry<key_t, uint64_t>);

  key_t high_key;

 private:
  Entry<key_t, uint64_t> entry[cardinality];

 public:
  LeafNode() : Node() {}

  /**
   * @brief constructor when leaf splits
   */
  LeafNode(Node* sibling, int _cnt, uint32_t _level)
      : Node(sibling, _cnt, _level) {}

  bool is_full() { return (cnt == cardinality); }

  /**
   * @brief find the first key in entry that greater than @p key .
   * @return position of the first key that greater than @p key .
   */
  int find_lowerbound(key_t key) { return lowerbound_linear(key); }

  /**
   * @brief find the first key in entry that greater than @p key .
   * @return value of the first key that greater than @p key .
   */
  uint64_t find(key_t key) { return find_linear(key); }

  /**
   * @brief Insert key, value in sorted entry.
   */
  void insert(key_t key, uint64_t value) {
    if (cnt) {
      int pos = find_lowerbound(key);
      memmove(&entry[pos + 1], &entry[pos],
              sizeof(Entry<key_t, uint64_t>) * (cnt - pos));
      entry[pos].key = key;
      entry[pos].value = value;
    } else {
      entry[0].key = key;
      entry[0].value = value;
    }
    cnt++;
    if (key > high_key) {
      high_key = key;
    }
  }

  /**
   * @brief Split half entries to new node, and rearrange sibling ptr.
   *        original: prev_node -> cur_node -> next_node,
   *             now: prev_node -> cur_node -> new_node -> next_node
   * @param[out] split_key
   * @return new allocated leaf node
   */
  LeafNode<key_t>* split(key_t& split_key) {
    int half = cnt / 2;
    int new_cnt = cnt - half;
    split_key = entry[half - 1].key;

    auto new_leaf = new LeafNode<key_t>(sibling_ptr, new_cnt, level);
    new_leaf->high_key = high_key;
    memcpy(new_leaf->entry, entry + half,
           sizeof(Entry<key_t, uint64_t>) * new_cnt);

    sibling_ptr = static_cast<Node*>(new_leaf);
    high_key = entry[half - 1].key;
    cnt = half;
    return new_leaf;
  }

  bool remove(key_t key) {
    if (cnt) {
      int pos = find_pos_linear(key);
      // no matching key found
      if (pos == -1) return false;
      memmove(&entry[pos], &entry[pos + 1],
              sizeof(Entry<key_t, uint64_t>) * (cnt - pos - 1));
      cnt--;
      return true;
    }
    return false;
  }

  bool update(key_t key, uint64_t value) { return update_linear(key, value); }

 private:
  int lowerbound_linear(key_t key) {
    for (int i = 0; i < cnt; i++) {
      if (key <= entry[i].key) return i;
    }
    return cnt;
  }

  bool update_linear(key_t key, uint64_t value) {
    for (int i = 0; i < cnt; i++) {
      if (key == entry[i].key) {
        entry[i].value = value;
        return true;
      }
    }
    return false;
  }

  uint64_t find_linear(key_t key) {
    for (int i = 0; i < cnt; i++) {
      if (key == entry[i].key) {
        auto ret = entry[i].value;
        return ret;
      }
    }
    return 0;
  }

  int find_pos_linear(key_t key) {
    for (int i = 0; i < cnt; i++) {
      if (key == entry[i].key) {
        return i;
      }
    }
    return -1;
  }
};  // class Node

}  // namespace BLINK_TREE

#endif  // NODE_H_