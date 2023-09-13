#ifndef BLINK_TREE_
#define BLINK_TREE_
#include <vector>

#include "node.h"

namespace BLINK_TREE {

template <typename key_t>
class BLinkTree {
 private:
  Node* root;

 public:
  BLinkTree() { root = static_cast<Node*>(new InternalNode<key_t>()); }
  ~BLinkTree() {}

  /**
   * @brief insert key-value pair into blinktree
   */
  void insert(key_t key, uint64_t value) {
  restart:
    std::vector<InternalNode<key_t>*> stack;
    LeafNode<key_t>* leaf = nullptr;
    uint64_t leaf_vstart = 0;
    leaf = traverse_to_leafnode(key, stack, &leaf_vstart);

    bool need_restart = false;
    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
    if (need_restart) {
      goto restart;
    }

    // leaf node is not full
    if (!leaf->is_full()) {
      leaf->insert(key, value);
      leaf->write_unlock();
    } else {  // leaf node split
      backtrack_insertion_split_key(stack, leaf, key, value);
    }
  }

  /**
   * @brief update key-value pair from blinktree
   */
  bool update(key_t key, uint64_t value) {
  restart:
    bool need_restart = false;

    std::vector<InternalNode<key_t>*> stack;
    LeafNode<key_t>* leaf = nullptr;
    uint64_t leaf_vstart = 0;
    leaf = traverse_to_leafnode(key, stack, &leaf_vstart);

    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
    if (need_restart) {
      goto restart;
    }

    bool ret = leaf->update(key, value);
    leaf->write_unlock();

    return ret;
  }

  /**
   * @brief lookup key from blinktree
   */
  uint64_t lookup(key_t key) {
  restart:
    bool need_restart = false;

    std::vector<InternalNode<key_t>*> stack;
    LeafNode<key_t>* leaf = nullptr;
    uint64_t leaf_vstart = 0;
    leaf = traverse_to_leafnode(key, stack, &leaf_vstart);

    auto ret = leaf->find(key);
    auto leaf_vend = leaf->get_version(need_restart);
    if (need_restart || (leaf_vstart != leaf_vend)) {
      goto restart;
    }

    return ret;
  }

  /**
   * @brief remove key-value pair from blinktree
   */
  bool remove(key_t key) {
  restart:
    bool need_restart = false;

    std::vector<InternalNode<key_t>*> stack;
    LeafNode<key_t>* leaf = nullptr;
    uint64_t leaf_vstart = 0;
    leaf = traverse_to_leafnode(key, stack, &leaf_vstart);

    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
    if (need_restart) {
      goto restart;
    }

    auto ret = leaf->remove(key);
    leaf->write_unlock();
    return ret;
  }

  /**
   * @brief lookup continuous @p range values greater than or equal to @p min_key .
   * @param min_key lookup begin
   * @param range lookup num
   * @param[out] buf lookuped values
   * @return the amount of values found out
   */
  int range_lookup(key_t min_key, int range, uint64_t* buf) {
  restart:
    bool need_restart = false;

    std::vector<InternalNode<key_t>*> stack;
    LeafNode<key_t>* leaf = nullptr;
    uint64_t leaf_vstart = 0;
    leaf = traverse_to_leafnode(min_key, stack, &leaf_vstart);

    int count = 0;
    auto idx = leaf->find_lowerbound(min_key);
    while (count < range) {
      auto ret = leaf->range_lookup(idx, buf, count, range);
      auto sibling = leaf->sibling_ptr;
      // collected all keys within range or reaches the rightmost leaf
      if ((ret == range) || !sibling) {
        auto leaf_vend = leaf->get_version(need_restart);
        if (need_restart || (leaf_vstart != leaf_vend)) {
          goto restart;
        }
        return ret;
      }
      auto sibling_vstart = sibling->try_readlock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto leaf_vend = leaf->get_version(need_restart);
      if (need_restart || (leaf_vstart != leaf_vend)) {
        goto restart;
      }

      leaf = static_cast<LeafNode<key_t>*>(sibling);
      leaf_vstart = sibling_vstart;
      count = ret;
      idx = 0;
    }
    return count;
  }

  /**
   * @brief return the height of blinktree
   */
  int height() { return root->level; }

 private:
  /**
   * @brief traverse tree from root to leaf by @p key
   * @param key lookup key
   * @param[out] stacks traversed nodes ptr
   * @param[out] leaf_version_start leafnode's read lock version
   * @return traversed leaf node
   */
  LeafNode<key_t>* traverse_to_leafnode(
      key_t key, std::vector<InternalNode<key_t>*>& stacks,
      uint64_t* leaf_version_start) {
  restart:
    auto cur = root;
    stacks.clear();
    stacks.reserve(root->level);

    bool need_restart = false;
    auto cur_vstart = cur->try_readlock(need_restart);
    if (need_restart) {
      goto restart;
    }

    // tree traversal
    while (cur->level != 0) {
      // Find the next node cotains key, may be next level node or next sibling
      // node.
      auto child = static_cast<InternalNode<key_t>*>(cur)->scan_node(key);
      auto child_vstart = child->try_readlock(need_restart);
      if (need_restart) {
        goto restart;
      }

      // When execute cur->scan_node(), cur may be changed by other thread.
      // So if strat_version != cur_version, means cur is changed,
      //    if need_restart == true, means cur is changing.
      auto cur_vend = cur->get_version(need_restart);
      if (need_restart || (cur_vstart != cur_vend)) {
        goto restart;
      }

      // If cur->scan_node() return sibling node, continue current level,
      // else go to next level.
      if (child != static_cast<InternalNode<key_t>*>(cur)->sibling_ptr) {
        stacks.push_back(static_cast<InternalNode<key_t>*>(cur));
      }

      cur = child;
      cur_vstart = child_vstart;
    }

    // get leaf node
    auto leaf = static_cast<LeafNode<key_t>*>(cur);
    auto leaf_vstart = cur_vstart;
    while (leaf->sibling_ptr && (leaf->high_key < key)) {
      auto sibling = static_cast<LeafNode<key_t>*>(leaf->sibling_ptr);
      auto sibling_vstart = sibling->try_readlock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto leaf_vend = leaf->get_version(need_restart);
      if (need_restart || (leaf_vstart != leaf_vend)) {
        goto restart;
      }

      leaf = sibling;
      leaf_vstart = sibling_vstart;
    }

    *leaf_version_start = leaf_vstart;
    return leaf;
  }

  /**
   * @brief The @p leaf node is full, so it needs to be split recursively.
   *        The caller needs to ensure that the current thread holds the write
   *        lock of @p leaf .
   * @param stack traversed nodes ptr
   * @param leaf  the leaf node that should be split, current thread should hold
   * the write lock of @p leaf
   * @param key   the key need to be inserted into leaf
   * @param value the value need to be inserted into leaf
   */
  void backtrack_insertion_split_key(
      const std::vector<InternalNode<key_t>*>& stack, LeafNode<key_t>* leaf,
      key_t key, uint64_t value) {
    // leaf node is full, need split
    key_t split_key;
    auto new_leaf = leaf->split(split_key);
    if (key <= split_key) {
      leaf->insert(key, value);
    } else {
      new_leaf->insert(key, value);
    }

    // current leaf node is root
    if (stack.empty()) {
      // root node not changed
      if (leaf == root) {
        auto new_root =
            new InternalNode<key_t>(split_key, leaf, new_leaf, nullptr,
                                    leaf->level + 1, new_leaf->high_key);
        root = static_cast<Node*>(new_root);
        leaf->write_unlock();
      } else {  // other thread changed the root
        update_splitted_root(split_key, new_leaf, leaf);
        return;
      }
    } else {  // current leaf node is not root
      bool need_restart = false;
      int stack_idx = stack.size() - 1;

      auto parent = stack[stack_idx];
      auto left_node = static_cast<Node*>(leaf);
      auto right_node = static_cast<Node*>(new_leaf);

      // backtrack internal node split
      while (stack_idx > -1) {
        parent = stack[stack_idx];
      parent_restart:
        need_restart = false;
        auto parent_vstart = parent->try_readlock(need_restart);
        if (need_restart) {
          goto parent_restart;
        }

        while (parent->sibling_ptr && (parent->high_key < split_key)) {
          auto p_sibling = parent->sibling_ptr;
          auto p_sibling_vstart = p_sibling->try_readlock(need_restart);
          if (need_restart) {
            goto parent_restart;
          }

          auto parent_vend = parent->get_version(need_restart);
          if (need_restart || (parent_vstart != parent_vend)) {
            goto parent_restart;
          }

          parent = static_cast<InternalNode<key_t>*>(p_sibling);
          parent_vstart = p_sibling_vstart;
        }

        parent->try_upgrade_writelock(parent_vstart, need_restart);
        if (need_restart) {
          goto parent_restart;
        }

        left_node->write_unlock();

        // normal insert
        if (!parent->is_full()) {
          parent->insert(split_key, right_node);
          parent->write_unlock();
          return;
        }

        // internal node split
        key_t insert_key = split_key;
        auto new_parent = parent->split(split_key);
        if (insert_key < split_key) {
          parent->insert(insert_key, right_node);
        } else {
          new_parent->insert(insert_key, right_node);
        }

        left_node = static_cast<Node*>(parent);
        right_node = static_cast<Node*>(new_parent);
        if (stack_idx) {
          insert_key = split_key;
          parent = stack[--stack_idx];
        } else {
          if (parent == root) {
            auto new_root = new InternalNode<key_t>(
                split_key, left_node, right_node, nullptr, parent->level + 1,
                new_parent->high_key);
            root = static_cast<Node*>(new_root);
            parent->write_unlock();
            return;
          } else {
            update_splitted_root(split_key, right_node, left_node);
            return;
          }
        }
      }
    }
  }

  /**
   * @brief this function is called when root has been split by another threads
   * @param key   middle key should be insert into root
   * @param value splitted right node
   * @param prev  splitted left node
   */
  void update_splitted_root(key_t key, Node* value, Node* prev) {
  restart:
    auto cur = root;
    bool need_restart = false;

    auto cur_vstart = cur->try_readlock(need_restart);
    if (need_restart) {
      goto restart;
    }

    // since we need to find the internal node which has been previously the
    // root, we use readlock for traversal
    while (cur->level != prev->level + 1) {
      auto child = (static_cast<InternalNode<key_t>*>(cur))->scan_node(key);
      auto child_vstart = child->try_readlock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto cur_vend = cur->get_version(need_restart);
      if (need_restart || (cur_vstart != cur_vend)) {
        goto restart;
      }

      cur = child;
      cur_vstart = child_vstart;
    }

    // found parent level node
    while ((static_cast<InternalNode<key_t>*>(cur))->sibling_ptr &&
           ((static_cast<InternalNode<key_t>*>(cur))->high_key < key)) {
      auto sibling = (static_cast<InternalNode<key_t>*>(cur))->sibling_ptr;
      auto sibling_vstart = sibling->try_readlock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto cur_vend = cur->get_version(need_restart);
      if (need_restart || (cur_vstart != cur_vend)) {
        goto restart;
      }

      cur = static_cast<InternalNode<key_t>*>(sibling);
      cur_vstart = sibling_vstart;
    }

    cur->try_upgrade_writelock(cur_vstart, need_restart);
    if (need_restart) {
      goto restart;
    }
    prev->write_unlock();

    auto node = static_cast<InternalNode<key_t>*>(cur);
    if (!node->is_full()) {
      node->insert(key, value);
      node->write_unlock();
      return;
    } else {
      key_t split_key;
      auto new_node = node->split(split_key);
      if (key <= split_key)
        node->insert(key, value);
      else
        new_node->insert(key, value);

      if (node == root) {  // if current nodes is root
        auto new_root =
            new InternalNode<key_t>(split_key, node, new_node, nullptr,
                                    node->level + 1, new_node->high_key);
        root = static_cast<Node*>(new_root);
        node->write_unlock();
        return;
      } else {  // other thread has already created a new root
        update_splitted_root(split_key, new_node, node);
        return;
      }
    }
  }

};  // class BLinkTree
}  // namespace BLINK_TREE

#endif  // BLINK_TREE_