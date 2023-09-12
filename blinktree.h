#ifndef BLINK_TREE_
#define BLINK_TREE_
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
   * @brief Tree height
   */
  int check_height() { return root->level; }

  void insert(key_t key, uint64_t value) {
    int restart_cnt = -1;
  restart:
    auto cur = root;

    int stack_cnt = 0;
    InternalNode<key_t>* stack[root->level];

    bool need_restart = false;
    auto cur_vstart = cur->whether_lock(need_restart);
    if (need_restart) {
      goto restart;
    }

    // tree traversal
    while (cur->level != 0) {
      // Find the child node cotains key.
      auto child = (static_cast<InternalNode<key_t>*>(cur))->scan_node(key);
      auto child_vstart = child->whether_lock(need_restart);
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
      if (child != (static_cast<InternalNode<key_t>*>(cur))->sibling_ptr) {
        stack[stack_cnt++] = static_cast<InternalNode<key_t>*>(cur);
      }
      cur = child;
      cur_vstart = child_vstart;
    }

    // get leaf node
    auto leaf = static_cast<LeafNode<key_t>*>(cur);
    auto leaf_vstart = cur_vstart;

    while (leaf->sibling_ptr && (leaf->high_key < key)) {
      auto sibling = static_cast<LeafNode<key_t>*>(leaf->sibling_ptr);
      auto sibling_v = sibling->whether_lock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto leaf_vend = leaf->get_version(need_restart);
      if (need_restart || (leaf_vstart != leaf_vend)) {
        goto restart;
      }

      leaf = sibling;
      leaf_vstart = sibling_v;
    }

    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
    if (need_restart) {
      goto restart;
    }

    if (!leaf->is_full()) {  // normal insert
      leaf->insert(key, value);
      leaf->write_unlock();
      return;
    } else {            // leaf node split
      key_t split_key;  /// here
      auto new_leaf = leaf->split(split_key);
      if (key <= split_key) leaf->insert(key, value);
      else new_leaf->insert(key, value);

      if (stack_cnt) {
        int stack_idx = stack_cnt - 1;
        auto old_parent = stack[stack_idx];

        auto original_node = static_cast<Node*>(leaf);
        auto new_node = static_cast<Node*>(new_leaf);

        while (stack_idx > -1) {  // backtrack
          old_parent = stack[stack_idx];

        parent_restart:
          need_restart = false;
          auto parent_vstart = old_parent->whether_lock(need_restart);
          if (need_restart) {
            goto parent_restart;
          }

          while (old_parent->sibling_ptr &&
                 (old_parent->high_key < split_key)) {
            auto p_sibling = old_parent->sibling_ptr;
            auto p_sibling_v = p_sibling->whether_lock(need_restart);
            if (need_restart) {
              goto parent_restart;
            }

            auto parent_vend = old_parent->get_version(need_restart);
            if (need_restart || (parent_vstart != parent_vend)) {
              goto parent_restart;
            }

            old_parent = static_cast<InternalNode<key_t>*>(p_sibling);
            parent_vstart = p_sibling_v;
          }

          old_parent->try_upgrade_writelock(parent_vstart, need_restart);
          if (need_restart) {
            goto parent_restart;
          }

          original_node->write_unlock();

          if (!old_parent->is_full()) {  // normal insert
            old_parent->insert(split_key, new_node);
            old_parent->write_unlock();
            return;
          }

          // internal node split
          key_t _split_key;
          auto new_parent = old_parent->split(_split_key);
          if (split_key <= _split_key) old_parent->insert(split_key, new_node);
          else new_parent->insert(split_key, new_node);

          if (stack_idx) {
            original_node = static_cast<Node*>(old_parent);
            new_node = static_cast<Node*>(new_parent);
            split_key = _split_key;
            old_parent = stack[--stack_idx];
          } else {                     // set new root
            if (old_parent == root) {  // current node is root
              auto new_root = new InternalNode<key_t>(
                  _split_key, old_parent, new_parent, nullptr,
                  old_parent->level + 1, new_parent->high_key);
              root = static_cast<Node*>(new_root);
              old_parent->write_unlock();
              return;
            } else {  // other thread has already created a new root
              insert_key(_split_key, new_parent, old_parent);
              return;
            }
          }
        }
      } else {               // set new root
        if (root == leaf) {  // current node is root
          auto new_root =
              new InternalNode<key_t>(split_key, leaf, new_leaf, nullptr,
                                 root->level + 1, new_leaf->high_key);
          root = static_cast<Node*>(new_root);
          leaf->write_unlock();
          return;
        } else {  // other thread has already created a new root
          insert_key(split_key, new_leaf, leaf);
          return;
        }
      }
    }
  }

  /**
   * @brief this function is called when root has been split by another threads
   */
  void insert_key(key_t key, Node* value, Node* prev) {
  restart:
    auto cur = root;
    bool need_restart = false;

    auto cur_vstart = cur->whether_lock(need_restart);
    if (need_restart) {
      goto restart;
    }

    // since we need to find the internal node which has been previously the
    // root, we use readlock for traversal
    while (cur->level != prev->level + 1) {
      auto child = (static_cast<InternalNode<key_t>*>(cur))->scan_node(key);
      auto child_vstart = child->whether_lock(need_restart);
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
      auto sibling_vstart = sibling->whether_lock(need_restart);
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
        auto new_root = new InternalNode<key_t>(split_key, node, new_node, nullptr,
                                           node->level + 1, new_node->high_key);
        root = static_cast<Node*>(new_root);
        node->write_unlock();
        return;
      } else {  // other thread has already created a new root
        insert_key(split_key, new_node, node);
        return;
      }
    }
  }

  bool update(key_t key, uint64_t value) {
    int restart_cnt = -1;
  restart:
    auto cur = root;
    bool need_restart = false;
    auto cur_vstart = cur->whether_lock(need_restart);
    if (need_restart) {
      goto restart;
    }

    // traversal
    while (cur->level != 0) {
      auto child = (static_cast<InternalNode<key_t>*>(cur))->scan_node(key);
      auto child_vstart = child->whether_lock(need_restart);
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

    // found leaf
    auto leaf = static_cast<LeafNode<key_t>*>(cur);
    auto leaf_vstart = cur_vstart;

    // move right if necessary
    while (leaf->sibling_ptr && (leaf->high_key < key)) {
      auto sibling = leaf->sibling_ptr;
      auto sibling_v = sibling->whether_lock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto leaf_vend = leaf->get_version(need_restart);

      if (need_restart || (leaf_vstart != leaf_vend)) {
        goto restart;
      }

      leaf = static_cast<LeafNode<key_t>*>(sibling);
      leaf_vstart = sibling_v;
    }

    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
    if (need_restart) {
      goto restart;
    }

    bool ret = leaf->update(key, value);
    leaf->write_unlock();

    return ret;
  }

  uint64_t lookup(key_t key) {
  restart:
    auto cur = root;
    bool need_restart = false;

    auto cur_vstart = cur->whether_lock(need_restart);
    if (need_restart) {
      goto restart;
    }

    // traversal
    while (cur->level != 0) {
      auto child = (static_cast<InternalNode<key_t>*>(cur))->scan_node(key);
      auto child_vstart = child->whether_lock(need_restart);
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

    // found leaf
    auto leaf = static_cast<LeafNode<key_t>*>(cur);
    auto leaf_vstart = cur_vstart;

    // move right if necessary
    while (leaf->sibling_ptr && (leaf->high_key < key)) {
      auto sibling = leaf->sibling_ptr;
      auto sibling_v = sibling->whether_lock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto leaf_vend = leaf->get_version(need_restart);
      if (need_restart || (leaf_vstart != leaf_vend)) {
        goto restart;
      }

      leaf = static_cast<LeafNode<key_t>*>(sibling);
      leaf_vstart = sibling_v;
    }

    auto ret = leaf->find(key);
    auto leaf_vend = leaf->get_version(need_restart);
    if (need_restart || (leaf_vstart != leaf_vend)) {
      goto restart;
    }

    return ret;
  }

  bool remove(key_t key) {
  restart:
    auto cur = root;
    bool need_restart = false;

    int stack_cnt = 0;
    InternalNode<key_t>* stack[root->level];

    auto cur_vstart = cur->whether_lock(need_restart);
    if (need_restart) {
      goto restart;
    }

    // traversal
    while (cur->level != 0) {
      auto child = (static_cast<InternalNode<key_t>*>(cur))->scan_node(key);
      auto child_vstart = child->whether_lock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto cur_vend = cur->get_version(need_restart);
      if (need_restart || (cur_vstart != cur_vend)) {
        goto restart;
      }

      if (child != (static_cast<InternalNode<key_t>*>(cur))->sibling_ptr)
        stack[stack_cnt++] = static_cast<InternalNode<key_t>*>(cur);

      cur = child;
      cur_vstart = child_vstart;
    }

    // found leaf
    auto leaf = static_cast<LeafNode<key_t>*>(cur);
    auto leaf_vstart = cur_vstart;

    while (leaf->sibling_ptr && (leaf->high_key < key)) {
      auto sibling = static_cast<LeafNode<key_t>*>(leaf->sibling_ptr);
      auto sibling_v = sibling->whether_lock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto leaf_vend = leaf->get_version(need_restart);
      if (need_restart || (leaf_vstart != leaf_vend)) {
        goto restart;
      }

      leaf = sibling;
      leaf_vstart = sibling_v;
    }

    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
    if (need_restart) {
      goto restart;
    }

    auto ret = leaf->remove(key);
    leaf->write_unlock();
    return ret;
  }

  int range_lookup(key_t min_key, int range, uint64_t* buf) {
  restart:
    auto cur = root;
    bool need_restart = false;
    auto cur_vstart = cur->whether_lock(need_restart);
    if (need_restart) {
      goto restart;
    }

    // traversal
    while (cur->level != 0) {
      auto child = (static_cast<InternalNode<key_t>*>(cur))->scan_node(min_key);
      auto child_vstart = child->whether_lock(need_restart);
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

    // found leaf
    int count = 0;
    auto leaf = static_cast<LeafNode<key_t>*>(cur);
    auto leaf_vstart = cur_vstart;

    // move right if necessary
    while (leaf->sibling_ptr && (leaf->high_key < min_key)) {
      auto sibling = leaf->sibling_ptr;

      auto sibling_v = sibling->whether_lock(need_restart);
      if (need_restart) {
        goto restart;
      }

      auto leaf_vend = leaf->get_version(need_restart);
      if (need_restart || (leaf_vstart != leaf_vend)) {
        goto restart;
      }

      leaf = static_cast<LeafNode<key_t>*>(sibling);
      leaf_vstart = sibling_v;
    }

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
      auto sibling_vstart = sibling->whether_lock(need_restart);
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

  int height() {
    auto cur = root;
    return cur->level;
  }
};  // class BLinkTree
}  // namespace BLINK_TREE

#endif  // BLINK_TREE_