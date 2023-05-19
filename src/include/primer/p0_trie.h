// ===----------------------------------------------------------------------===//
//
//                          BusTub
//
//  p0_trie.h
//
//  Identification: src/include/primer/p0_trie.h
//
//  Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
// ===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {
/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) {
    this->key_char_ = key_char;
    this->is_end_ = false;
    this->children_.clear();
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */

  TrieNode(TrieNode &&other_trie_node) noexcept {
    this->key_char_ = other_trie_node.key_char_;
    this->is_end_ = other_trie_node.is_end_;
    this->children_.swap(other_trie_node.children_);
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return children_.find(key_char) != children_.end(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return !children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return this->is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return this->key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    if (HasChild(key_char) || key_char != child->key_char_) {
      return nullptr;
    }
    children_[key_char] = std::forward<std::unique_ptr<TrieNode>>(child);
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    auto node = children_.find(key_char);
    if (node != children_.end()) {
      return &(node->second);
    }
    return nullptr;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    auto node = children_.find(key_char);
    if (node != children_.end()) {
      children_.erase(key_char);
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { this->is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that mark the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::forward<TrieNode>(trieNode)) {
    this->value_ = value;
    SetEndNode(true);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char) {
    this->value_ = value;
    SetEndNode(true);
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() {
    auto *root = new TrieNode('\0');
    root_.reset(root);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If key is empty string, return false immediately.
   *
   * If key alreadys exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and return false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    if (key.empty()) {
      return false;
    }
    latch_.WLock();
    auto c = key.begin();
    auto pre_child = &root_;
    while (c != key.end()) {
      auto cur = c++;
      // 若当前节点将为 end 节点，跳出循环进行特殊处理
      if (c == key.end()) {
        break;
      }

      // 如果该字符不存在 则直接创建
      if (!pre_child->get()->HasChild(*cur)) {
        pre_child = pre_child->get()->InsertChildNode(*cur, std::make_unique<TrieNode>(*cur));
      } else {
        // 存在则直接跳过
        pre_child = pre_child->get()->GetChildNode(*cur);
      }
    }

    // 此时c为end 退回一个
    c--;

    auto end_node = pre_child->get()->GetChildNode(*c);
    // 若最后一个节点存在，且已经为 end 则插入失败
    if (end_node != nullptr && end_node->get()->IsEndNode()) {
      latch_.WUnlock();
      return false;
    }
    // 若最后一个节点存在，且不为 end 则转为 TrieNodeWithValue
    if (end_node != nullptr) {
      auto new_node = new TrieNodeWithValue(std::move(**end_node), value);
      end_node->reset(new_node);
      latch_.WUnlock();
      return true;
    }
    //  节点不存在，则直接插入
    pre_child = pre_child->get()->InsertChildNode(*c, std::make_unique<TrieNode>(*c));
    auto new_node = new TrieNodeWithValue(std::move(**pre_child), value);
    pre_child->reset(new_node);
    latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and is not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find correct node
   * @return True if key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    // 为空返回
    if (key.empty()) {
      return false;
    }
    latch_.WLock();
    // 含义为，第二个元素 TrieNode 需要删除 keyChar 等于第一个元素的节点
    std::stack<std::tuple<char, std::unique_ptr<TrieNode> *>> s;
    auto c = key.begin();
    auto pre_child = &root_;
    while (c != key.end()) {
      auto cur = c++;
      if (pre_child->get()->HasChild(*cur)) {
        s.push(std::make_tuple(*cur, pre_child));
        // 到下一个 key
        pre_child = pre_child->get()->GetChildNode(*cur);
        continue;
      }
      // 存在key没有的现象
      latch_.WUnlock();
      return false;
    }

    // 开始删除
    while (!s.empty()) {
      std::tuple<char, std::unique_ptr<TrieNode> *> temp = s.top();
      s.pop();
      auto key = std::get<0>(temp);
      auto del_node = std::get<1>(temp);
      // 若被删除的节点没有孩子，则可以直接删除
      auto flag = (*del_node)->GetChildNode(key);
      if (flag != nullptr && (*flag)->HasChildren()) {
        continue;
      }
      (*del_node)->RemoveChildNode(key);
    }
    latch_.WUnlock();

    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    // 这个初始化很重要，如果没有找到就直接返回 false,卡这里好久
    *success = false;
    latch_.RLock();

    auto pre_child = &root_;
    auto c = key.begin();
    while (c != key.end()) {
      auto cur = c++;
      auto next_node = pre_child->get()->GetChildNode(*cur);

      if (!next_node) {
        *success = false;
        break;
      }
      // 若是目标尾节点，返回值
      // 这里犯了个错，并非只要是 end 节点就是所需要的节点，key也得对，所以要 key == end 时
      if (next_node->get()->IsEndNode() && c == key.end()) {
        // 若所需要类型和存储的类型不同也失败
        auto flag_node = dynamic_cast<TrieNodeWithValue<T> *>(next_node->get());
        if (!flag_node) {
          *success = false;
          break;
        }
        *success = true;
        latch_.RUnlock();
        return flag_node->GetValue();
      }
      // 切换到下个
      pre_child = next_node;
    }
    latch_.RUnlock();
    return {};
  }
};
}  //  namespace bustub
