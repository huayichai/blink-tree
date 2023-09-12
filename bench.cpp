#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "blinktree.h"

using Key_t = uint64_t;

using namespace BLINK_TREE;

/**
 * @brief Generate data in random order within the range of [begin, end)
 * @param datas [output]
 */
template <typename T>
void generate_data(T* datas, int begin, int end) {
  for (int i = begin; i < end; i++) {
    datas[i] = i + 1;
  }
  std::random_shuffle(datas, datas + end - begin);
}

/**
 * @brief Execute concurrent insert into tree.
 */
void concurrent_insert(BLinkTree<Key_t>* tree, Key_t* keys, int num_data,
                       int num_threads) {
  size_t chunk = num_data / num_threads;
  auto insert = [&tree, &keys, chunk, num_data, num_threads](int tid) {
    int from = chunk * tid;
    int to = chunk * (tid + 1);
    for (int i = from; i < to; i++) {
      tree->insert(keys[i], (uint64_t)&keys[i]);
    }
  };

  std::vector<std::thread> insert_threads;
  std::cout << "Insertion Start" << std::endl;
  const auto insertion_start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < num_threads; i++) {
    insert_threads.push_back(std::thread(insert, i));
  }
  for (auto& t : insert_threads) {
    t.join();
  }
  const auto insertion_end = std::chrono::high_resolution_clock::now();
  const auto insertion_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  insertion_end - insertion_start)
                                  .count();

  std::cout << "Insertion time: " << insertion_time / 1000000000.0 << " sec" << std::endl;
  std::cout << "throughput: " << num_data / (double)insertion_time * 1000000000.0 / 1000000
            << " mops/sec" << std::endl;
}

/**
 * @brief Execute concurrent search.
 */
void concurrent_search(BLinkTree<Key_t>* tree, Key_t* keys, int num_data,
                       int num_threads) {
  size_t chunk = num_data / num_threads;
  std::vector<uint64_t> notfound_keys[num_threads];
  auto search = [&tree, &keys, &notfound_keys, chunk, num_data,
                 num_threads](int tid) {
    int from = chunk * tid;
    int to = chunk * (tid + 1);
    for (int i = from; i < to; i++) {
      auto ret = tree->lookup(keys[i]);
      if (ret != (uint64_t)&keys[i]) {
        notfound_keys[tid].push_back(i);
      }
    }
  };

  std::vector<std::thread> insert_threads;
  std::cout << "Search Start" << std::endl;
  const auto search_start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < num_threads; i++) {
    insert_threads.push_back(std::thread(search, i));
  }
  for (auto& t : insert_threads) {
    t.join();
  }
  const auto search_end = std::chrono::high_resolution_clock::now();
  const auto search_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               search_end - search_start)
                               .count();
  std::cout << "Search time: " << search_time / 1000000000.0 << " sec" << std::endl;
  std::cout << "throughput: " << num_data / (double)search_time * 1000000000.0 / 1000000
            << " mops/sec" << std::endl;
  bool not_found = false;
  for (int i = 0; i < num_threads; i++) {
    for (auto &it : notfound_keys[i]) {
      auto ret = tree->lookup(keys[it]);
      if (ret != (uint64_t)&keys[it]) {
        std::cout << "key " << keys[it] << " not found" << std::endl;
        not_found = true;
      }
    }
  }

  auto height = tree->height();
  std::cout << "Height of tree: " << height + 1 << std::endl;
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " num_data num_threads" << std::endl;
    exit(0);
  }
  int num_data = atoi(argv[1]);
  int num_threads = atoi(argv[2]);
  Key_t* keys = new Key_t[num_data];
  generate_data<Key_t>(keys, 0, num_data);

  auto tree = new BLinkTree<Key_t>();
  std::cout << "InternalNode_Size(" << InternalNode<Key_t>::cardinality << "), "
            << "LeafNode_Size(" << LeafNode<Key_t>::cardinality << ")"
            << std::endl;

  concurrent_insert(tree, keys, num_data, num_threads);
  concurrent_search(tree, keys, num_data, num_threads);
}