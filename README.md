# BLink-Tree

## build
```bash
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```

## test
```bash
./bench 1000000 10
InternalNode_Size(30), LeafNode_Size(30)
Insertion Start
Insertion time: 0.0894113 sec
throughput: 11.1843 mops/sec
Search Start
Search time: 0.0188568 sec
throughput: 53.0313 mops/sec
Height of tree: 5
```