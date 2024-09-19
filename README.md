# MiniSQL

A mini database management system supporting simple SQL requests such as SELECT, INSERT, DELETE and UPDATE.

Key features:

- Paged disk and buffer management.
- Utilizing B+-tree to optimize indexing.
- Hand-written parser.



## Build

```bash
mkdir build
cd build
cmake ..
make -j
```