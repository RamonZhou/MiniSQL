#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 3000;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  // ShuffleArray(keys);ddd
  // ShuffleArray(values);
  // ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  std::cout << "Inserted " << n << " records." << std::endl;
  // Print tree
  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  int half = n;
  for (int i = 0; i < half; i++) {
    // printf("[%d]: Remove %d\n", i+1, delete_seq[i]);
    if(i > 890 && i < 902) std::cout<<i<<" "<<delete_seq[i]<<std::endl;
    tree.Remove(delete_seq[i]);
    if (i > 860 && i < 890) tree.PrintTree(mgr[i-860]);
  }
  // tree.PrintTree(mgr[1]);
  std::cout << "Deleted " << n << " records." << std::endl;
  // Check valid
  ans.clear();
  for (int i = 0; i < half; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = half; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
  // tree.Destroy();
  // ASSERT_TRUE(tree.IsEmpty());
}