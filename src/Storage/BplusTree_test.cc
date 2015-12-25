#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"
#include "BplusTree.h"

namespace DataBaseFiles {

class BplusTreeTest: public UnitTest {
 private:
  

 public:
  void setup() override {
  }

  void teardown() override {
  }  

  void Test_Header_Page_Consistency_Check() {
    const char* datafile = "test/test_BplusTree1.indata";
    BplusTree tree;
    AssertTrue(tree.CreateFile(datafile, INDEX_DATA),
                 "Create B+ tree file faild");

    tree.meta()->set_num_pages(1);
    AssertFalse(tree.SaveToDisk());
    tree.meta()->set_num_pages(0);

    tree.meta()->set_num_pages(1);
    tree.meta()->set_num_free_pages(1);
    AssertFalse(tree.SaveToDisk());
    tree.meta()->set_num_pages(0);
    tree.meta()->set_num_free_pages(0);

    tree.meta()->set_free_page(0);
    AssertFalse(tree.SaveToDisk());
    tree.meta()->set_free_page(-1);

    tree.meta()->set_num_leaves(1);
    AssertFalse(tree.SaveToDisk());
    tree.meta()->set_num_leaves(0);

    AssertTrue(tree.SaveToDisk());
  }

  void Test_Create_Load_Empty_Tree() {
    const char* datafile = "test/test_BplusTree1.indata";
    // Create a new empty B+ tree and save it file.
    {
      BplusTree tree;
      AssertTrue(tree.CreateFile(datafile, INDEX_DATA),
                 "Create B+ tree file faild");
      AssertTrue(tree.SaveToDisk(), "Save to disk failed");
    }

    // Load a B +Tree from this file.
    {
      BplusTree tree2(datafile);
      AssertEqual(INDEX_DATA, tree2.meta()->file_type());
      AssertEqual(0, tree2.meta()->num_pages());
      AssertEqual(0, tree2.meta()->num_free_pages());
      AssertEqual(0, tree2.meta()->num_used_pages());
      AssertEqual(-1, tree2.meta()->free_page());
      AssertEqual(-1, tree2.meta()->root_page());
      AssertEqual(0, tree2.meta()->num_leaves());
      AssertEqual(0, tree2.meta()->depth());
    }
  }

  void Test_SortRecords() {
    std::vector<Schema::Record> records(10);
    for (int i = 0; i < 10; i++) {
      records[i].AddField(new Schema::IntType(10 - i / 5));
      records[i].AddField(new Schema::LongIntType(1111111111111));
      records[i].AddField(new Schema::DoubleType(3.5));
      records[i].AddField(new Schema::BoolType(false));
      if (i % 2 == 0) {
        records[i].AddField(new Schema::StringType("axy"));  
      }
      else {
        records[i].AddField(new Schema::StringType("abc"));
      }
      records[i].AddField(new Schema::CharArrayType("acd", 3, 10));
      records[i].Print();
    }

    BplusTree::SortRecords(records, std::vector<int>{4, 0});
    std::cout << "After sorting:" << std::endl;
    for (int i = 0; i < 10; i++) {
      records[i].Print();
    }
    AssertTrue(Schema::RecordBase::RecordComparator(records[4], records[5], std::vector<int>{4, 0}));
  }
};

}  // namespace DataBaseFiles

int main() {
  DataBaseFiles::BplusTreeTest test;
  test.setup();
  test.Test_Header_Page_Consistency_Check();
  test.Test_Create_Load_Empty_Tree();
  test.Test_SortRecords();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}

