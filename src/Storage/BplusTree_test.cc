#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"
#include "BplusTree.h"
#include "Schema/DBTable_pb.h"

namespace DataBaseFiles {

class BplusTreeTest: public UnitTest {
 private:
  std::string tablename = "testTable";
  std::vector<int> key_indexes;
  Schema::TableSchema* schema = nullptr;

 public:
  void setup() override {
    // Create key_indexes.
    key_indexes = std::vector<int>{1, 2};

    // Create a table schema.
    schema = new Schema::TableSchema();
    schema->set_name(tablename);
    // Add string type
    Schema::TableField* field = schema->add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(Schema::TableField::STRING);
    // Add int type
    field = schema->add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(Schema::TableField::INTEGER);
    // Add long int type
    field = schema->add_fields();
    field->set_name("money");
    field->set_index(2);
    field->set_type(Schema::TableField::LLONG);
    // Add double type
    field = schema->add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(Schema::TableField::DOUBLE);
    // Add bool type
    field = schema->add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(Schema::TableField::BOOL);
    // Add char array type
    field = schema->add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(Schema::TableField::CHARARR);
    field->set_size(20);
  }

  void teardown() override {
    if (schema) {
      delete schema;
    }
  }

  bool CreateSchemaFile(std::string tablename) {
    // Serialize the schema message and write to schema file
    ::proto::SerializedMessage* sdmsg = schema->Serialize();
    const char* obj_data = sdmsg->GetBytes();

    std::string schema_filename = kDataDirectory + tablename + ".schema.pb";
    FILE* file = fopen(schema_filename.c_str(), "w+");
    if (!file) {
      LogERROR("Failed to open schema file %s", schema_filename.c_str());
      return false;
    }
    // Read schema file.
    int re = fwrite(obj_data, 1, sdmsg->size(), file);
    if (re != (int)sdmsg->size()) {
      LogERROR("Read schema file %s error, expect %d bytes, actual %d",
               schema_filename.c_str(), sdmsg->size(), re);
      return false;
    }
    fclose(file);
    return true;
  }

  void Test_SchemaFile() {
    std::string tablename = "testTable";
    AssertTrue(CreateSchemaFile(tablename));

    // Create a B+ tree and load the schema.
    BplusTree tree;
    tree.set_tablename(tablename);
    tree.LoadSchema();
    const auto* loaded_schema = tree.schema();
    AssertTrue(loaded_schema->Equals(*schema),
                "Loaded schema differs from original");
  }

  void Test_Header_Page_Consistency_Check() {
    std::string tablename = "testTable";
    BplusTree tree;
    AssertTrue(tree.CreateFile(tablename, key_indexes, INDEX_DATA),
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
    std::string tablename = "testTable";
    // Create a new empty B+ tree and save it file.
    {
      BplusTree tree;
      AssertTrue(tree.CreateFile(tablename, key_indexes, INDEX_DATA),
                 "Create B+ tree file faild");
      AssertTrue(tree.SaveToDisk(), "Save to disk failed");
    }

    // Load a B +Tree from this file.
    {
      BplusTree tree2(tablename, key_indexes);
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
  }
};

}  // namespace DataBaseFiles

int main() {
  DataBaseFiles::BplusTreeTest test;
  test.setup();
  test.Test_SchemaFile();
  test.Test_Header_Page_Consistency_Check();
  test.Test_Create_Load_Empty_Tree();
  test.Test_SortRecords();
  test.teardown();

  std::cout << "\033[2;32mAll Passed ^_^\033[0m" << std::endl;
  return 0;
}

