#include "Base/Log.h"
#include "Base/MacroUtils.h"
#include "UnitTest/UnitTest.h"

#include "Query/Interpreter.h"

using Storage::RecordBase;
using Storage::RecordID;
using Storage::DataRecord;
using Storage::IndexRecord;

namespace Query {

namespace {
const char* const kTableName = "testTable";
}  // namespace

class InterpreterTest: public UnitTest {
 private:
  std::shared_ptr<RecordBase> data_record_;
  std::shared_ptr<RecordBase> index_record_;
  DB::TableInfo schema_;
  std::vector<int> key_fields_ = std::vector<int>{0, 2, 3};

  Interpreter interpreter_;

 public:
  void InitSchema() {
    // Create a table schema_.
    schema_.set_name(kTableName);
    // Add string type
    DB::TableField* field = schema_.add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(DB::TableField::STRING);
    // Add int type
    field = schema_.add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(DB::TableField::INTEGER);
    // Add long int type
    field = schema_.add_fields();
    field->set_name("id");
    field->set_index(2);  // primary key
    field->set_type(DB::TableField::LLONG);
    // Add double type
    field = schema_.add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(DB::TableField::DOUBLE);
    // Add bool type
    field = schema_.add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(DB::TableField::BOOL);
    // Add char array type
    field = schema_.add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(DB::TableField::CHARARR);
    field->set_size(20);
  }

  void InitRecordResource() {
    auto data_record_ = std::make_shared<DataRecord>();

    // Init fields to records.
    // name
    {
      int str_len = Utils::RandomNumber(10);
      char buf[str_len];
      for (int i = 0; i < str_len; i++) {
        buf[i] = 'a' + Utils::RandomNumber(26);
      }
      data_record_->AddField(new Schema::StringField(buf, str_len));
    }
    // age
    int rand_int = Utils::RandomNumber(20);
    data_record_->AddField(new Schema::IntField(rand_int));
    // weight
    double rand_double = 1.0 * Utils::RandomNumber() / Utils::RandomNumber();
    data_record_->AddField(new Schema::DoubleField(rand_double));
    // adult
    bool rand_bool = Utils::RandomNumber() % 2 == 1 ? true : false;
    data_record_->AddField(new Schema::BoolField(rand_bool));
    // signature
    {
      int len_limit = 20;
      int str_len = Utils::RandomNumber(len_limit) + 1;
      char buf[str_len];
      for (int i = 0; i < str_len; i++) {
        buf[i] = 'a' + Utils::RandomNumber(26);
      }
      data_record_->AddField(
          new Schema::CharArrayField(buf, str_len, len_limit));
    }

    auto index_record_ = std::make_shared<IndexRecord>();
    data_record_->ExtractKey(index_record_.get(), key_fields_);
    index_record_->set_rid(RecordID(0, 0));
  }

  void setup() {
    InitSchema();
    InitRecordResource();
  }

  void Test_EvaluateConst() {
    interpreter_.set_debug(true);
    std::string expr;

    expr = "1 != 1.0\n";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_.parse(expr));
    auto node = interpreter_.GetCurrentNode();
    AssertTrue(node->valid());
    node->Evaluate();
    AssertEqual(node->value().type, Query::BOOL);
    AssertFalse(node->value().v_bool);
    std::cout << node->value().AsString() << std::endl;
    printf("\n");

    expr = "1 + 2 * -3";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_.parse(expr));
    node = interpreter_.GetCurrentNode();
    AssertTrue(node->valid());
    node->Evaluate();
    AssertEqual(node->value().type, Query::INT64);
    AssertEqual(-5, node->value().v_int64);
    std::cout << node->value().AsString() << std::endl;
    printf("\n");

    expr = "(-1.5 + 3 )* 2";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_.parse(expr));
    node = interpreter_.GetCurrentNode();
    AssertTrue(node->valid());
    node->Evaluate();
    AssertEqual(node->value().type, Query::DOUBLE);
    AssertFloatEqual(3.0, node->value().v_double);
    std::cout << node->value().AsString() << std::endl;
    printf("\n");

    expr = "1 + 2 * -3 - 4/0.5 <= 8.0";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_.parse(expr));
    node = interpreter_.GetCurrentNode();
    AssertTrue(node->valid());
    node->Evaluate();
    AssertEqual(node->value().type, Query::BOOL);
    AssertTrue(node->value().v_bool);
    std::cout << node->value().AsString() << std::endl;
    printf("\n");
  }
};

}  // namespace Storage

int main() {
  Query::InterpreterTest test;
  test.setup();

  test.Test_EvaluateConst();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
