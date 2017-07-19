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
const char* const kTableName = "Puppy";
const char* const kDBName = "testDB";
}  // namespace

class InterpreterTest: public UnitTest {
 private:
  std::shared_ptr<RecordBase> data_record_;
  std::shared_ptr<RecordBase> index_record_;
  std::vector<int> key_fields_ = std::vector<int>{0, 2, 3};

  DB::DatabaseCatalog catalog_;
  std::shared_ptr<DB::CatalogManager> catalog_m_;

  std::shared_ptr<Interpreter> interpreter_;

 public:
  void InitCatalog() {
    catalog_.set_name(kDBName);

    // Create a table Puppy.
    auto table_info = catalog_.add_tables();
    table_info->set_name(kTableName);

    // Add string type
    DB::TableField* field = table_info->add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(DB::TableField::STRING);
    // Add int type
    field = table_info->add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(DB::TableField::INT);
    // Add long int type
    field = table_info->add_fields();
    field->set_name("id");
    field->set_index(2);  // primary key
    field->set_type(DB::TableField::LONGINT);
    // Add double type
    field = table_info->add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(DB::TableField::DOUBLE);
    // Add bool type
    field = table_info->add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(DB::TableField::BOOL);
    // Add char array type
    field = table_info->add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(DB::TableField::CHARARRAY);
    field->set_size(20);

    // Create a table Host.
    table_info = catalog_.add_tables();
    table_info->set_name("Host");

    // Add string type
    field = table_info->add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(DB::TableField::STRING);
    // Add long int type
    field = table_info->add_fields();
    field->set_name("id");
    field->set_index(1);  // primary key
    field->set_type(DB::TableField::LONGINT);

    catalog_m_ = std::make_shared<DB::CatalogManager>(&catalog_);
    AssertTrue(catalog_m_->Init());
  }

  void InitRecordResource() {
    auto* data_record = new DataRecord();
    auto* index_record = new IndexRecord();

    // Init fields to records.
    // name
    data_record->AddField(new Schema::StringField("snoopy"));
    // age
    data_record->AddField(new Schema::IntField(3));
    // id
    data_record->AddField(new Schema::LongIntField(2));
    // weight
    data_record->AddField(new Schema::DoubleField(0.5));
    // adult
    data_record->AddField(new Schema::BoolField(false));
    // signature
    data_record->AddField(new Schema::CharArrayField("smart dog ^_^", 20));

    data_record->ExtractKey(index_record, key_fields_);
    index_record->set_rid(RecordID(0, 0));

    data_record_.reset(data_record);
    index_record_.reset(index_record);
  }

  void setup() {
    InitCatalog();
    InitRecordResource();

    interpreter_ = std::make_shared<Interpreter>(catalog_m_.get());
    interpreter_->set_debug(true);
  }

  void Test_EvaluateConst() {
    EvaluateArgs evalute_args(catalog_m_.get(),
                              *data_record_, Storage::DATA_RECORD,
                              key_fields_);

    std::string expr;

    expr = "1 != 1.0\n";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    auto node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    auto result = node->Evaluate(evalute_args);
    AssertEqual(result.type, BOOL);
    AssertFalse(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "1 + 2 * -3";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    result = node->Evaluate(evalute_args);
    AssertEqual(result.type, INT64);
    AssertEqual(-5, result.v_int64);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "(-1.5 + 3 )* 2";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    result = node->Evaluate(evalute_args);
    AssertEqual(result.type, DOUBLE);
    AssertFloatEqual(3.0, result.v_double);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "1 + 2 * -3 - 4/0.5 <= 8.0";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    result = node->Evaluate(evalute_args);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");
  }

  void Test_ColumnNodeExpr() {
    std::string expr;

    expr = "Puppy.name";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    auto node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    AssertEqual(node->value().type, STRING);
    printf("\n");

    expr = "Puppy.age";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    AssertEqual(node->value().type, INT64);
    printf("\n");

    expr = "Puppy.xx";
    std::cout << expr << std::endl;
    AssertFalse(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertFalse(node->valid());
    std::cout << "Error msg - " << node->error_msg() << std::endl;
    printf("\n");

    expr = "Puppy.id = 1";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    AssertEqual(node->value().type, BOOL);
    printf("\n");

    expr = "-Puppy.weight + 30.0 > 50";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    AssertEqual(node->type(), ExprTreeNode::OPERATOR);
    AssertEqual(node->value().type, BOOL);
    printf("\n");

    expr = "Puppy.signature + 3 = 6";
    std::cout << expr << std::endl;
    AssertFalse(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertFalse(node->valid());
    std::cout << "Error msg - " << node->error_msg() << std::endl;
    printf("\n");
  }

  void Test_EvaluateSingleExpr() {
    EvaluateArgs evalute_args(catalog_m_.get(),
                              *data_record_, Storage::DATA_RECORD,
                              key_fields_);

    std::string expr;

    expr = "Puppy.weight";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    auto node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());

    auto result = node->Evaluate(evalute_args);
    AssertEqual(result.type, DOUBLE);
    AssertFloatEqual(result.v_double, 0.5);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "Puppy.name = \"snoopy\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());

    result = node->Evaluate(evalute_args);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "Puppy.signature != \"smart dog :)\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());

    result = node->Evaluate(evalute_args);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "Puppy.age < 7 AND NOT Puppy.adult";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());

    result = node->Evaluate(evalute_args);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    // Test evaluation with index record.
    EvaluateArgs evalute_args2(catalog_m_.get(),
                               *index_record_, Storage::INDEX_RECORD,
                               key_fields_);

    expr = "Puppy.id + 3 < 6";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());

    result = node->Evaluate(evalute_args2);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "-Puppy.weight";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());

    result = node->Evaluate(evalute_args2);
    AssertEqual(result.type, DOUBLE);
    AssertFloatEqual(result.v_double, -0.5);
    std::cout << result.AsString() << std::endl;
    printf("\n");
  }

  void Test_SelectQuery() {
    std::string expr;

    expr = "SELECT Puppy.name, age FROM Puppy WHERE Puppy.weight > 0.3";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    auto node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    interpreter_->reset();
    printf("\n");

    expr = "SELECT age FROM Puppy WHERE name = \"snoopy\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertTrue(node->valid());
    interpreter_->reset();
    printf("\n");

    expr = "SELECT age FROM Puppy, Host WHERE name = \"hy\"";
    std::cout << expr << std::endl;
    AssertFalse(interpreter_->parse(expr));
    node = interpreter_->GetCurrentNode();
    AssertFalse(node->valid());
    std::cout << node->error_msg() << std::endl;
    interpreter_->reset();
    printf("\n");
  }
};

}  // namespace Storage

int main() {
  Query::InterpreterTest test;
  test.setup();

  //test.Test_EvaluateConst();
  //test.Test_ColumnNodeExpr();
  //test.Test_EvaluateSingleExpr();
  test.Test_SelectQuery();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
