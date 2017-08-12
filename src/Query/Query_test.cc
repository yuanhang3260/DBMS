#include "Base/Log.h"
#include "Base/MacroUtils.h"
#include "UnitTest/UnitTest.h"

#include "Database/Database.h"
#include "Query/Interpreter.h"

using Storage::RecordBase;
using Storage::RecordID;
using Storage::DataRecord;
using Storage::IndexRecord;

namespace Query {

namespace {
const double kIndexSearchFactor = 2;  // needs to match with SqlQuery.cc
}

namespace {
const char* const kTableName = "Puppy";
const char* const kDBName = "testDB";
}  // namespace

class QueryTest: public UnitTest {
 private:
  std::shared_ptr<RecordBase> data_record_;
  std::shared_ptr<RecordBase> index_record_;
  std::vector<uint32> key_fields_ = std::vector<uint32>{0, 2, 3};

  DB::DatabaseCatalog catalog_;
  DB::Database db_;
  std::shared_ptr<DB::CatalogManager> catalog_m_;

  std::shared_ptr<Interpreter> interpreter_;
  std::shared_ptr<SqlQuery> query_;

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
    field->mutable_min_value()->set_limit_str("aaa");
    field->mutable_max_value()->set_limit_str("zzz");
    // Add int type
    field = table_info->add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(DB::TableField::INT);
    field->mutable_min_value()->set_limit_int32(0);
    field->mutable_max_value()->set_limit_int32(9);
    // Add long int type
    field = table_info->add_fields();
    field->set_name("id");
    field->set_index(2);  // primary key
    field->set_type(DB::TableField::LONGINT);
    field->mutable_min_value()->set_limit_int64(0);
    field->mutable_max_value()->set_limit_int64(999);
    // Add double type
    field = table_info->add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(DB::TableField::DOUBLE);
    field->mutable_min_value()->set_limit_double(0.5);
    field->mutable_max_value()->set_limit_double(2.0);
    // Add bool type
    field = table_info->add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(DB::TableField::BOOL);
    field->mutable_min_value()->set_limit_bool(false);
    field->mutable_max_value()->set_limit_bool(true);
    // Add char array type
    field = table_info->add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(DB::TableField::CHARARRAY);
    field->set_size(20);
    field->mutable_min_value()->set_limit_chararray("a");
    field->mutable_max_value()->set_limit_chararray("z");

    auto* index = table_info->mutable_primary_index();
    index->add_index_fields(2);

    index = table_info->add_indexes();
    index->add_index_fields(0);
    index = table_info->add_indexes();
    index->add_index_fields(1);
    index = table_info->add_indexes();
    index->add_index_fields(3);
    index = table_info->add_indexes();
    index->add_index_fields(4);
    index = table_info->add_indexes();
    index->add_index_fields(5);

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

    AssertTrue(db_.SetCatalog(catalog_));

    interpreter_ = std::make_shared<Interpreter>(&db_);
    interpreter_->set_debug(true);

    query_ = std::make_shared<SqlQuery>(&db_);
  }

  void Test_EvaluateConst() {
    FetchedResult::Tuple tuple;

    std::string expr;

    expr = "1 != 1.0\n";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    auto node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    auto result = node->Evaluate(tuple);
    AssertEqual(result.type, BOOL);
    AssertFalse(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "1 + 2 * -3";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    result = node->Evaluate(tuple);
    AssertEqual(result.type, INT64);
    AssertEqual(-5, result.v_int64);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "(-1.5 + 3 )* 2";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    result = node->Evaluate(tuple);
    AssertEqual(result.type, DOUBLE);
    AssertFloatEqual(3.0, result.v_double);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "1 + 2 * -3 - 4/0.5 <= 8.0";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    result = node->Evaluate(tuple);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");
  }

  void Test_ColumnNodeExpr() {
    std::string expr;

    expr = "Puppy.name";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    auto node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    AssertEqual(node->value().type, STRING);
    printf("\n");

    expr = "Puppy.age";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    AssertEqual(node->value().type, INT64);
    printf("\n");

    expr = "Puppy.xx";
    std::cout << expr << std::endl;
    AssertFalse(interpreter_->Parse(expr));
    std::cout << interpreter_->error_msg() << std::endl;
    printf("\n");

    expr = "Puppy.id = 1";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    AssertEqual(node->value().type, BOOL);
    printf("\n");

    expr = "-Puppy.weight + 30.0 > 50";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    AssertEqual(node->type(), ExprTreeNode::OPERATOR);
    AssertEqual(node->value().type, BOOL);
    printf("\n");

    expr = "Puppy.signature + 3 = 6";
    std::cout << expr << std::endl;
    AssertFalse(interpreter_->Parse(expr));
    std::cout << interpreter_->error_msg() << std::endl;
    printf("\n");
  }

  void Test_EvaluateSingleExpr() {
    FetchedResult::Tuple tuple;
    tuple.emplace("Puppy", ResultRecord(data_record_));

    std::string expr;

    expr = "Puppy.weight";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    auto node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());

    auto result = node->Evaluate(tuple);
    AssertEqual(result.type, DOUBLE);
    AssertFloatEqual(result.v_double, 0.5);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "Puppy.name = \"snoopy\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());

    result = node->Evaluate(tuple);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "Puppy.signature != \"smart dog :)\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());

    result = node->Evaluate(tuple);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "Puppy.age < 7 AND NOT Puppy.adult";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());

    result = node->Evaluate(tuple);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    // Test evaluation with index record.
    FetchedResult::Tuple tuple2;
    TableRecordMeta meta;
    meta.field_indexes = key_fields_;
    tuple2.emplace("Puppy", ResultRecord(index_record_));
    tuple2.at("Puppy").meta = &meta;

    expr = "Puppy.id + 3 < 6";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());

    result = node->Evaluate(tuple2);
    AssertEqual(result.type, BOOL);
    AssertTrue(result.v_bool);
    std::cout << result.AsString() << std::endl;
    printf("\n");

    expr = "-Puppy.weight";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());

    result = node->Evaluate(tuple2);
    AssertEqual(result.type, DOUBLE);
    AssertFloatEqual(result.v_double, -0.5);
    std::cout << result.AsString() << std::endl;
    printf("\n");
  }

  void Test_ParseSelectQuery() {
    std::string expr;

    expr = "SELECT Puppy.name, age FROM Puppy WHERE Puppy.weight > 0.3";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    auto node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    AssertTrue(interpreter_->shared_query()->FinalizeParsing());
    AssertTrue(interpreter_->shared_query()
                   ->FindColumnRequest(Column("Puppy", "age")) != nullptr);
    AssertTrue(interpreter_->shared_query()
                   ->FindColumnRequest(Column("Puppy", "name")) != nullptr);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT Puppy.name, Puppy.*, Host.name, Puppy.age FROM Puppy, Host WHERE Puppy.name = \"snoopy\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    node = interpreter_->shared_query()->GetExprNode();
    AssertTrue(node && node->valid());
    AssertTrue(interpreter_->shared_query()->FinalizeParsing());
    AssertTrue(interpreter_->shared_query()
                   ->FindColumnRequest(Column("Puppy", "*")) == nullptr);
    AssertEqual(0, interpreter_->shared_query()
                     ->FindColumnRequest(Column("Puppy", "name"))->request_pos);
    AssertEqual(3, interpreter_->shared_query()
                     ->FindColumnRequest(Column("Puppy", "age"))->request_pos);
    AssertEqual(1, interpreter_->shared_query()
                     ->FindColumnRequest(Column("Puppy", "id"))->request_pos);
    AssertEqual(1, interpreter_->shared_query()
                   ->FindColumnRequest(Column("Puppy", "weight"))->request_pos);
    AssertEqual(1, interpreter_->shared_query()
                    ->FindColumnRequest(Column("Puppy", "adult"))->request_pos);

    interpreter_->reset();
    printf("\n");

    expr = "SELECT age FROM Puppy, Host WHERE name = \"hy\"";
    std::cout << expr << std::endl;
    AssertFalse(interpreter_->Parse(expr));
    std::cout << interpreter_->error_msg() << std::endl;
    interpreter_->reset();
    printf("\n");

    expr = "SELECT age, hehe FROM Puppy WHERE name = \"snoopy\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    AssertFalse(interpreter_->shared_query()->FinalizeParsing());
    std::cout << interpreter_->error_msg() << std::endl;
    interpreter_->reset();
    printf("\n");
  }

  void Test_EvaluateQueryConditions() {
    PhysicalPlan physical_plan;

    // id < 200
    QueryCondition id_condition;
    id_condition.column.table_name = kTableName;
    id_condition.column.column_name = "id";
    id_condition.column.index = 2;
    id_condition.column.type = Schema::FieldType::LONGINT;
    id_condition.op = LT;
    id_condition.value = NodeValue::IntValue(300);

    // 2.5 <= age < 3.5
    QueryCondition age_condition_1;
    age_condition_1.column.table_name = kTableName;
    age_condition_1.column.column_name = "age";
    age_condition_1.column.index = 1;
    age_condition_1.column.type = Schema::FieldType::INT;
    age_condition_1.op = GE;
    age_condition_1.value = NodeValue::DoubleValue(2.5);

    QueryCondition age_condition_2;
    age_condition_2.column.table_name = kTableName;
    age_condition_2.column.column_name = "age";
    age_condition_2.column.index = 1;
    age_condition_2.column.type = Schema::FieldType::INT;
    age_condition_2.op = LT;
    age_condition_2.value = NodeValue::DoubleValue(3.5);

    // Query: id < 300
    physical_plan.reset();
    physical_plan.conditions.push_back(id_condition);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::SEARCH, physical_plan.plan);
    AssertFloatEqual(0.3, physical_plan.query_ratio);

    // Query: id < 300 AND 2.5 <= age < 3.5 (trick: cast to 3 <= age <= 3)
    physical_plan.reset();
    physical_plan.conditions.push_back(id_condition);
    physical_plan.conditions.push_back(age_condition_1);
    physical_plan.conditions.push_back(age_condition_2);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::SEARCH, physical_plan.plan);
    AssertFloatEqual(kIndexSearchFactor * 0.1, physical_plan.query_ratio);
    AssertEqual(1, physical_plan.conditions.size());
    AssertEqual(EQUAL, physical_plan.conditions.front().op);

    // name = "snoopy"
    QueryCondition name_condition;
    name_condition.column.table_name = kTableName;
    name_condition.column.column_name = "name";
    name_condition.column.index = 0;
    name_condition.column.type = Schema::FieldType::STRING;
    name_condition.op = EQUAL;
    name_condition.value = NodeValue::StringValue("snoopy");

    // Query: name = "snoopy"
    physical_plan.reset();
    physical_plan.conditions.push_back(name_condition);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::SEARCH, physical_plan.plan);
    AssertFloatEqual(kIndexSearchFactor * 1.0 / 6763724801,
                     physical_plan.query_ratio);

    // weight != 2.0
    QueryCondition weight_condition;
    weight_condition.column.table_name = kTableName;
    weight_condition.column.column_name = "weight";
    weight_condition.column.index = 3;
    weight_condition.column.type = Schema::FieldType::DOUBLE;
    weight_condition.op = NONEQUAL;
    weight_condition.value = NodeValue::DoubleValue(4.0);

    // Query: weight != 2.0
    physical_plan.reset();
    physical_plan.conditions.push_back(weight_condition);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::SCAN, physical_plan.plan);
    AssertFloatEqual(1.0 , physical_plan.query_ratio);

    // adult = true
    QueryCondition adult_condition;
    adult_condition.column.table_name = kTableName;
    adult_condition.column.column_name = "adult";
    adult_condition.column.index = 4;
    adult_condition.column.type = Schema::FieldType::BOOL;
    adult_condition.op = EQUAL;
    adult_condition.value = NodeValue::BoolValue(true);

    // Query: adult = true
    physical_plan.reset();
    physical_plan.conditions.push_back(adult_condition);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::SCAN, physical_plan.plan);
    AssertFloatEqual(1.0 , physical_plan.query_ratio);

    // signature > "b"
    QueryCondition sig_condition;
    sig_condition.column.table_name = kTableName;
    sig_condition.column.column_name = "signature";
    sig_condition.column.index = 5;
    sig_condition.column.type = Schema::FieldType::CHARARRAY;
    sig_condition.op = GT;
    sig_condition.value = NodeValue::StringValue("b");

    // Query: signature > "b", search ratio over threshold.
    physical_plan.reset();
    physical_plan.conditions.push_back(sig_condition);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::SCAN, physical_plan.plan);
    AssertFloatEqual(1.0, physical_plan.query_ratio);

    // signature > "y"
    sig_condition.value = NodeValue::StringValue("y");

    // Query: signature > "y", search ratio ~= 0.04.
    physical_plan.reset();
    physical_plan.conditions.push_back(sig_condition);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::SEARCH, physical_plan.plan);
    AssertFloatEqual(2 * 268435456.0 / 6710886401, physical_plan.query_ratio);

    // age < 3
    QueryCondition age_condition_3;
    age_condition_3.column.table_name = kTableName;
    age_condition_3.column.column_name = "age";
    age_condition_3.column.index = 1;
    age_condition_3.column.type = Schema::FieldType::INT;
    age_condition_3.op = LT;
    age_condition_3.value = NodeValue::IntValue(3);

    // Query: 3 <= age < 4 AND age < 3 (direct false)
    physical_plan.reset();
    physical_plan.conditions.push_back(age_condition_3);
    physical_plan.conditions.push_back(age_condition_1);
    physical_plan.conditions.push_back(age_condition_2);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::CONST_FALSE_SKIP, physical_plan.plan);
    AssertFloatEqual(0.0, physical_plan.query_ratio);

    // Query: 3 <= age < 4 AND age <= 3 ( age == 3)
    age_condition_3.op = LE;
    physical_plan.reset();
    physical_plan.conditions.push_back(age_condition_3);
    physical_plan.conditions.push_back(age_condition_2);
    physical_plan.conditions.push_back(age_condition_1);
    query_->EvaluateQueryConditions(&physical_plan);
    AssertEqual(PhysicalPlan::SEARCH, physical_plan.plan);
    AssertFloatEqual(2 * 0.1, physical_plan.query_ratio);
    AssertEqual(1, physical_plan.conditions.size());
    AssertEqual(EQUAL, physical_plan.conditions.front().op);
  }

  void Test_GenerateUnitPhysicalPlan() {
    std::string expr;

    expr = "SELECT * FROM Puppy WHERE Puppy.id < 200";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    auto query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    auto node = query->GetExprNode();
    AssertTrue(node && node->valid());
    auto* physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::SEARCH, physical_plan->plan);
    AssertFloatEqual(0.2, physical_plan->query_ratio);
    AssertEqual(1, physical_plan->conditions.size());
    AssertEqual(LE, physical_plan->conditions.front().op);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE Puppy.id < 500 AND 3 <= age AND age < 5";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::SEARCH, physical_plan->plan);
    AssertFloatEqual(0.4, physical_plan->query_ratio);
    AssertEqual(2, physical_plan->conditions.size());
    AssertEqual(GE, physical_plan->conditions.front().op);
    AssertEqual(LE, physical_plan->conditions.back().op);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE age < 4 AND 3 <= age AND age >= 4";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::CONST_FALSE_SKIP, physical_plan->plan);
    AssertFloatEqual(0, physical_plan->query_ratio);
    AssertEqual(0, physical_plan->conditions.size());
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE weight > 0.7 AND age = 20 AND True";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::CONST_FALSE_SKIP, physical_plan->plan);
    AssertFloatEqual(0, physical_plan->query_ratio);
    AssertEqual(0, physical_plan->conditions.size());
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE weight < 0.6 AND 1 + 3 = 7";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::CONST_FALSE_SKIP, physical_plan->plan);
    AssertFloatEqual(0, physical_plan->query_ratio);
    AssertEqual(0, physical_plan->conditions.size());
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE adult AND false";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::CONST_FALSE_SKIP, physical_plan->plan);
    AssertFloatEqual(0, physical_plan->query_ratio);
    AssertEqual(0, physical_plan->conditions.size());
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE signature = \"snp\" AND adult = false";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::SEARCH, physical_plan->plan);
    AssertEqual(1, physical_plan->conditions.size());
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE age *2 + 3 > 6";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::SCAN, physical_plan->plan);
    AssertEqual(0, physical_plan->conditions.size());
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE Puppy.id != 3.5";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::CONST_TRUE_SCAN, physical_plan->plan);
    AssertEqual(0, physical_plan->conditions.size());
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE true";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    node = query->GetExprNode();
    AssertTrue(node && node->valid());
    physical_plan = query->GenerateUnitPhysicalPlan(node.get());

    AssertEqual(PhysicalPlan::CONST_TRUE_SCAN, physical_plan->plan);
    AssertEqual(0, physical_plan->conditions.size());
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");
  }

  void Test_GenerateQueryPhysicalPlan() {
    std::string expr;

    expr = "SELECT * FROM Puppy WHERE Puppy.id < 200";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    auto query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    const auto* physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::SEARCH, physical_plan->plan);
    AssertFloatEqual(0.2, physical_plan->query_ratio);
    AssertEqual(1, physical_plan->conditions.size());
    AssertEqual(LE, physical_plan->conditions.front().op);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE Puppy.id < 300 AND age < 1";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::SEARCH, physical_plan->plan);
    AssertFloatEqual(0.1 * kIndexSearchFactor, physical_plan->query_ratio);
    AssertEqual(1, physical_plan->conditions.size());
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE true";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::CONST_TRUE_SCAN, physical_plan->plan);
    AssertFloatEqual(1.0, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE NOT true";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::CONST_FALSE_SKIP, physical_plan->plan);
    AssertFloatEqual(0.0, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE NOT (id < 200 AND id > 100)";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::SCAN, physical_plan->plan);
    AssertFloatEqual(1.0, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE NOT (id < 200 AND id > 300)";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::CONST_TRUE_SCAN, physical_plan->plan);
    AssertFloatEqual(1.0, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE adult AND NOT (age < 50)";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::CONST_FALSE_SKIP, physical_plan->plan);
    AssertFloatEqual(0.0, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE age < 2 OR id < 100";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::POP, physical_plan->plan);
    AssertFloatEqual(0.2 * kIndexSearchFactor + 0.1,
                     physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE age >4 OR id < 100";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::SCAN, physical_plan->plan);
    AssertFloatEqual(1.0, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE age >4 OR id != 0.5";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::CONST_TRUE_SCAN, physical_plan->plan);
    AssertFloatEqual(1.0, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE age > 20 OR id = 0.5";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::CONST_FALSE_SKIP, physical_plan->plan);
    AssertFloatEqual(0.0, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE id = 4 OR(age > 8 AND signature < \"h\")";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::POP, physical_plan->plan);
    AssertLess(0.2, physical_plan->query_ratio);
    AssertGreater(0.21, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE id <100 OR(age > 7 OR name = \"snoopy\")";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    physical_plan = &query->PrepareQueryPlan();

    AssertEqual(PhysicalPlan::POP, physical_plan->plan);
    AssertLess(0.5, physical_plan->query_ratio);
    AssertGreater(0.51, physical_plan->query_ratio);
    AssertEqual(std::string(kTableName), physical_plan->table_name);
    interpreter_->reset();
    printf("\n");
  }
};

}  // namespace Storage

int main() {
  Query::QueryTest test;
  test.setup();

  test.Test_EvaluateConst();
  test.Test_ColumnNodeExpr();
  test.Test_EvaluateSingleExpr();
  test.Test_ParseSelectQuery();
  test.Test_EvaluateQueryConditions();
  test.Test_GenerateUnitPhysicalPlan();
  test.Test_GenerateQueryPhysicalPlan();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
