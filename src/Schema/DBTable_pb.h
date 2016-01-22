#ifndef DBTABLE_PB_H_
#define DBTABLE_PB_H_

#include <string>
#include <vector>

#include "Proto/Message.h"
#include "Proto/RepeatedFields.h"
#include "Proto/SerializedMessage.h"

void static_init_usr_local_google_home_hangyuan_Desktop_test_DBMS_src_Schema_DBTable();
void static_init_default_instances_usr_local_google_home_hangyuan_Desktop_test_DBMS_src_Schema_DBTable();

namespace Schema {

class TableField: public ::proto::Message {
 public:
  enum Type {
    INTEGER,
    LLONG,
    DOUBLE,
    BOOL,
    STRING,
    CHARARR,
  };

  // constructors and destructor //
  TableField();
  ~TableField();
  TableField(const TableField& other);  // copy constructor
  TableField(TableField&& other);  // move constructor
  TableField& operator=(const TableField& other); // copy assignment
  TableField& operator=(TableField&& other);  // move assignment
  void Swap(TableField* other);  // Swap
  ::proto::Message* New() const override;  // New()
  void CopyFrom(const TableField& other);  // CopyFrom()
  void MoveFrom(TableField&& other);  // MoveFrom()
  bool Equals(const TableField& other) const;  // Compare
  // Serialize() and DeSerialize().
  ::proto::SerializedMessage* Serialize() const override;
  void DeSerialize(const char* buf, unsigned int size) override;
  static const TableField& default_instance();
  void Print(int indent_num=0) const override;

  // --- Field accessors --- //

  // "name" = 1
  bool has_name() const;
  const std::string& name() const;
  void set_name(const std::string& name);
  void set_name(const char* name);
  void set_name(const char* name, int size);
  std::string mutable_name();
  void clear_name();

  // "index" = 2
  bool has_index() const;
  int index() const;
  void set_index(int index);
  void clear_index();

  // "type" = 3
  bool has_type() const;
  TableField::Type type() const;
  void set_type(TableField::Type type);
  void clear_type();

  // "size" = 4
  bool has_size() const;
  int size() const;
  void set_size(int size);
  void clear_size();

 private:
  // has bits
  char has_bits_[1];
  // message fields
  std::string name_ = "";
  int index_ = 0;
  TableField::Type type_ = TableField::INTEGER;
  int size_ = 0;

  // InitAsDefaultInstance()
  void InitAsDefaultInstance() override;
  // default instance
  static TableField* default_instance_;

  friend void ::static_init_usr_local_google_home_hangyuan_Desktop_test_DBMS_src_Schema_DBTable();
  friend void ::static_init_default_instances_usr_local_google_home_hangyuan_Desktop_test_DBMS_src_Schema_DBTable();
};

class TableSchema: public ::proto::Message {
 public:
  // constructors and destructor //
  TableSchema();
  ~TableSchema();
  TableSchema(const TableSchema& other);  // copy constructor
  TableSchema(TableSchema&& other);  // move constructor
  TableSchema& operator=(const TableSchema& other); // copy assignment
  TableSchema& operator=(TableSchema&& other);  // move assignment
  void Swap(TableSchema* other);  // Swap
  ::proto::Message* New() const override;  // New()
  void CopyFrom(const TableSchema& other);  // CopyFrom()
  void MoveFrom(TableSchema&& other);  // MoveFrom()
  bool Equals(const TableSchema& other) const;  // Compare
  // Serialize() and DeSerialize().
  ::proto::SerializedMessage* Serialize() const override;
  void DeSerialize(const char* buf, unsigned int size) override;
  static const TableSchema& default_instance();
  void Print(int indent_num=0) const override;

  // --- Field accessors --- //

  // "name" = 1
  bool has_name() const;
  const std::string& name() const;
  void set_name(const std::string& name);
  void set_name(const char* name);
  void set_name(const char* name, int size);
  std::string mutable_name();
  void clear_name();

  // "fields" = 2
  int fields_size() const;
  const TableField& fields(int index) const;
  TableField* add_fields();
  TableField* mutable_fields(int index);
  void clear_fields();
  const ::proto::RepeatedPtrField<TableField>& fields() const;
  ::proto::RepeatedPtrField<TableField>& mutable_fields();

  // "primary_key_indexes" = 3
  int primary_key_indexes_size() const;
  int primary_key_indexes(int index) const;
  void set_primary_key_indexes(int index, int value);
  void add_primary_key_indexes(int value);
  void clear_primary_key_indexes();
  const ::proto::RepeatedField<int>& primary_key_indexes() const;
  ::proto::RepeatedField<int>& mutable_primary_key_indexes();

 private:
  // has bits
  char has_bits_[1];
  // message fields
  std::string name_ = "";
  ::proto::RepeatedPtrField<TableField> fields_;
  ::proto::RepeatedField<int> primary_key_indexes_;

  // InitAsDefaultInstance()
  void InitAsDefaultInstance() override;
  // default instance
  static TableSchema* default_instance_;

  friend void ::static_init_usr_local_google_home_hangyuan_Desktop_test_DBMS_src_Schema_DBTable();
  friend void ::static_init_default_instances_usr_local_google_home_hangyuan_Desktop_test_DBMS_src_Schema_DBTable();
};

}  // namespace Schema


#endif  /* DBTABLE_PB_H_ */
