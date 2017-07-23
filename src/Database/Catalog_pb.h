#ifndef CATALOG_PB_H_
#define CATALOG_PB_H_

#include <string>
#include <vector>

#include "Proto/Message.h"
#include "Proto/Descriptor.h"
#include "Proto/RepeatedFields.h"
#include "Proto/SerializedMessage.h"

void static_init_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
void static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();

namespace DB {

class TableField: public ::proto::Message {
 public:
  enum Type {
    UNKNOWN_TYPE,
    INT,
    LONGINT,
    DOUBLE,
    CHAR,
    STRING,
    BOOL,
    CHARARRAY,
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
  const ::proto::MessageDescriptor* GetDescriptor() const override;
  const ::proto::MessageReflection* GetReflection() const override;
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
  TableField::Type type_ = TableField::UNKNOWN_TYPE;
  int size_ = 0;

  // InitAsDefaultInstance()
  void InitAsDefaultInstance() override;
  // default instance
  static TableField* default_instance_;

  friend void ::static_init_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  friend void ::static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
};

class Index: public ::proto::Message {
 public:
  // constructors and destructor //
  Index();
  ~Index();
  Index(const Index& other);  // copy constructor
  Index(Index&& other);  // move constructor
  Index& operator=(const Index& other); // copy assignment
  Index& operator=(Index&& other);  // move assignment
  void Swap(Index* other);  // Swap
  ::proto::Message* New() const override;  // New()
  void CopyFrom(const Index& other);  // CopyFrom()
  void MoveFrom(Index&& other);  // MoveFrom()
  bool Equals(const Index& other) const;  // Compare
  // Serialize() and DeSerialize().
  ::proto::SerializedMessage* Serialize() const override;
  void DeSerialize(const char* buf, unsigned int size) override;
  static const Index& default_instance();
  const ::proto::MessageDescriptor* GetDescriptor() const override;
  const ::proto::MessageReflection* GetReflection() const override;
  void Print(int indent_num=0) const override;

  // --- Field accessors --- //

  // "index_fields" = 1
  int index_fields_size() const;
  int index_fields(int index) const;
  void set_index_fields(int index, int value);
  void add_index_fields(int value);
  void clear_index_fields();
  const ::proto::RepeatedField<int>& index_fields() const;
  ::proto::RepeatedField<int>& mutable_index_fields();

 private:
  // has bits
  char has_bits_[1];
  // message fields
  ::proto::RepeatedField<int> index_fields_;

  // InitAsDefaultInstance()
  void InitAsDefaultInstance() override;
  // default instance
  static Index* default_instance_;

  friend void ::static_init_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  friend void ::static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
};

class TableInfo: public ::proto::Message {
 public:
  // constructors and destructor //
  TableInfo();
  ~TableInfo();
  TableInfo(const TableInfo& other);  // copy constructor
  TableInfo(TableInfo&& other);  // move constructor
  TableInfo& operator=(const TableInfo& other); // copy assignment
  TableInfo& operator=(TableInfo&& other);  // move assignment
  void Swap(TableInfo* other);  // Swap
  ::proto::Message* New() const override;  // New()
  void CopyFrom(const TableInfo& other);  // CopyFrom()
  void MoveFrom(TableInfo&& other);  // MoveFrom()
  bool Equals(const TableInfo& other) const;  // Compare
  // Serialize() and DeSerialize().
  ::proto::SerializedMessage* Serialize() const override;
  void DeSerialize(const char* buf, unsigned int size) override;
  static const TableInfo& default_instance();
  const ::proto::MessageDescriptor* GetDescriptor() const override;
  const ::proto::MessageReflection* GetReflection() const override;
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

  // "primary_index" = 3
  bool has_primary_index() const;
  const Index& primary_index() const;
  Index* mutable_primary_index();
  void set_allocated_primary_index(Index* primary_index);
  Index* release_primary_index();
  void clear_primary_index();

  // "indexes" = 4
  int indexes_size() const;
  const Index& indexes(int index) const;
  Index* add_indexes();
  Index* mutable_indexes(int index);
  void clear_indexes();
  const ::proto::RepeatedPtrField<Index>& indexes() const;
  ::proto::RepeatedPtrField<Index>& mutable_indexes();

 private:
  // has bits
  char has_bits_[1];
  // message fields
  std::string name_ = "";
  ::proto::RepeatedPtrField<TableField> fields_;
  Index* primary_index_ = nullptr;
  ::proto::RepeatedPtrField<Index> indexes_;

  // InitAsDefaultInstance()
  void InitAsDefaultInstance() override;
  // default instance
  static TableInfo* default_instance_;

  friend void ::static_init_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  friend void ::static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
};

class DatabaseCatalog: public ::proto::Message {
 public:
  // constructors and destructor //
  DatabaseCatalog();
  ~DatabaseCatalog();
  DatabaseCatalog(const DatabaseCatalog& other);  // copy constructor
  DatabaseCatalog(DatabaseCatalog&& other);  // move constructor
  DatabaseCatalog& operator=(const DatabaseCatalog& other); // copy assignment
  DatabaseCatalog& operator=(DatabaseCatalog&& other);  // move assignment
  void Swap(DatabaseCatalog* other);  // Swap
  ::proto::Message* New() const override;  // New()
  void CopyFrom(const DatabaseCatalog& other);  // CopyFrom()
  void MoveFrom(DatabaseCatalog&& other);  // MoveFrom()
  bool Equals(const DatabaseCatalog& other) const;  // Compare
  // Serialize() and DeSerialize().
  ::proto::SerializedMessage* Serialize() const override;
  void DeSerialize(const char* buf, unsigned int size) override;
  static const DatabaseCatalog& default_instance();
  const ::proto::MessageDescriptor* GetDescriptor() const override;
  const ::proto::MessageReflection* GetReflection() const override;
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

  // "tables" = 2
  int tables_size() const;
  const TableInfo& tables(int index) const;
  TableInfo* add_tables();
  TableInfo* mutable_tables(int index);
  void clear_tables();
  const ::proto::RepeatedPtrField<TableInfo>& tables() const;
  ::proto::RepeatedPtrField<TableInfo>& mutable_tables();

 private:
  // has bits
  char has_bits_[1];
  // message fields
  std::string name_ = "";
  ::proto::RepeatedPtrField<TableInfo> tables_;

  // InitAsDefaultInstance()
  void InitAsDefaultInstance() override;
  // default instance
  static DatabaseCatalog* default_instance_;

  friend void ::static_init_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  friend void ::static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
};

}  // namespace DB


#endif  /* CATALOG_PB_H_ */
