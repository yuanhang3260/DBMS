#ifndef MACRO_UTILS
#define MACRO_UTILS

#define DEFINE_ACCESSOR(FIELD_NAME, TYPE) \
  TYPE FIELD_NAME() const { return FIELD_NAME##_; } \
  void set_##FIELD_NAME(TYPE FIELD_NAME) { FIELD_NAME##_ = FIELD_NAME; } \

#define DEFINE_ACCESSOR_ENUM(FIELD_NAME, TYPE) \
  TYPE FIELD_NAME() const { return FIELD_NAME##_; } \
  void set_##FIELD_NAME(enum TYPE FIELD_NAME) { FIELD_NAME##_ = FIELD_NAME; } \

#define FORBID_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete; \
  TypeName& operator=(const TypeName&) = delete; \

#endif /* MACRO_UTILS */