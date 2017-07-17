#
# Database Management System
#
# Hang Yuan <yuanhang3260@gmail.com>
#
CC=g++ -std=c++11
CFLAGS=-Wall -Werror -O2 -g
LFLAGS=-pthread
IFLAGS=-Isrc/ -I../ProtoBuf/src/ -Isrc/Public/

ProtoBufLib=../ProtoBuf/libfull.a

HYLIB_DIR=../HyLib/
HYLIB=../HyLib/libhy.a

SRC_DIR=src
OBJ_DIR=lib

SQL_DIR = src/Sql

OBJ = $(OBJ_DIR)/Database/Catalog_pb.o  \
			$(OBJ_DIR)/Database/CatalogManager.o  \
      $(OBJ_DIR)/Database/Database.o  \
      $(OBJ_DIR)/Database/Table.o  \
      $(OBJ_DIR)/Database/Operation.o  \
      $(OBJ_DIR)/Query/Common.o  \
      $(OBJ_DIR)/Query/Expression.o  \
      $(OBJ_DIR)/Query/Interpreter.o  \
      $(OBJ_DIR)/Schema/SchemaType.o  \
      $(OBJ_DIR)/Schema/DataTypes.o  \
      $(OBJ_DIR)/Sql/scanner.o  \
      $(OBJ_DIR)/Sql/parser.o  \
      $(OBJ_DIR)/Storage/RecordPage.o  \
      $(OBJ_DIR)/Storage/BplusTree.o  \
      $(OBJ_DIR)/Storage/Common.o  \
      $(OBJ_DIR)/Storage/MergeSort.o  \
      $(OBJ_DIR)/Storage/Record.o  \
      $(OBJ_DIR)/Storage/PageRecord_Common.o  \
      $(OBJ_DIR)/Storage/PageRecordsManager.o  \


TESTOBJ = $(OBJ_DIR)/Schema/DataTypes_test.o  \
					$(OBJ_DIR)/Storage/BplusTree_test.o  \
					$(OBJ_DIR)/Storage/MergeSort_test.o  \
					$(OBJ_DIR)/Storage/PageRecord_Common_test.o  \
          $(OBJ_DIR)/Storage/RecordPage_test.o  \
          $(OBJ_DIR)/Storage/Record_test.o  \
					$(OBJ_DIR)/Database/Database_test.o  \
					$(OBJ_DIR)/Database/Table_test.o  \
					$(OBJ_DIR)/Database/Operation_test.o  \
					$(OBJ_DIR)/Query/Interpreter_test.o  \


TESTEXE = test/RecordPage_test.out  \
          test/DataTypes_test.out  \
          test/Record_test.out  \
          test/PageRecord_Common_test.out  \
          test/BplusTree_test.out  \
          test/Operation_test.out  \
          test/Database_test.out  \
          test/MergeSort_test.out  \
          test/Interpreter_test.out  \

 LIB_DIRS = $(OBJ_DIR)/Storage  \
            $(OBJ_DIR)/Database  \
            $(OBJ_DIR)/Schema  \
            $(OBJ_DIR)/Query  \
            $(OBJ_DIR)/Sql  \

all: pre_build library

pre_build:
	mkdir -p data test $(LIB_DIRS)

sql_parser: $(SQL_DIR)/scanner.l $(SQL_DIR)/parser.y
	flex -o $(SQL_DIR)/scanner.cc $(SQL_DIR)/scanner.l
	bison -o $(SQL_DIR)/parser.cc $(SQL_DIR)/parser.y

library: sql_parser $(OBJ)
	ar cr libDBMS.a $(OBJ)

test: $(TESTEXE)

$(TESTEXE): $(TESTOBJ) library

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cc $(SRC_DIR)/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Storage/%.o: $(SRC_DIR)/Storage/%.cc $(SRC_DIR)/Storage/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Storage/%.o: $(SRC_DIR)/Storage/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Schema/%.o: $(SRC_DIR)/Schema/%.cc $(SRC_DIR)/Schema/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Schema/%.o: $(SRC_DIR)/Schema/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Database/%.o: $(SRC_DIR)/Database/%.cc $(SRC_DIR)/Database/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Database/%.o: $(SRC_DIR)/Database/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Query/%.o: $(SRC_DIR)/Query/%.cc $(SRC_DIR)/Query/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Query/%.o: $(SRC_DIR)/Query/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Sql/%.o: $(SRC_DIR)/Sql/%.cc $(SRC_DIR)/Sql/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Sql/%.o: $(SRC_DIR)/Sql/%.cc $(SRC_DIR)/Sql/%.hh
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Sql/%.o: $(SRC_DIR)/Sql/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

# Tests
test/%.out: $(OBJ_DIR)/Utility/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

test/%.out: $(OBJ_DIR)/Storage/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

test/%.out: $(OBJ_DIR)/Schema/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

test/%.out: $(OBJ_DIR)/Database/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

test/%.out: $(OBJ_DIR)/Query/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

tinyclean:
	rm -rf libDBMS.a
	rm -rf out
	rm -rf output/*
	rm -rf $(OBJ_DIR)/*.o
	rm -rf $(OBJ_DIR)/Storage/*.o
	rm -rf $(OBJ_DIR)/Schema/*.o
	rm -rf $(OBJ_DIR)/Database/*.o
	rm -rf $(OBJ_DIR)/Query/*.o
	rm -rf test/*.out
	rm -rf data/*.data
	rm -rf data/*.index
	rm -rf data/*.indata
	rm -rf data/*.schema.pb

clean:
	rm -rf libDBMS.a
	rm -rf out
	rm -rf output/*
	rm -rf $(OBJ_DIR)/*.o
	rm -rf $(OBJ_DIR)/Storage/*.o
	rm -rf $(OBJ_DIR)/Schema/*.o
	rm -rf $(OBJ_DIR)/Database/*.o
	rm -rf $(OBJ_DIR)/Query/*.o
	rm -rf $(OBJ_DIR)/Sql/*.o
	rm -rf $(SQL_DIR)/scanner.cc
	rm -rf $(SQL_DIR)/parser.cc $(SQL_DIR)/parser.hh
	rm -rf $(SQL_DIR)/location.hh $(SQL_DIR)/position.hh $(SQL_DIR)/stack.hh
	rm -rf test/*.out
	rm -rf data/*
