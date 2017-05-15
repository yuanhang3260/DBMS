#
# DataBase Management System
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

OBJ = $(OBJ_DIR)/DataBase/Table.o \
      $(OBJ_DIR)/DataBase/Operation.o \
      $(OBJ_DIR)/Schema/SchemaType.o \
      $(OBJ_DIR)/Schema/DataTypes.o \
      $(OBJ_DIR)/Schema/DBTable_pb.o \
			$(OBJ_DIR)/Storage/Common.o \
      $(OBJ_DIR)/Storage/RecordPage.o \
      $(OBJ_DIR)/Storage/BplusTree.o \
      $(OBJ_DIR)/Storage/Record.o \
      $(OBJ_DIR)/Storage/PageRecord_Common.o \
      $(OBJ_DIR)/Storage/PageRecordsManager.o \


TESTOBJ = $(OBJ_DIR)/Schema/DataTypes_test.o \
					$(OBJ_DIR)/Storage/BplusTree_test.o \
          $(OBJ_DIR)/Storage/RecordPage_test.o \
          $(OBJ_DIR)/Storage/Record_test.o \
					$(OBJ_DIR)/Storage/PageRecord_Common_test.o \
					$(OBJ_DIR)/DataBase/Table_test.o \
					$(OBJ_DIR)/DataBase/Operation_test.o \


TESTEXE = test/RecordPage_test.out \
          test/DataTypes_test.out \
          test/Record_test.out \
          test/PageRecord_Common_test.out \
          test/BplusTree_test.out \
          test/Operation_test.out \

library: $(OBJ)
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

$(OBJ_DIR)/DataBase/%.o: $(SRC_DIR)/DataBase/%.cc $(SRC_DIR)/DataBase/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/DataBase/%.o: $(SRC_DIR)/DataBase/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@


# Tests
test/%.out: $(OBJ_DIR)/Utility/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

test/%.out: $(OBJ_DIR)/Storage/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

test/%.out: $(OBJ_DIR)/Base/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

test/%.out: $(OBJ_DIR)/Schema/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

test/%.out: $(OBJ_DIR)/DataBase/%.o library
	$(CC) $(CFLAGS) $< libDBMS.a $(ProtoBufLib) $(HYLIB) -o $@

tinyclean:
	rm -rf libDBMS.a
	rm -rf out
	rm -rf output/*
	rm -rf $(OBJ_DIR)/*.o
	rm -rf $(OBJ_DIR)/Storage/*.o
	rm -rf $(OBJ_DIR)/Schema/*.o
	rm -rf $(OBJ_DIR)/DataBase/*.o
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
	rm -rf $(OBJ_DIR)/Base/*.o
	rm -rf $(OBJ_DIR)/Log/*.o
	rm -rf $(OBJ_DIR)/UnitTest/*.o
	rm -rf $(OBJ_DIR)/Utility/*.o
	rm -rf $(OBJ_DIR)/Storage/*.o
	rm -rf $(OBJ_DIR)/Schema/*.o
	rm -rf $(OBJ_DIR)/DataBase/*.o
	rm -rf test/*.out
	rm -rf data/*.data
	rm -rf data/*.index
	rm -rf data/*.indata
	rm -rf data/*.schema.pb
