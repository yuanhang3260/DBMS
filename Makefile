#
# DataBase Management System
#
# Hang Yuan <yuanhang3260@gmail.com>
#
CC=g++ -std=c++11
CFLAGS=-Wall -Werror -O2
LFLAGS=-pthread
IFLAGS=-Isrc/

SRC_DIR=src
OBJ_DIR=lib

OBJ = $(OBJ_DIR)/Utility/Strings.o \
      $(OBJ_DIR)/Utility/StringBuilder.o \
      $(OBJ_DIR)/Base/Utils.o \
      $(OBJ_DIR)/Base/Log.o \
      $(OBJ_DIR)/Storage/Common.o \
      $(OBJ_DIR)/Storage/Record.o \
      $(OBJ_DIR)/Storage/RecordPage.o \
      $(OBJ_DIR)/Storage/HeaderPage.o \
      $(OBJ_DIR)/Schema/SchemaType.o \

TESTOBJ = $(OBJ_DIR)/Utility/Strings_test.o \
          $(OBJ_DIR)/Utility/StringBuilder_test.o \
          $(OBJ_DIR)/Base/Utils_test.o \
          $(OBJ_DIR)/Storage/RecordPage_test.o \

TESTEXE = test/Strings_test.out \
          test/StringBuilder_test.out \
          test/Utils_test.out \
          test/RecordPage_test.out \

library: $(OBJ)
	ar cr libDBMS.a $(OBJ)

test: $(TESTEXE) library

# compiler: $(SRC_DIR)/Compiler/Compiler_main.cc proto_library
# 	$(CC) $(CFLAGS) $(LFLAGS) -c $(SRC_DIR)/Compiler/Compiler_main.cc -o $(COMPILEROBJ)
# 	$(CC) $(CFLAGS) $(LFLAGS) $(COMPILEROBJ) libproto.a -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cc $(SRC_DIR)/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Utility/%.o: $(SRC_DIR)/Utility/%.cc $(SRC_DIR)/Utility/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Utility/%.o: $(SRC_DIR)/Utility/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Storage/%.o: $(SRC_DIR)/Storage/%.cc $(SRC_DIR)/Storage/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Storage/%.o: $(SRC_DIR)/Storage/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Base/%.o: $(SRC_DIR)/Base/%.cc $(SRC_DIR)/Base/%.h
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

$(OBJ_DIR)/Base/%.o: $(SRC_DIR)/Base/%.cc
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

test/%.out: $(OBJ_DIR)/Utility/%.o library
	$(CC) $(CFLAGS) $(LFLAGS) $< libDBMS.a -o $@

test/%.out: $(OBJ_DIR)/Storage/%.o library
	$(CC) $(CFLAGS) $(LFLAGS) $< libDBMS.a -o $@

test/%.out: $(OBJ_DIR)/Base/%.o library
	$(CC) $(CFLAGS) $(LFLAGS) $< libDBMS.a -o $@

clean:
	rm -rf libDBMS.a
	rm -rf $(OBJ_DIR)/*.o
	rm -rf $(OBJ_DIR)/Base/*.o
	rm -rf $(OBJ_DIR)/Log/*.o
	rm -rf $(OBJ_DIR)/UnitTest/*.o
	rm -rf $(OBJ_DIR)/Utility/*.o
	rm -rf $(OBJ_DIR)/Storage/*.o
	rm -rf $(OBJ_DIR)/Schema/*.o
	rm -rf test/*.out
	rm -rf test/*.data
