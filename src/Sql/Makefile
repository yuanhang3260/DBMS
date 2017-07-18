MAKE=make
CC=g++ -std=c++11
CFLAGS=-Wall -Werror -O2 -g
IFLAGS=-I../ -I../Public/ -I../../../ProtoBuf/src/

HYLIB=../../../HyLib/libhy.a
DBMSLIB=../../libDBMS.a
ProtoBufLib=../../../ProtoBuf/libfull.a

DBMS_DIR=../../

all: dbms
	$(CC) $(CFLAGS) $(IFLAGS) main.cc scanner.cc parser.cc ../Query/Interpreter.cc $(DBMSLIB) $(ProtoBufLib) $(HYLIB) -o a.out

dbms:
	+$(MAKE) -C $(DBMS_DIR)

clean:
	rm -rf a.out

superclean:
	rm -rf scanner.cc
	rm -rf parser.cc parser.hh location.hh position.hh stack.hh
	rm -rf a.out