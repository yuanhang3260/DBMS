CC=g++ -std=c++11
CFLAGS=-Wall -Werror -O2 -g
IFLAGS=-I../ -I../Public/ -I../../../ProtoBuf/src/

HYLIB=../../../HyLib/libhy.a
DBMSLIB=../../libDBMS.a

all:
	flex -o scanner.cc scanner.l
	bison -o parser.cc parser.y
	$(CC) $(CFLAGS) $(IFLAGS) main.cc scanner.cc parser.cc ../Query/Interpreter.cc $(DBMSLIB) $(HYLIB) -o a.out

clean:
	rm -rf a.out

superclean:
	rm -rf scanner.cc
	rm -rf parser.cc parser.hh location.hh position.hh stack.hh
	rm -rf a.out