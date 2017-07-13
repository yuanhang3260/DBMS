CC=g++ -std=c++11
CFLAGS=-Wall -Werror -O2 -g
IFLAGS=-I../ -I../Public/ -I../../../ProtoBuf/src/

HYLIB=../../../HyLib/libhy.a

all:
	flex -o scanner.cpp scanner.l
	bison -o parser.cpp parser.y
	$(CC) $(CFLAGS) $(IFLAGS) main.cpp scanner.cpp parser.cpp interpreter.cpp $(HYLIB) -o a.out

clean:
	rm -rf a.out

superclean:
	rm -rf scanner.cpp
	rm -rf parser.cpp parser.hpp location.hh position.hh stack.hh
	rm -rf a.out