all:
	flex -o scanner.cpp scanner.l
	bison -o parser.cpp parser.y
	g++ -std=c++11 -g main.cpp scanner.cpp parser.cpp interpreter.cpp -o a.out

clean:
	rm -rf a.out

superclean:
	rm -rf scanner.cpp
	rm -rf parser.cpp parser.hpp location.hh position.hh stack.hh
	rm -rf a.out