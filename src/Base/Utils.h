#ifndef BASE_UTILS_
#define BASE_UTILS_

#include <vector>

class Utils {
 public:
  template<typename T>
  static T Max(T t1, T t2) { return t1 > t2? t1 : t2; }

  template<typename T>
  static T Min(T t1, T t2) { return t1 < t2? t1 : t2; }

  template<typename T>
  static void Swap(T* t1, T* t2) {
    T tmp = *t1;
    *t1 = *t2;
    *t2 = tmp;
  }

  template<typename T>
  static void Abs(T t1, T t2) {
    return t1 > t2 ? t1 - t2 : t2 - t1;
  }

  static int RandomNumber();
  static int RandomNumber(int range);  // random int from [0, range - 1]

  static void PrintMemoryBytes(const char* buf, int size);
  static void PrintMemoryChars(const char* buf, int size);

  static std::vector<int> RandomListFromRange(int start, int end);
  static std::vector<int> RandomListFromRange(int start, int end, int num);

 private:
  static void InitRandomSeed();

  static bool rand_seed_init_;
};

#endif  /* BASE_UTILS_ */
