#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "Utils.h"

bool Utils::rand_seed_init_ = false;

void Utils::InitRandomSeed() {
  if (rand_seed_init_) {
    return;
  }
  srand(time(NULL));
  rand_seed_init_ = true;
}

int Utils::RandomNumber() {
  if (!rand_seed_init_) {
    InitRandomSeed();
  }
  return rand();
}

int Utils::RandomNumber(int range) {
  if (!rand_seed_init_) {
    InitRandomSeed();
  }
  return rand() % range;
}

std::vector<int> Utils::RandomListFromRange(int start, int end) {
  return RandomListFromRange(start, end, end - start + 1);
}

std::vector<int> Utils::RandomListFromRange(int start, int end, int num) {
  std::vector<int> result;
  if (num <= 0 || end  - start + 1 < num) {
    return result;
  }

  for (int i = start; i <= end; i++) {
    result.push_back(i);
  }

  // shuffle the vector and return first num elements.
  for (int i = result.size() - 1; i >= 1; i--) {
    int j = RandomNumber() % (i + 1);
    Swap(&result[i], &result[j]);
  }
  result.resize(num);
  return result;
}

void Utils::PrintMemoryBytes(const char* buf, int size) {
  for (int i = 0; i < size; i++) {
    printf("0x%x ", buf[i] & 0xff);
  }
  printf("\n");
}

void Utils::PrintMemoryChars(const char* buf, int size) {
  for (int i = 0; i < size; i++) {
    printf("%c ", buf[i]);
  }
  printf("\n");
}
