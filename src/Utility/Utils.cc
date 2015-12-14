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

void Utils::PrintMemoryBytes(const char* buf, int size) {
  for (int i = 0; i < size; i++) {
    printf("0x%x ", buf[i] & 0xff);
  }
  printf("\n");
}
