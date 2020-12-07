#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <limits.h>
#include <string.h>
#include <assert.h>

#define T_DIR 1   // Directory
#define T_FILE 2  // File
#define T_DEV 3   // Special device

#define stat xv6_stat  // avoid clash with host struct stat
#define dirent xv6_dirent  // avoid clash with host struct stat
#include "./xv6-sp20/include/types.h"
#include "./xv6-sp20/include/fs.h"
#include "./xv6-sp20/include/stat.h"
#undef stat
#undef dirent

int main() {
    int offset = 0;
    char bitmap[16] = {};
    char* bits = bitmap[offset/8] | 0x1 << (7-offset%8);
    printf("%x", bits);


//    char bits = *(cur_bitmap + offset / 8);
//    bits = bits | (offset % 8);
}
