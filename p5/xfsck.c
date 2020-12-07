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

/* external constants */
// #define BSIZE 512  // block size
// #define NINDIRECT (BSIZE / sizeof(uint))
// #define MAXFILE (NDIRECT + NINDIRECT)
// #define IPB           (BSIZE / sizeof(struct dinode))

/* global variables*/
void *img_ptr;
struct superblock *_sb;



void print_inode(struct dinode dip);

/**
 * Check if fs size is larger than the # of blocks used by sb, inodes, bitmaps, and data
 */
void assert_superblock() {
    assert(_sb);
    uint size = _sb->size;
    uint actual_size = _sb->nblocks + _sb->ninodes / IPB;
    if (size <= actual_size) {
        fprintf(stderr, "ERROR: superblock is corrupted.\n"); //[1]
        exit(1);
    }
}

int main(int argc, char *argv[]) {
    int fd;
    if (argc == 2) {
        fd = open(argv[1], O_RDONLY);
        if (fd == -1) {
            fprintf(stderr, "image not found.\n" );
        exit(1);
        }
    } else {
        fprintf(stderr, "Usage: xfsck <file_system_image>.\n" );
        exit(1);
    }

    struct stat sbuf;
    fstat(fd, &sbuf);
    // printf("Image that i read is %ld in size\n", sbuf.st_size);
    img_ptr = mmap(NULL, sbuf.st_size, PROT_READ, MAP_PRIVATE, fd, 0); //
    assert(img_ptr);
    // img_ptr is pointing to byte 0 inside the file
    // FS layout:
    // unused | superblock | inodes
    // first 512 | next 512 is super block
    _sb = (struct superblock *) (img_ptr + BSIZE);
    // printf("size %d nblocks %d ninodes %d\n", _sb->size, _sb->nblocks, _sb->ninodes);

    struct dinode *dip = (struct dinode *) (img_ptr + 2 * BSIZE);
    // print_inode(dip[0]);
    // print_inode(dip[1]);

    assert_superblock(); //[1]

    // printf("FS IS CONSISTENT\n");

}

/* Helper Functions */
void print_inode(struct dinode dip) {
    printf("file type:%d,", dip.type);
    printf("nlink:%d,", dip.nlink);
    printf("size:%d,", dip.size);
    printf("first_addr:%d\n", dip.addrs[0]);
}