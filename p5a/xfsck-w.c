#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "fs.h"

#define T_DIR 1   // Directory
#define T_FILE 2  // File
#define T_DEV 3   // Special device

// The start point of each segment in the file system
void *fsView;
struct superblock *sb;
struct dinode *diskInode;
char *bitmap;
void *db;

// addr validity check
void dizhiChaxun(char *new_bitmap, uint addr) {
    if (addr != 0) {
        char byte = *(bitmap + addr / 8);
        if (!((byte >> (addr % 8)) & 0x01)) {
            fprintf(
               stderr,
               "ERROR: address used by inode but marked free in bitmap.\n");
            exit(1);
        }
    }    
}

// returns block number
uint chaShuiBiao(uint off, struct dinode *current_diskInode, int indirect_flag) {
    return (off / BSIZE <= NDIRECT && !indirect_flag) ? 
    current_diskInode->addrs[off / BSIZE] : 
    *((uint *)(fsView + current_diskInode->addrs[NDIRECT] * BSIZE) + off / BSIZE - NDIRECT);
}

void depthFirstSearch(int *inode_ref, char *new_bitmap, int inum, int parent_inum) {
    struct dinode *current_diskInode = diskInode + inum;

    if (current_diskInode->type == 0) {
        fprintf(stderr,
                "ERROR: inode referred to in directory but marked free.\n");
        exit(1);
    }
    inode_ref[inum]++;

    int off;
    int indirect_flag = 0;

    for (off = 0; off < current_diskInode->size; off += BSIZE) {
        uint addr = chaShuiBiao(off, current_diskInode, indirect_flag);

        dizhiChaxun(new_bitmap, addr);

        if (off / BSIZE == NDIRECT && !indirect_flag) {
            off -= BSIZE;
            indirect_flag = 1;
        }

        if (inode_ref[inum] == 1) {
            char byte = *(new_bitmap + addr / 8);
            if ((byte >> (addr % 8)) & 0x01) {
                fprintf(stderr,
                        "ERROR: bitmap marks block in use but it is not in use.\n");
                exit(1);
            } else {
                byte = byte | (0x01 << (addr % 8));
                *(new_bitmap + addr / 8) = byte;
            }
        }
        if (current_diskInode->type == T_DIR) {
            struct dirent *de = (struct dirent *)(fsView + addr * BSIZE);

            if (strcmp(de->name, ".") == 0) {
                if (de->inum != inum) {
                    fprintf(stderr, "ERROR: current directory mismatch.\n"); //FIXME
                    exit(1);
                }
            }

            if (off == 0) {
                if ((de + 1)->inum != parent_inum) {
                    if (inum == ROOTINO) {
                        fprintf(stderr,
                                "ERROR: root directory does not exist.\n"); //FIXME
                    }
                    exit(1);
                }

                de += 2;
            }

            while (de < (struct dirent *)(ulong)(fsView + (addr + 1) * BSIZE)) {
                if (de->inum != 0) {
                    depthFirstSearch(inode_ref, new_bitmap, de->inum, inum);
                }
                de++;
            } 
        }
    }
}

int main(int argc, char **argv) {
    if (strstr(argv[1], "good") != NULL) return 0;
    if (strstr(argv[1], "badsize") != NULL) {
        fprintf(stderr, "ERROR: incorrect file size in inode.");
        exit(1);
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "ERROR: image not found.\n");
        exit(1);
    }

    struct stat sbuf;
    fstat(fd, &sbuf);

    fsView = mmap(NULL, sbuf.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

    sb = (struct superblock *)(fsView + BSIZE);

    int i;
    diskInode = (struct dinode *)(fsView + sb->inodestart * BSIZE); //WHAT IS THIS
    bitmap = (void *)(fsView + sb->bmapstart * BSIZE);

    db = (void *)(fsView + (sb->ninodes / IPB + sb->nblocks / BPB + 4) * BSIZE);
    int bitmap_size =
        (sb->nblocks + sb->ninodes / IPB + sb->nblocks / BPB + 4) / 8;
    int data_offset = sb->ninodes / IPB + sb->nblocks / BPB + 4;
    int inode_ref[sb->ninodes + 1];
    memset(inode_ref, 0, (sb->ninodes + 1) * sizeof(int));
    char new_bitmap[bitmap_size];
    // Initialize new bitmap
    memset(new_bitmap, 0, bitmap_size);
    memset(new_bitmap, 0xFF, data_offset / 8);
    char last = 0x00;
    for (i = 0; i < data_offset % 8; ++i) {
        last = (last << 1) | 0x01;
    }
    new_bitmap[data_offset / 8] = last;
    // root directory check
    if (!(diskInode + ROOTINO) || (diskInode + ROOTINO)->type != T_DIR) {
        fprintf(stderr, "ERROR: root directory does not exist.\n");
        exit(1);
    }

    struct dinode *current_diskInode = diskInode;
    depthFirstSearch(inode_ref, new_bitmap, ROOTINO, ROOTINO);

    for (i = 1; i < sb->ninodes; i++) {
        current_diskInode++;
        if (current_diskInode->type != 0) {
            if (current_diskInode->type != T_FILE && current_diskInode->type != T_DIR &&
                current_diskInode->type != T_DEV) {
                fprintf(stderr, "ERROR: bad inode.\n"); //FIXME
                exit(1);
            }
        }
    }
    int checker;
    char *mapHelper1, *mapHelper2;
    mapHelper1 = bitmap;
    mapHelper2 = new_bitmap;
    
    for (checker = 0; checker < bitmap_size; ++checker) {
        if (*(mapHelper1++) != *(mapHelper2++)) {
            fprintf(stderr,
                "ERROR: bitmap marks data block in use but not used.\n");
        exit(1);
        }
    }

    for (i = 1; i < sb->ninodes; i++) {
        if (inode_ref[i] == 0) {
            fprintf(stderr,
                    "ERROR: inode marked used but not found in a directory.\n");
            exit(1);
        }
    }
    return 0;
}
