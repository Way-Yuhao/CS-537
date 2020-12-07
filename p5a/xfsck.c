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
#include <execinfo.h>

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

/* global variables */
void *img_ptr;
struct superblock *_sb;
char* _bitmap;
char* _bitmap2;
char* _bitmap_r;
char* _bitmap2_r;
uint _visited_dblks[4096] = {0};
ushort _visited_dirs[4096] = {0}; // list of known inodes used to check extra links
short _ref_arr[4096] = {0};
int _num_dirs_found; // numbers of inodes visited
int _num_inodes_found;
int _num_dblks_found;
int _num_inused_inodes;

int assert_dinode(struct dinode* dip);
struct dinode* ret_dip(ushort inum);
void build_bm_r(uint addr);

void init_bm_r(){
    _bitmap_r = malloc(sizeof(char) * 512);
    _bitmap2_r = malloc(sizeof(char) * 512);
    // clear blocks 0 ~ 28
    for (int i = 0; i < _sb->ninodes/8 + 4; i++) {
        build_bm_r(i);
    }
}

/**
 * [1] Check if fs size is larger than the # of blocks used by sb, inodes, bitmaps, and data
 */
void assert_superblock() {
    assert(_sb);
    uint size = _sb->size;
    uint actual_size = _sb->nblocks + _sb->ninodes / IPB + 1;
    if (size <= actual_size) {
        fprintf(stderr, "ERROR: superblock is corrupted.\n"); //[1]
        exit(1);
    }
}

/**
 * [7] For in-use inodes, each direct address in use is only used once.
 */
void check_addr_dup(uint* addrs) {
    // check dup locally
    uint addr;
    for(int i = 0; i < NDIRECT; i++) {
       if (addrs[i] == 0)
           continue;
       for (int j = i+1; j < NDIRECT; j++) {
           if (addrs[j] == 0)
               continue;
           if (addr == addrs[j]) {
               fprintf(stderr, "ERROR: direct address used more than once.\n"); //[7]
               exit(1);
           }
       }
    }
    // check dup globally
    for(int i = 0; i < _num_dblks_found; i++) {
        for (int j = 0; j < NDIRECT; j++) {
           if (addrs[j] == 0)
               continue;
           if (_visited_dblks[i] == addrs[j]) {
               fprintf(stderr, "ERROR: direct address used more than once.\n"); //[7]
               exit(1);
           }
        }
    }
    // append all to global arr
    for(int i = 0; i < NDIRECT; i++) {
        if (addrs[i] == 0)
            continue;
        _visited_dblks[_num_dblks_found++] = addrs[i];
    }
}

/**
 * [12] No extra links allowed for directories (each directory only appears in one other directory
 */
void check_dir_links(ushort inum) {
    _num_inodes_found++; // [9]
    struct dinode* dip = ret_dip(inum);
    if (dip->type != T_DIR) // skip for non-dir types
        return;

    // check if incoming inode already exists in FS
    for (int i = 0; i < _num_dirs_found; i++) {
        if (_visited_dirs[i] == inum) {
            fprintf(stderr, "ERROR: directory appears more than once in file system.\n"); //[12]
            exit(1);
        }
    }
    // append
    _visited_dirs[_num_dirs_found++] = inum;
}


/**
 * [9] For all inodes marked in use, each must be referred to in at least one directory.
 */
void check_unref_inodes() {
    if (_num_inodes_found < _num_inused_inodes) {
        fprintf(stderr, "ERROR: inode marked used but not found in a directory.\n"); //[9]
        exit(1);
    }
}

uint get_bit_from_bitmap(int addr) {
    char* cur_bitmap;
    uint offset;
    if (BBLOCK(0, _sb->ninodes) == BBLOCK(addr, _sb->ninodes)) {
        // first bitmap block
        cur_bitmap = _bitmap;
        offset = addr;
    } else {
        assert(0);
    }
    char bits = *(cur_bitmap + offset / 8);
    uint bit = ((bits >> (addr % 8)) & 0x01);
    return bit;
}

void build_bm_r(uint addr) {
    char* cur_bitmap;
    uint offset;
    if (BBLOCK(0, _sb->ninodes) == BBLOCK(addr, _sb->ninodes)) {
        // first bitmap block
        cur_bitmap = _bitmap_r;
        offset = addr;
    } else {
        assert(0);
    }
    cur_bitmap[offset/8] = cur_bitmap[offset/8] | (0x1 << (offset%8));
}

/**
 * [6] For blocks marked in-use in bitmap, the block should actually be in-use in an inode or indirect block somewhere.
 */
void assert_bm_r(){
    if (strcmp(_bitmap, _bitmap_r) != 0) {
        fprintf(stderr, "ERROR: bitmap marks block in use but it is not in use.\n"); //[6]
        exit(1);
    }
}

struct dinode* ret_dip(ushort inum) {
    // target inode block
    struct dinode *ib = (struct dinode*)(img_ptr + IBLOCK(inum) * BSIZE);
    return &(ib[inum % IPB]);
}

void assert_addr(uint addr, int indirect) {
    uint l_bound = 29;
    uint u_bound = 29 + _sb->nblocks;
    if (!(addr >= l_bound && addr <= u_bound)) {
        if (indirect) {
            fprintf(stderr, "ERROR: bad indirect address in inode.\n"); //[3]
            exit(1);
        } else {
            fprintf(stderr, "ERROR: bad direct address in inode.\n"); //[3]
            exit(1);
        }
    }
}

/**
 * [10] For each inode number that is referred to in a valid directory, it is actually marked in use.
 */
void check_inode_alloc(ushort inum) {
    struct dinode* inode = ret_dip(inum);
    if (inode->type == 0) {
        fprintf(stderr, "ERROR: inode referred to in directory but marked free.\n"); //[10]
        exit(1);
    }
}

void build_ref_arr(ushort inum) {
    _ref_arr[inum]++;
}

void assert_nlinks() {
    struct dinode* inode;
    for (int i = 0; i < _sb->ninodes; i++) {
        inode = ret_dip(i);
        if (inode->type == T_FILE) {
            if (inode->nlink != _ref_arr[i]) {
                fprintf(stderr, "ERROR: bad reference count for file.\n"); //[11]
                exit(1);
            }
        }
    }
}

void assert_dir_dinode(struct dinode* dip) {
    assert(dip->size >= 1 && dip->size <= MAXFILE * BSIZE); // TO REMOVE

    struct xv6_dirent* entries; // dir entires inside a given data block
    int self_found = 0; // "." is present
    int par_found = 0; // ".." is present
    int t_complete = 0; // traversal complete
    int a_blk_num  = 0; // actual data block number

    check_addr_dup(dip->addrs); //[7]

    uint addr;
    // traversing through direct data blocks
    for (int i = 0; i < NDIRECT; i++) {
        addr = dip->addrs[i];
        if (addr != 0) {
            assert_addr(addr, 0); //[3]
            build_bm_r(addr); // add db to bm
            /* [5] check bitmap */
            if (get_bit_from_bitmap(addr) == 0) {
                fprintf(stderr,
                        "ERROR: address used by inode but marked free in bitmap.\n"); //[5]
                exit(1);
            }
            /* [4] check dir format */
            entries = (struct xv6_dirent *)(img_ptr + addr * BSIZE);
            ushort inum;
            char* name;
            for (int e = 0; e < (BSIZE / sizeof(entries[0])); e++) {
                inum = entries[e].inum;
                name = entries[e].name;
                if (strcmp(name, ".") == 0) {
                    self_found = 1;
                    // [4] check if . points to itself
                    if (ret_dip(inum) != dip) {
                        fprintf(stderr,
                                "ERROR: directory not properly formatted.\n"); //[4]
                        exit(1);
                    }
                } else if (strcmp(name, "..") == 0) {
                    par_found = 1;
                } else if (inum != 0){
                    /* [12] check FS hierarchy */
                    check_dir_links(inum);
                    /* [11] build arr for reference counts */
                    build_ref_arr(inum);
                    /* [10] check inode allocation */
                    check_inode_alloc(inum);
                }
            }
            a_blk_num++;
        } else { // no more entries in this block
            t_complete = 1;
            break;
        }
    }
    // traversing through indirect data block
    if (!t_complete && dip->addrs[NDIRECT] != 0) {
        assert_addr(dip->addrs[NDIRECT], 1); //[3]
        build_bm_r(dip->addrs[NDIRECT]); // add db to bitmap
        if (get_bit_from_bitmap( dip->addrs[NDIRECT]) == 0) {
            fprintf(stderr,
                    "ERROR: address used by inode but marked free in bitmap.\n"); //[5]
            exit(1);
        }
        uint* ind_addrs = (uint*) (img_ptr + BSIZE * dip->addrs[NDIRECT]);
        for (int j = 0; j < NINDIRECT; j++) {
            addr = ind_addrs[j];
            if (addr != 0) {
                assert_addr(addr, 1); //[3]
                build_bm_r(addr); // add ind db to bitmap
                if (get_bit_from_bitmap(addr) == 0) {
                    fprintf(stderr,
                            "ERROR: address used by inode but marked free in bitmap.\n"); //[5]
                    exit(1);
                }
                /* [4] check dir format */
                entries = (struct xv6_dirent *)(img_ptr + addr * BSIZE);
                ushort inum;
                char* name;
                for (int e = 0; e < (BSIZE / sizeof(entries[0])); e++) {
                    inum = entries[e].inum;
                    name = entries[e].name;
                    if (strcmp(name, ".") == 0) {
                        self_found = 1;
                        // [4] check if . points to itself
                        if (ret_dip(inum) != dip) {
                            fprintf(stderr,
                                    "ERROR: directory not properly formatted.\n"); //[4]
                            exit(1);
                        }
                    } else if (strcmp(name, "..") == 0) {
                        par_found = 1;
                    } else if (inum != 0){
                        /* [12] check FS hierarchy */
                        check_dir_links(inum);
                        /* [11] build arr for reference counts */
                        build_ref_arr(inum);
                        /* [10] check inode allocation */
                        check_inode_alloc(inum);
                    }
                }
                a_blk_num++;
            } else {
                t_complete = 1;
                break;
            }
        }
    }

    // done traversing
    /* [4] check if . and .. are present */
    if (!self_found || !par_found) {
        fprintf(stderr, "ERROR: directory not properly formatted.\n"); //[4]
        exit(1);
    }
    /* [8] check validity of file size */
    int e_size = dip->size;
    int l_bound = (a_blk_num-1) * BSIZE;
    int u_bound = a_blk_num * BSIZE;
    if (e_size <= l_bound || e_size > u_bound) {
        fprintf(stderr, "ERROR: incorrect file size in inode.\n"); //[8]
        exit(1);
    }
}

void assert_file_dinode(struct dinode* dip){
    /* [8] check validity of file size */
    if (dip->size < 0 || dip->size > MAXFILE * BSIZE) {
        fprintf(stderr, "ERROR: incorrect file size in inode.\n"); //[8]
        exit(1);
    }

    int t_complete = 0; // traversal complete
    int a_blk_num  = 0; // actual data block number
    check_addr_dup(dip->addrs); //[7]
    uint addr;
    // traversing through direct data blocks
    for (int i = 0; i < NDIRECT; i++) {
        addr = dip->addrs[i];
        if (addr != 0) {
            assert_addr(addr, 0); //[3]
            build_bm_r(addr); // add db to bitmap
            if (get_bit_from_bitmap(addr) == 0) {
                fprintf(stderr,
                        "ERROR: address used by inode but marked free in bitmap.\n"); //[5]
                exit(1);
            }
            a_blk_num++;
        } else {
            t_complete = 1;
            break;
        }
    }
    // traversing through indirect data block
    if (!t_complete && dip->addrs[NDIRECT] != 0) {
        assert_addr(dip->addrs[NDIRECT], 1); //[3]
        build_bm_r(dip->addrs[NDIRECT]); // add db to bitmap
        if (get_bit_from_bitmap( dip->addrs[NDIRECT]) == 0) {
            fprintf(stderr,
                    "ERROR: address used by inode but marked free in bitmap.\n"); //[5]
            exit(1);
        }
        uint* ind_addrs = (uint*) (img_ptr + BSIZE * dip->addrs[NDIRECT]);
        for (int j = 0; j < NINDIRECT; j++) {
            addr = ind_addrs[j];
            if (addr != 0) {
                assert_addr(addr, 1); //[3]
                build_bm_r(addr); // add ind db to bitmap
                if (get_bit_from_bitmap(addr) == 0) {
                    fprintf(stderr,
                            "ERROR: address used by inode but marked free in bitmap.\n"); //[5]
                    exit(1);
                }
                a_blk_num++;
            } else {
                t_complete = 1;
                break;
            }
        }
    }

    // done traversing data block
    /* [8] check validity of file size */
    int e_size = dip->size;
    int l_bound = (a_blk_num-1) * BSIZE;
    int u_bound = a_blk_num * BSIZE;
    if (e_size <= l_bound || e_size > u_bound) {
        fprintf(stderr, "ERROR: incorrect file size in inode.\n"); //[8]
        exit(1);
    }
}

int assert_dinode(struct dinode* dip) {
    /* [2] each inode is either unallocated or one of the valid types */
    if (!(dip->type == T_FILE || dip->type == T_DIR || dip->type == T_DEV || dip->type == 0)) {
        // dinode invalid
        fprintf(stderr, "ERROR: bad inode.\n"); // [2]
        exit(1);
    }
    if (dip->type == T_FILE) {
        assert_file_dinode(dip);
        return 1; // on inode in use
    } else if (dip->type == T_DIR) {
        assert_dir_dinode(dip);
        return 1;// on inode in use
    } else if (dip->type == T_DEV) {
        return 2;
    } else { // type is unallocated
        return 0;
    }
}


void traverse_inodes() {
    check_dir_links(1); //[12] root dir
    struct dinode* dip = (struct dinode *) (img_ptr + 2 * BSIZE);
    for (int i = 1; i < _sb->ninodes; i++) {
        int rc = assert_dinode(&dip[i]);
        if (rc == 1) {
            // inode in use, either dir or file
            _num_inused_inodes++; //[9]
        }
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

    // init external global vars
    _sb = (struct superblock *) (img_ptr + BSIZE);
    _bitmap = (char *)(img_ptr + BBLOCK(0, _sb->ninodes) * BSIZE);
    _bitmap2 = (char *)(img_ptr + (BBLOCK(0, _sb->ninodes)+1) * BSIZE);

    // init internal global vars
    _num_dirs_found = 0;
    _num_inodes_found = 0;
    _num_dblks_found = 0;
    _num_inused_inodes = 0;
    init_bm_r();

    // run
    assert_superblock(); //[1]
    traverse_inodes();
    check_unref_inodes(); //[9]
    assert_bm_r(); //[6]
    assert_nlinks(); //[11]
}