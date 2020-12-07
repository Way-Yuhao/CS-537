// Physical memory allocator, intended to allocate
// memory for user processes, kernel stacks, page table pages,
// and pipe buffers. Allocates 4096-byte pages.

#include "types.h"
#include "defs.h"
#include "param.h"
#include "mmu.h"
#include "spinlock.h"

struct run {
  struct run *next;
};

struct {
  struct spinlock lock;
  struct run *freelist;
} kmem;

static uint framelist[16384];
static int endindex = 0;

static void remove(uint framenum) {
    
    for (int i = 0; i < 16384; i++) {
        if (framenum == framelist[i]) {
            // cprintf("removing %d--%d   ", framenum, i);
            for (int j = i; framelist[j] != 0; j++) {
                framelist[j] = framelist[j + 1];
            }
            endindex--;
            framelist[endindex]=0;
            break;
        }
    }
}

static void add(uint framenum) {
    
    if (endindex >= 16384) return;
    framelist[endindex++] = framenum;
//     cprintf("adding %d--%d   ", framenum, endindex);
//     
}


extern char end[]; // first address after kernel loaded from ELF file

// Initialize free list of physical pages.
void
kinit(void)
{
  char *p;
  cprintf("kalloc init called\n");
  initlock(&kmem.lock, "kmem");
  p = (char*)PGROUNDUP((uint)end);
  for(; p + 2*PGSIZE <= (char*)PHYSTOP; p += 2*PGSIZE)
    kfree(p);
  cprintf("init done\n");
}

// Free the page of physical memory pointed at by v,
// which normally should have been returned by a
// call to kalloc().  (The exception is when
// initializing the allocator; see kinit above.)
void
kfree(char *v)
{
  struct run *r;
  struct run *r2;
  if((uint)v % PGSIZE || v < end || (uint)v >= PHYSTOP) 
    panic("kfree");

  // Fill with junk to catch dangling refs.
  memset(v, 1, PGSIZE);

  acquire(&kmem.lock);
  r = (struct run*)v;
  r2 = (struct run*)(v+PGSIZE);
  r2->next = r;
  r->next = kmem.freelist;
  
  kmem.freelist = r2;
  release(&kmem.lock);
  remove((uint)r);
  
}

// Allocate one 4096-byte page of physical memory.
// Returns a pointer that the kernel can use.
// Returns 0 if the memory cannot be allocated.
char*
kalloc(void)
{
  struct run *r;

  acquire(&kmem.lock);
  r = kmem.freelist;
//   r = kmem.freelist->next;
  if(r->next){
    kmem.freelist = r->next->next;
    add((uint)r);
  }
  release(&kmem.lock);
  return (char*)r;
}

int dump_allocated(int *frames, int numframes){

    
    cprintf("dumping for numframes:%d, endindex:%d\n", numframes, endindex);
//     int newframes[numframes];
//     frames = newframes; 
//     acquire(&kmem.lock);
//     frames = (int *)malloc(sizeof(int) * numframes);
    
//     if(endindex < numframes)return -1;
    for(int i = endindex-1; i >= endindex-numframes; i--){
        /*test code
        kmem.allocFrames[i]->frameNum = i;
        kmem.allocFrames[i]->pid = i;
        */

        frames[endindex-1-i] = framelist[i] ;
      //   frames[endindex-1-i]=i;
      //   cprintf("dumping %d: %d ", endindex-1-i, frames[endindex-1-i]);
    }
    
//     release(&kmem.lock);

    return 0;
}


