#include <stdio.h>

int main() {
    /*
    FILE* fp = fopen("test.txt", "w");
    int fSize = 64;
    // fprintf(fp, fSize);
    fwrite(&fSize, sizeof(fSize), 1, fp);
    fp->_close;
    /*/

    //*
     FILE* f = fopen("test.txt", "r");
     if (f == NULL)
         return -1;
     int fileSize;
     fread(&fileSize, sizeof(int), 1, f);
     //int fileSize = fscanf(f, "%d", &fileSize);
     //int fileSize2 = fscanf(f, "%d", &fileSize2);
     //int fileSize3 = fscanf(f, "%d", &fileSize3);
     printf(fileSize);
     //*/
}
