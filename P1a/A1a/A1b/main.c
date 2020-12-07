#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>



void writeToTar(FILE* tarFile, char* filePath){
    FILE *fp = fopen(filePath, "r");
    if (fp == NULL) {printf("wis-tar: cannot open file\n");
        exit(1);
    }
    // writing to tar file
    // file name [100 bytes in ASCII]
    char fileName[101];
    if (strlen(filePath) > 100) {
        memcpy(fileName, &filePath[0], 100); //FIXME
        fileName[100] = '\0';
    } else {
        strcpy(fileName, filePath);
    }
    fprintf(tarFile, "%s", fileName);
    // limit file name to 100 bytes
    for (int i = 100 - strlen(filePath); i > 0; i--) {
        fprintf(tarFile, "%c", '\0');
    }
    // file size [8 bytes as binary]
    struct stat info;
    int err = stat(filePath, &info);
    if (err != 0) {
        exit(1); //FIXME
    }
    long long int fSize = (long long int) info.st_size; //FIXME: why??
    //putw(fSize, tarFile);
    fwrite(&fSize, sizeof(long long int), 1, tarFile);
    // content of file [in ASCII]
    char* curLine = NULL;
    int len = 0;
    while (getline(&curLine, (size_t *) &len, fp) != -1 ) {
        //output each line to tarFile
        fprintf(tarFile, "%s", curLine);
    }
    fclose(fp);
}

int main(int argc, char** argv) {
    if (argc == 1 || argc == 2) {
        printf("wis-tar: tar-file file […]\n");
        return 1;
    } else {
        // FIXME: override?
        FILE* tarFile = fopen(argv[1], "w");
        if (tarFile == NULL) {
            printf("wis-tar: cannot open file\n");
            return 1;
        }
        for (int i = 2; i < argc; i++) {
            writeToTar(tarFile, argv[i]);
        }
        fclose(tarFile);
    }
}
