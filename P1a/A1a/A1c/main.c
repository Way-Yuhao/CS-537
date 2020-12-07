#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main(int argc, char** argv) {
    switch(argc) {
        case 1:
            printf("wis-untar: tar-file\n");
            return 1;
        case 2: {
            FILE* tarFile = fopen(argv[1], "r");
            if (tarFile == NULL) {
                printf("wis-untar: cannot open file\n");
                return 1;
            }
            while(!feof(tarFile)) {
                // handling file name
                char *fileName = malloc(100 * sizeof(char));
                char c;
                for (int i = 0; i < 100; i++) {
                    c = (char) fgetc(tarFile);
                    if (c == EOF) {
                        fclose(tarFile);
                        exit(0);
                    } else if (c == '\0') {
                            continue;
                    } else {
                        fileName[i] = c;
                    }
                }
                // handling file size
                long long int fileSize;
                fread(&fileSize, sizeof(long long int), 1, tarFile);
                // handling file content
                FILE *fp = fopen(fileName, "w");
                if (fp == NULL)
                    printf("wis-untar: cannot open file\n");
                char *buffer = malloc(fileSize);
                fread(buffer, fileSize, 1, tarFile);
                //printf("%s", buffer); FIXME
                fwrite(buffer, fileSize, 1, fp);
                fclose(fp);
            }
            fclose(tarFile);
            break;
        }
        default: {
            printf("wis-untar: tar-file\n");
            return 1;
        }
    }
}
