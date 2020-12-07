#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main(int argc, char** argv) {
    switch(argc) {
        case 1:
            // no command line argument
            printf("wis-grep: searchterm [file …]\n");
            exit(1);
        case 2: {
            // no files are specified. Reading from stdin
            if (strncmp(argv[1], "\0", 1) == 0) {
                break;
            }
            char* curLine = NULL;
            int len = 0;
            while (getline(&curLine, (size_t *) &len, stdin) != -1) {
                if (strstr(curLine, argv[1]) != NULL) {
                    printf("%s", curLine);
                }
            }
            break;
        }
        default: {
            char* searchTerm = argv[1];
            // both search term and files are provided
            // iterate through each input file
            int i = argc - 1;
            do {
                FILE* fp = fopen(argv[i], "r");
                if (fp == NULL) {
                    printf("wis-grep: cannot open file\n");
                    exit(1);
                }
                if (strncmp(searchTerm, "\0", 1) == 0) {
                    return 0;
                }
                i -= 1;
                char* curLine = NULL;
                int len = 0;
                // read till end of file
                while (getline(&curLine, (size_t *) &len, fp) != -1 ) {
                    // search searchTerm within each line
                    if (strstr(curLine, searchTerm) != NULL) {
                        printf("%s", curLine);
                    }
                }
                fclose(fp);
            } while (i > 1);
            break;
        }
    }
    return 0;
}
