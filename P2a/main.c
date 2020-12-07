#include <stdio.h>
#include <stdlib.h>
#include <zconf.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>

/*FIXME: bugs:
 * 1. order of stdout and stderr
 * 2. path a -> malloc error
 **/

char* DEFAULT_PATH = "/bin";
int batchMode = 0;

struct Node {
    char* PATH;
    struct Node* next;
};

void recFreePath(struct Node* curNode) {
    if (curNode->next != NULL) {
        recFreePath(curNode->next);
    }
    free(curNode);
    curNode = NULL;
    return;
}
void printError() {
    char error_message[30] = "An error has occurred\n";
    write(STDERR_FILENO, error_message, strlen(error_message));
    fflush(stderr);
}

int main(int argc, char** argv) {
    int isChild = 0;
    // initializing shell
    FILE* input = NULL;
    if (argc == 1) {
        // interactive mode
        batchMode = 0;
        input = stdin;
    } else if (argc == 2) {
        // batch mode
        batchMode = 1;
        input = fopen(argv[1], "r");
        if (input == NULL) {
            printError(); // TODO: verify behavior
        }
    } else {
        //FIXME unknown behavior
    }
    // initializing path
    struct Node* path_head = (struct Node*) malloc(sizeof(struct Node));
    path_head->PATH = DEFAULT_PATH;
    path_head->next = NULL;
    // initializing input buffer
    char* inputBuffer = NULL;
    char* line = NULL;
    int len = 0;
    // run
    while(!feof(input)) {
        if (!batchMode && inputBuffer == NULL && isChild == 0) {
            fflush(stdout);
            fprintf(stdout, "smash> ");
        }
        // handling multiple commands
        // clear buffer before reading the next line
        if (inputBuffer != NULL) {
            line =  strsep(&inputBuffer, ";");
            // remove preceding white space
        }
        if (line == NULL) {
            getline(&inputBuffer, (size_t *) &len, input);
            // removing newline
            if (inputBuffer[strlen(inputBuffer)-1] == '\n')
                inputBuffer[strlen(inputBuffer) - 1] = 0;
            line = strsep(&inputBuffer, ";");
        }

        // handling parallel commands
        if (strchr(line, '&') != NULL) {
            char* parent_empty_line_ptr = "*PARENT*";
            char* line_rem = strdup(line);
            do {
                line = strsep(&line_rem, "&");
                int rc = fork();
                if (rc == 0) { // child process;
                    isChild = 1;
                    break;
                } else { // parent process
                    line = parent_empty_line_ptr;
                }
            } while (strchr(line_rem, '&') != NULL);
            if (line == parent_empty_line_ptr) {
                line = strdup(line_rem);
            }
        }
        // deleting preceding whitepsace
        for(int i=0; i<strlen(line); i++) {
            if (line[i] != ' ' && line[i] != '\t') {
                line = &line[i];
                break;
            }
        }
        char* cmd = strsep(&line, " \t");
        if (strcmp(cmd, "exit") == 0) {
            char* arg1 = strsep(&line, " \t");
            // having params is illegal
            if (arg1 != NULL) {
                printError();
            } else {
                exit(0);
            }

        } else if (strcmp(cmd, "cd") == 0) {
            char* arg1 = strsep(&line, " \t");
            char* arg2 = strsep(&line, " \t");
            if (arg1 == NULL || arg2 != NULL) {
                printError();
            } else { // only 1 arg
                int r = chdir(arg1);
                if (r != 0)
                    printError();
            }
        } else if (strcmp(cmd, "path") == 0) {
            char* arg1 = strsep(&line, " \t");
            if (arg1 == NULL) {
                printError();
            } else if (strcmp(arg1, "add") == 0) {
                char* arg2 = strsep(&line, " \t");
                if (arg2 == NULL)
                    printError();
               // adding new PATH to the beginning of the list
               struct Node* temp = path_head;
                path_head = (struct Node*) malloc(sizeof(struct Node));
                path_head->PATH = arg2;
                path_head->next = temp;
            } else if (strcmp(arg1, "remove") == 0) {
                char* arg2 = strsep(&line, " \t");
                if (arg2 == NULL) {
                    printError();
                } else {
                    // looking for matching PATH
                    int pathFound = 0;
                    struct Node* prevPath = NULL;
                    struct Node* curPath = path_head;
                    while (curPath != NULL) {
                        if (strcmp(curPath->PATH, arg2) == 0) {
                            pathFound = 1;
                            break;
                        } else {
                            if (curPath->next != NULL) {
                                prevPath = curPath;
                                curPath = curPath->next;
                            } else {
                                break;
                            }
                        }
                    }
                    if (pathFound) {
                        if (curPath == path_head) {
                            path_head = curPath->next;
                            curPath = NULL;
                        } else {
                            prevPath->next = curPath->next;
                            curPath->next = NULL;
                            curPath = NULL;
                        }
                    } else {
                        printError();
                    }
                }
            } else if (strcmp(arg1, "clear") == 0) {
                char* arg2 = strsep(&line, " \t");
                if (arg2 != NULL) {
                    printError();
                } else {
                    if (path_head != NULL) {
                        recFreePath(path_head);
                    }
                    path_head = NULL;
                }
            } else if (strcmp(arg1, "print") == 0) { // TODO: remove
                struct Node* curPath = path_head;
                int i = 1;
                while (curPath != NULL) {
                    printf("PATH %d: %s\n", i, curPath->PATH);
                    i++;
                    if (curPath->next == NULL) {
                        break;
                    } else {
                        curPath = curPath->next;
                    }
                }
            } else {
                // invalid arguments for PATH
                printError();
            }
            // end of built-in commands
        } else { // non built-in commands
            // parsing args
            FILE* output = stdin;
            char* outputFileName = NULL;
            int redirect_mode = 0;
            char* args[3]; // FIXME: max arg count = 3?
            for (int j=0; j<=2; j++) {
                args[j] = NULL;
            }
            char* newArg = strsep(&line, " \t");
            int i = 1;
            while (newArg != NULL) {
                if (strcmp(newArg, ">") == 0) {
                    // redirection mode
                    redirect_mode = 1;
                    outputFileName = strsep(&line, " \t");
                    // TODO: check >>
                    break;
                }
                args[i] = strdup(newArg);
                i++;
                if (i > 3) {
                    printError(); // FIXME
                    break;
                }
                newArg = strsep(&line, " \t");
            }
            // reading from PATH
            int exeFound = 0;
            struct Node* curNode = path_head;
            char* path = NULL;
            while (curNode != NULL) {
                path = strdup(curNode->PATH);
                strcat(path, "/");
                strcat(path, cmd);
                args[0] = strdup(path);
                int r = access(path, X_OK);
                if (r == 0) { // if executable exits in current PATH
                    exeFound = 1;
                    int rc = fork();
                    if (rc == 0) { //child process
                        if (redirect_mode) {
                            int fd = open(outputFileName, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
                            if (fd != 0) {
                                exit(1);
                            }
                            dup2(fd, 1); // sending stdout to output
                            dup2(fd, 2); //sending stderr to output
                            close(fd);
                            redirect_mode = 0;
                        }
                        int exec_rc = execv(path, args);
                    } else { // parent process
                        int wait_rc = wait(NULL);
                    }
                    break;
                } else { // if exe doesn't exit in current PATH
                    curNode = curNode->next;
                }
            }
            if (!exeFound) {
                printError();
            }
            // TODO: close file here
        }
        free(line);
        line = NULL;
        if (isChild) {
            return 0;
        }
        fflush(stdout);
    }
}