#include <stdio.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

int main() {
    int rc = fork();
    if (rc == 0) {
       return 0;
    }
    printf("MaFacker");
}
