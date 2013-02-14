/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 * (C) 2012 Dirk Meister, Johannes Gutenberg University Mainz
 *
 * This file is part of dedupv1.
 *
 * dedupv1 is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * dedupv1 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with dedupv1. If not, see http://www.gnu.org/licenses/.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <unistd.h>

int main(int argc, char * argv[]) {
    int ret;
    char **cmd = malloc(sizeof(char*) * (argc + 1));
    cmd[0] = malloc(strlen(DEDUPV1_ROOT) + strlen("/bin/dedupv1d") + 1);
    strcpy(cmd[0], DEDUPV1_ROOT);
    strcat(cmd[0], "/bin/dedupv1d");

    int i = 1;
    for(i = 1; i < argc; i++) {
        cmd[i] = argv[i];
    }
    cmd[argc] = (char*) 0;

    ret = execv (cmd[0], cmd);
    free(cmd[0]);
    free(cmd);
    return ret;
}
