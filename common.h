/*
 * common.h
 * Copyright (C) 2011 KLab Inc.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
void dirbase(const char *path, char *d, char *f);
int mkpidfile(char *path);
int rmpidfile(char *path);
uint64_t atoikmg(char *str);
int mkdir_ex(const char *path);
