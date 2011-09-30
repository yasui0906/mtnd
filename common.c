/*
 * common.c
 * Copyright (C) 2011 KLab Inc.
 */
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "mtn.h"
#include "libmtn.h"
#include "common.h"

void dirbase(const char *path, char *d, char *f)
{
  char b[PATH_MAX];
	if(d){
		strcpy(b, path);
		strcpy(d, dirname(b));
	}
	if(f){
		strcpy(b, path);
		strcpy(f, basename(b));
	}
}

int mkpidfile(char *path)
{
  FILE *fd;
  if(strlen(path)){
    if((fd = fopen(path, "w"))){
      fprintf(fd, "%d\n", getpid());
      fclose(fd);
    }else{
      mtnlogger(NULL, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      return(1);
    }
  }
  return(0);
}

int rmpidfile(char *pidfile)
{
  if(strlen(pidfile)){
    unlink(pidfile);
  }
  return(0);
}

uint64_t atoikmg(char *str)
{
  int unit = 1;
  uint64_t val;
  char buff[1024];
  char *p = buff;
  strcpy(buff, str);
  while(*p){
    if((*p >= '0') && (*p <= '9')){
      p++;
      continue;
    }
    switch(*p){
      case 'K':
        unit = 1024;
        break;
      case 'M':
        unit = 1024 * 1024;
        break;
      case 'G':
        unit = 1024 * 1024 * 1024;
        break;
    }
    *p = 0;
  }
  val = atoi(buff);
  return(val * unit);
}

int mkdir_ex(const char *path)
{
  struct stat st;
  char d[PATH_MAX];
  char f[PATH_MAX];
  dirbase(path,d,f);
  if(stat(path, &st) == 0){
    return(0);
  }
  if(stat(d, &st) == -1){
    if(mkdir_ex(d) == -1){
      return(-1);
    }
  }
  return(mkdir(path, 0755));
}

