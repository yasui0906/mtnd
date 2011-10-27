/*
 * mtntool.c
 * Copyright (C) 2011 KLab Inc.
 */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdarg.h>
#include <limits.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <mtn.h>
#include "mtntool.h"

MTN *mtn;

void version()
{
  printf("%s version %s\n", MODULE_NAME, MTN_VERSION);
}

void usage()
{
  version();
  printf("usage: %s [OPTION] [PATH]\n", MODULE_NAME);
  printf("\n");
  printf("  OPTION\n");
  printf("   -h      --help      #\n");
  printf("   -v      --version   #\n");
  printf("   -i      --info      #\n");
  printf("   -l      --list      #\n");
  printf("   -s      --set       #\n");
  printf("   -g      --get       #\n");
  printf("   -d      --delete    #\n");
  printf("   -f path --file=path #\n");
  printf("\n");
}

void mtntool_info()
{

}

int mtntool_list(char *path)
{
  uint8_t m[16];
  uint8_t field[4][32];
  uint8_t pname[64];
  uint8_t gname[64];
  struct tm     *tm;
  struct passwd *pw;
  struct group  *gr;
  MTNSTAT *kst = mtn_list(mtn, path);
  sprintf(field[0], "%%s: " );
  sprintf(field[1], "%%s "  );
  sprintf(field[2], "%%s "  );
  sprintf(field[3], "%%llu ");
  while(kst){
    tm = localtime(&(kst->stat.st_mtime));
    if(pw = getpwuid(kst->stat.st_uid)){
      strcpy(pname, pw->pw_name);
    }else{
      sprintf(pname, "%d", kst->stat.st_uid);
    }
    if(gr = getgrgid(kst->stat.st_gid)){
      strcpy(gname, gr->gr_name);
    }else{
      sprintf(gname, "%d", kst->stat.st_gid);
    }
    get_mode_string(m, kst->stat.st_mode);
    printf(field[0], kst->svr->host);
    printf("%s ", m);
    printf(field[1], pname);
    printf(field[2], gname);
    printf(field[3], kst->stat.st_size);
    printf("%02d/%02d/%02d ", (1900 + tm->tm_year) % 100, tm->tm_mon, tm->tm_mday);
    printf("%02d:%02d:%02d ", tm->tm_hour, tm->tm_min, tm->tm_sec);
    printf("%s\n", kst->name);
    kst = kst->next;
  }
  return(0);
}

int mtntool_put(char *save_path, char *file_path)
{
  int f = 0;
  if(strcmp("-", file_path)){
    if((f = open(file_path, O_RDONLY)) == -1){
      return(-1);
    }
  }
  mtn_put(mtn, f, save_path);
  if(f){
    close(f);
  }
  return(0); 
}

int mtntool_get_open(char *path, MTNSTAT *st)
{
  int f;
  if(strcmp("-", path) == 0){
    f = 0;
  }else{
    f = creat(path, st->stat.st_mode);
    if(f == -1){
      printf("error: %s %s\n", strerror(errno), path);
    }
  }
  return(f);
}

int mtntool_get(char *path, char *file)
{
  int f = 0;
  MTNSTAT *st;
  st = mtn_stat(mtn, path);
  f = mtntool_get_open(file, st);
  if(f == -1){
    printf("error: %s\n", __func__);
    return(1);
  }
  mtn_get(mtn, f, path);
  if(f){
    close(f);
  }
  return(0); 
}

static struct option opts[]={
  "help",    0, NULL, 'h',
  "version", 0, NULL, 'v',
  "info",    0, NULL, 'i',
  "list",    0, NULL, 'l',
  "file",    1, NULL, 'f',
  "put",     1, NULL, 'P',
  "get",     1, NULL, 'G',
  "console", 0, NULL, 'C',
  NULL,      0, NULL, 0
};

int main(int argc, char *argv[])
{
  int  r;
  char local_path[PATH_MAX] ={0};
  char remote_path[PATH_MAX]={0};
  mtn = mtn_init(MODULE_NAME);
  while((r = getopt_long(argc, argv, "f:P:G:lhviC", opts, NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'i':
        mtntool_info();
        exit(0);

      case 'l':
        r = mtntool_list(optarg);
        exit(r);

      case 'P':
        if(local_path[0]){
          r = mtntool_put(optarg, local_path);
        }else{
          r = mtntool_put(optarg, optarg);
        }
        exit(r);

      case 'G':
        if(local_path[0]){
          r = mtntool_get(optarg, local_path);
        }else{
          r = mtntool_get(optarg, optarg);
        }
        exit(r);

      case 'f':
        strcpy(local_path, optarg);
        break;

      case 'C':
        strcpy(local_path, "-");
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  usage();
  exit(0);
}

