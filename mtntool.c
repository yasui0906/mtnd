/*
 * mtntool.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"

void version()
{
  printf("%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
}

void usage()
{
  version();
  printf("usage: %s [OPTION] [PATH]\n", MODULE_NAME);
  printf("\n");
  printf("  OPTION\n");
  printf("   -h      --help\n");
  printf("   -V      --version\n");
  printf("   -i      --info\n");
  printf("   -l      --list\n");
  printf("   -s      --set\n");
  printf("   -g      --get\n");
  printf("   -f path --file=path\n");
  printf("\n");
}

struct option *get_optlist()
{
  static struct option opt[8];
  opt[0].name    = "help";
  opt[0].has_arg = 0;
  opt[0].flag    = NULL;
  opt[0].val     = 'h';

  opt[1].name    = "version";
  opt[1].has_arg = 0;
  opt[1].flag    = NULL;
  opt[1].val     = 'v';

  opt[2].name    = "info";
  opt[2].has_arg = 0;
  opt[2].flag    = NULL;
  opt[2].val     = 'i';

  opt[3].name    = "list";
  opt[3].has_arg = 0;
  opt[3].flag    = NULL;
  opt[3].val     = 'l';

  opt[4].name    = "file";
  opt[4].has_arg = 1;
  opt[4].flag    = NULL;
  opt[4].val     = 'f';

  opt[5].name    = "set";
  opt[5].has_arg = 0;
  opt[5].flag    = NULL;
  opt[5].val     = 's';

  opt[6].name    = "get";
  opt[6].has_arg = 0;
  opt[6].flag    = NULL;
  opt[6].val     = 'g';

  opt[7].name    = NULL;
  opt[7].has_arg = 0;
  opt[7].flag    = NULL;
  opt[7].val     = 0;
  return(opt);
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
  kstat *kst = mtn_list(path);
  sprintf(field[0], "%%%ds: %%s ", kopt.field_size[0]);
  sprintf(field[1], "%%%ds ",      kopt.field_size[1]);
  sprintf(field[2], "%%%ds ",      kopt.field_size[2]);
  sprintf(field[3], "%%%dllu ",    kopt.field_size[3]);
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
    printf(field[0], kst->member->host, m);
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

int main(int argc, char *argv[])
{
  int r;
  int mode;
  char load_path[PATH_MAX];
  char save_path[PATH_MAX];
  char file_path[PATH_MAX];

  load_path[0] = 0;
  save_path[0] = 0;
  file_path[0] = 0;
  mtn_init_option();

  mode = MTNCMD_NONE;
  while((r = getopt_long(argc, argv, "f:sglhvi", get_optlist(), NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'i':
        mode = MTNCMD_INFO;
        break;

      case 'l':
        mode = MTNCMD_LIST;
        break;

      case 's':
        mode = MTNCMD_SET;
        break;

      case 'g':
        mode = MTNCMD_GET;
        break;

      case 'f':
        strcpy(file_path, optarg);
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  mtn_hello();
  if(mode == MTNCMD_INFO){
    mtn_info();
    exit(0);
  }
  if(mode == MTNCMD_LIST){
    r = mtntool_list(argv[optind]);
    exit(r);
  }
  if(mode == MTNCMD_SET){
    if(file_path[0]){
      r = mtntool_set(argv[optind], file_path);
    }else{
      r = mtntool_set(argv[optind], argv[optind]);
    }
    exit(r);

  }
  if(mode == MTNCMD_GET){
    if(file_path[0]){
      r = mtntool_get(argv[optind], file_path);
    }else{
      r = mtntool_get(argv[optind], argv[optind]);
    }
    exit(r);
  }
  usage();
  exit(0);
}

