/*
 * mtntool.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"
#include "common.h"

void version()
{
  printf("%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
}

void usage()
{
  printf("mode_t=%d\n", sizeof(mode_t));
  printf("uid_t=%d\n", sizeof(uid_t));
  printf("gid_t=%d\n", sizeof(gid_t));
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

int mtntool_set(char *save_path, char *file_path)
{
  int f = 0;
  int s = 0;
  kdata data;
  kmember *member; 
  struct stat st;

  member = mtn_choose(save_path);
  if(member == NULL){
    printf("%s: node not found\n", __func__);
    return(1);
  }

  f = mtntool_set_open(file_path, &st);
  if(f == -1){
    printf("%s: %s %s\n", __func__, strerror(errno), file_path);
    return(1);
  }

  s = create_socket(0, SOCK_STREAM);
  if(s == -1){
    printf("error: %s\n", __func__);
    mtntool_set_close(f, s);
    return(1);
  }

  if(connect(s, &(member->addr.addr.addr), member->addr.len) == -1){
    printf("error: %s %s:%d\n", strerror(errno), inet_ntoa(member->addr.addr.in.sin_addr), ntohs(member->addr.addr.in.sin_port));
    mtntool_set_close(f, s);
    return(1);
  }
  //printf("connect: %s %s (%dM free)\n", member->host, inet_ntoa(member->addr.addr.in.sin_addr), member->free);
  if(mtntool_set_stat(s, save_path, &st) == -1){
    printf("%s: stat %s %s\n", __func__, strerror(errno), save_path);
    mtntool_set_close(f, s);
    return(1);
  }
  if(mtntool_set_write(f, s) == -1){
    printf("%s: write %s %s\n", __func__, strerror(errno), save_path);
    mtntool_set_close(f, s);
    return(1);
  }
  mtntool_set_close(f, s);
  return(0); 
}

int mtntool_get_open(char *path, kstat *st)
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

int mtntool_get_write(int f, int s, char *path)
{
  int r;
  kdata sd;
  kdata rd;
  uint8_t *buff;
  size_t size;

  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.type = MTNCMD_GET;
  mtn_set_string(path, &sd);
  send_stream(s, &sd);

  sd.head.size = 0;
  sd.head.type = MTNRES_SUCCESS;
  while(is_loop){
    recv_stream(s, (uint8_t *)&(rd.head), sizeof(rd.head));
    if(rd.head.size == 0){
      break;
    }else{
      recv_stream(s, rd.data.data, rd.head.size);
      write(f, rd.data.data, rd.head.size);
    }
  }
  send_stream(s, &sd);
  return(0);
}

int mtntool_get_close(int f, int s)
{
  if(f > 0){
    close(f);
  }
  if(s > 0){
    close(s);
  }
  return(0);
}

int mtntool_get(char *path, char *file)
{
  int f = 0;
  int s = 0;
  kstat *st;

  st = mtn_find(path, 0);
  if(st == NULL){
    printf("error: node not found\n");
    return(1);
  }

  s = create_socket(0, SOCK_STREAM);
  if(s == -1){
    printf("error: %s\n", __func__);
    return(1);
  }

  if(connect(s, &(st->member->addr.addr.addr), st->member->addr.len) == -1){
    printf("error: %s %s:%d\n", strerror(errno), inet_ntoa(st->member->addr.addr.in.sin_addr), ntohs(st->member->addr.addr.in.sin_port));
    mtntool_get_close(f, s);
    return(1);
  }
  //printf("connect: %s %s %s (%dM free)\n", path, st->member->host, inet_ntoa(st->member->addr.addr.in.sin_addr), st->member->free);
  f = mtntool_get_open(file, st);
  if(f == -1){
    printf("error: %s\n", __func__);
    return(1);
  }
  mtntool_get_write(f, s, path);
  mtntool_get_close(f, s);
  return(0); 
}

int mtntool_set_open(char *path, struct stat *st)
{
  int f;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  if(strcmp("-", path) == 0){
    st->st_uid   = getuid();
    st->st_gid   = getgid();
    st->st_mode  = 0640;
    st->st_atime = tv.tv_sec;
    st->st_mtime = tv.tv_sec;
  }else{
    f = open(path, O_RDONLY);
    if(f == -1){
      return(-1);
    }
    fstat(f, st);
  }
  return(f);
}

int mtntool_set_stat(int s, char *path, struct stat *st)
{
  kdata sd;
  kdata rd;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.type = MTNCMD_SET;
  mtn_set_string(path, &sd);
  mtn_set_stat(st, &sd);
  send_data_stream(s, &sd);
  recv_data_stream(s, &rd);
  if(rd.head.type == MTNRES_ERROR){
    mtn_get_int(&errno, &rd, sizeof(errno));
    return(-1);
  }
  return(0);
}

int mtntool_set_write(int f, int s)
{
  int r;
  kdata sd;
  kdata rd;

  sd.head.fin  = 0;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNCMD_SET;
  while(r = read(f, sd.data.data, sizeof(sd.data.data))){
    if(r == -1){
      return(-1);
    }
    sd.head.size = r;
    r = send_data_stream(s, &sd);
    if(r == -1){
      return(-1);
    }
    r = recv_data_stream(s, &rd);
    if(r == -1){
      return(-1);
    }
    if(rd.head.type == MTNRES_ERROR){
      mtn_get_int(&errno, &rd, sizeof(errno));
      return(-1);
    }
  }
  sd.head.fin  = 1;
  sd.head.size = 0;
  sd.head.type = MTNCMD_SET;
  r = send_stream(s, &sd);
  if(r == -1){
    return(-1);
  }
  r = recv_data_stream(s, &rd);
  if(r == -1){
    return(-1);
  }
  if(rd.head.type == MTNRES_ERROR){
    mtn_get_int(&errno, &rd, sizeof(errno));
    return(-1);
  }
  return(0);
}

int mtntool_set_close(int f, int s)
{
  if(f > 0){
    close(f);
  }
  if(s < 0){
    return(0);
  }
  close(s);
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

  mode = MTNCMD_MAX;
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

