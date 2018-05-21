/*
 * mtnfile.c
 * Copyright (C) 2011 KLab Inc.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <libgen.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <stddef.h>
#include <stdint.h>
#include <stdarg.h>
#include <limits.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include "mtn.h"
#include "libmtn.h"
#include "common.h"
#include "mtnfile.h"

static MTN *mtn;
static CTX *ctx;

void version()
{
  printf("%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
}

void usage()
{
  version();
  printf("usage: %s [OPTION] [HOST]:REMOTE_PATH\n", MODULE_NAME);
  printf("\n");
  printf("  OPTION\n");
  printf("   -h            (--help)      # help\n");
  printf("   -v            (--version)   # show version\n");
  printf("   -i            (--info)      # show infomation\n");
  printf("   -c            (--choose)    # choose host\n");
  printf("   -u nnnn[KMG]  (--use)       # use space\n");
  printf("   -P LOCAL_PATH (--put)       # file upload\n");
  printf("   -G LOCAL_PATH (--get)       # file download\n");
  printf("   -D            (--delete)    # file delete\n");
  printf("   -m addr                     # multicast addr\n");
  printf("   -p port                     # multicast port\n");
  printf("   -R host       (--rdonly)    # set read-only mode\n");
  printf("   -W host       (--no-rdonly) # set read-write mode\n");

  printf("\n");
}

MTNSVR *getinfo()
{
  MTNSVR *s;
  MTNSVR *members = NULL;
  MTNSVR *svrlist = NULL;
  svrlist = mtn_info(mtn);
  for(s=svrlist;s;s=s->next){
    if(is_export(s)){
      members = pushsvr(members, s);
    }
  }
  clrsvr(svrlist);
  return(members);
}

int mtnfile_info()
{
  MTNSVR *s;
  uint32_t node  = 0;
  uint64_t msize = 0;
  uint64_t mfree = 0;
  uint64_t dsize = 0;
  uint64_t dfree = 0;
  uint32_t cpu_num = 0;
  for(s=getinfo();s;s=s->next){
    node++;
    cpu_num += s->cnt.cpu;
    msize += s->memsize/1024/1024;
    mfree += s->memfree/1024/1024;
    dsize  = s->dsize/1024/1024;
    dfree  = s->dfree/1024/1024;
    printf("%5s: ",       s->host);
    printf("ORD=%03d ",   s->order);
    printf("CPU=%02d ",   s->cnt.cpu);
    printf("LA=%d.%02d ", s->loadavg / 100, s->loadavg % 100);
    printf("MS=%luM ",    s->memsize/1024/1024);
    printf("MF=%luM ",    s->memfree/1024/1024);
    printf("DF=%luG",     dfree/1024);
    printf("(%02lu%%) ",  dfree * 100 / dsize);
    printf("PS=%d ",      s->cnt.prc);
    printf("VSZ=%luK ",   s->vsz/1024);
    printf("RES=%luK ",   s->res/1024);
    printf("MLC=%d ",     s->cnt.mem);
    printf("TSK=%d ",     s->cnt.tsk);
    printf("TSV=%d ",     s->cnt.tsv);
    printf("SVR=%d ",     s->cnt.svr);
    printf("DIR=%d ",     s->cnt.dir);
    printf("STA=%d ",     s->cnt.sta);
    printf("STR=%d ",     s->cnt.str);
    printf("ARG=%d ",     s->cnt.arg);
    printf("CLD=%d ",     s->cnt.cld);
    if(s->flags & MTNMODE_RDONLY){
      printf("RDONLY ");
    }
    printf("\n");
  }
  if(!node){
    printf("node not found");
  }else{
    printf("TOTAL: ");
    printf("%dCPU(%dnode) ", cpu_num, node);
    if(msize){
      printf("MEM=%luM/%luM(%lu%%Free) ",  mfree, msize, mfree * 100 / msize);
    }
  }
  printf("\n");
  return(0);
}

int mtnfile_choose()
{
  MTNSVR *s = mtn_choose(mtn);
  while(s){
    printf("%s\n", s->host);
    s = delsvr(s);
  }
  return(0);
}

STR mtnfile_list_format_string(MTNSTAT *kst)
{
  int d = 0;
  int l = 0;
  int h = 0;
  int u = 0;
  int g = 0;
  int s = 0;
  char buff[64];
  struct passwd *pw;
  struct group  *gr;
  while(kst){
    l = strlen(kst->svr->host);
    h = MAX(l, h);
    if((pw = getpwuid(kst->stat.st_uid))){
      l = strlen(pw->pw_name);
    }else{
      l = 1;
      d = kst->stat.st_uid;
      while((d = d / 10)){
        l ++;
      }
    }
    u = MAX(l, u);

    if((gr = getgrgid(kst->stat.st_gid))){
      l = strlen(gr->gr_name);
    }else{
      l = 1;
      d = kst->stat.st_gid;
      while((d = d / 10)){
        l ++;
      }
    }
    g = MAX(l, g);

    l = 1;
    d = kst->stat.st_size;
    while((d = d / 10)){
      l ++;
    }
    s = MAX(l, s);
    kst = kst->next;
  }
  sprintf(buff, "%%-%ds: %%s %%-%ds %%-%ds %%-%dlu ", h, u, g, s);
  return(newstr(buff));
}

int mtnfile_list(char *path)
{
  char pname[64];
  char gname[64];
  struct tm     *tm;
  struct passwd *pw;
  struct group  *gr;
  MTNSTAT *rst = mtn_list(mtn, path);
  MTNSTAT *kst = rst;
  STR fs = mtnfile_list_format_string(rst);
  while(kst){
    tm = localtime(&(kst->stat.st_mtime));
    if((pw = getpwuid(kst->stat.st_uid))){
      strcpy(pname, pw->pw_name);
    }else{
      sprintf(pname, "%d", kst->stat.st_uid);
    }
    if((gr = getgrgid(kst->stat.st_gid))){
      strcpy(gname, gr->gr_name);
    }else{
      sprintf(gname, "%d", kst->stat.st_gid);
    }
    printf(fs, kst->svr->host, get_mode_string(kst->stat.st_mode), pname, gname, kst->stat.st_size);
    printf("%02d/%02d/%02d ", (1900 + tm->tm_year) % 100, tm->tm_mon, tm->tm_mday);
    printf("%02d:%02d:%02d ", tm->tm_hour, tm->tm_min, tm->tm_sec);
    printf("%s\n", kst->name);
    kst = kst->next;
  }
  rst = clrstat(rst);
  return(0);
}

int mtnfile_put(char *save_path, char *file_path)
{
  int r = 0;
  int f = 0;
  if(strcmp("-", file_path)){
    if((f = open(file_path, O_RDONLY)) == -1){
      mtnlogger(mtn, 0, "[error]: %s %s\n", strerror(errno), file_path);
      return(1);
    }
  }
  if(mtn_put(mtn, f, save_path) == 1){
    r = 1;
    mtnlogger(mtn, 0, "[error]: %s %s\n", strerror(errno), save_path);
  }
  if(f){
    close(f);
  }
  return(r); 
}

int mtnfile_get_open(char *path, MTNSTAT *st)
{
  int f = 1;
  if(strcmp("-", path)){
    f = creat(path, st->stat.st_mode);
    if(f == -1){
      mtnlogger(mtn, 0, "[error]: %s %s\n", strerror(errno), path);
    }
  }
  return(f);
}

int mtnfile_get(char *path, char *file)
{
  int f;
  MTNSTAT *st;
  st = mtn_stat(mtn, path);
  f  = mtnfile_get_open(file, st);
  if(f == -1){
    return(1);
  }
  mtn_get(mtn, f, path);
  if(f > 2){
    close(f);
  }
  return(0); 
}

int mtnfile_del(STR remote)
{
  return(0);
}

int mtnfile_rdonly(STR host, int flag)
{
  return(mtn_rdonly(mtn, host, flag));
}

int mtnfile_console_help(STR cmd)
{
  if(!cmd){
    printf("ls\n");
    printf("cat\n");
    printf("put\n");
    printf("get\n");
    printf("del\n");
    printf("cd\n");
    printf("lcd\n");
    printf("info\n");
    printf("help\n");
    printf("exit\n");
  }else if(!strcmp(cmd, "ls")){
  }else if(!strcmp(cmd, "cat")){
  }else if(!strcmp(cmd, "put")){
  }else if(!strcmp(cmd, "get")){
  }else if(!strcmp(cmd, "del")){
  }else if(!strcmp(cmd, "cd")){
  }else if(!strcmp(cmd, "lcd")){
  }else if(!strcmp(cmd, "info")){
  }else{
    printf("%s: no such command.\n", cmd);
  }
  return(0);
}

STR mtnfile_console_path(STR opt)
{
  int i;
  STR path;
  ARG dirs;
  path = newstr(ctx->remote_path);
  dirs = splitstr(opt, "/");
  for(i=0;dirs[i];i++){
    if(!strcmp(dirs[i], "..")){
      path = modstr(path, dirname(path));
    }else if(!strcmp(dirs[i], ".")){
    }else{
      if(lastchar(path) != '/'){
        path = catstr(path, "/");
      }
      path = catstr(path, dirs[i]);
    }
  }
  dirs = clrarg(dirs);
  return(path);
}

int mtnfile_console_cat(int argc, ARG args)
{
  STR path = mtnfile_console_path(args[1]);
  mtnfile_get(path, "-");
  path = clrstr(path);
  return(0);
}

int mtnfile_console_ls(int argc, ARG args)
{
  STR path = mtnfile_console_path(args[1]);
  mtnfile_list(path);
  path = clrstr(path);
  return(0);
}

int mtnfile_console_cd(int argc, ARG args)
{
  STR path;
  int r = 1;
  MTNSTAT *rst;
  MTNSTAT *kst;
  if(argc == 1){
    ctx->remote_path = modstr(ctx->remote_path, "/");
    return(0);
  }
  path = mtnfile_console_path(args[1]);
  rst = mtn_stat(mtn, path);
  for(kst=rst;kst;kst=kst->next){
    if(S_ISDIR(kst->stat.st_mode)){
      ctx->remote_path = modstr(ctx->remote_path, path);
      r = 0;      
      break;
    }
  }
  if(r){
    mtnlogger(mtn, 0, "error: %s: not directory.\n", args[1]);
  }
  path = clrstr(path);
  rst  = clrstat(rst);
  return(r);
}

int mtnfile_console_lcd()
{
  return(0);
}

int mtnfile_console_get()
{
  return(0);
}

int mtnfile_console_put()
{
  return(0);
}

int mtnfile_console_del()
{
  return(0);
}

char* mtnfile_console_readline_command(const char* text, int state)
{
  int i;
  int l;
  char *commands[] = {"ls", "cd", "lcd", "cat", "get", "put", "del", "help", "exit", NULL};
  l = strlen(text);
  for(i=state;commands[i];i++){
    if(!strncmp(text, commands[i], l)){
      return(strdup(commands[i]));
    }
  }
  return(NULL);
}

char** mtnfile_console_readline_callback(const char* text, int start, int end)
{
  return(rl_completion_matches(text, mtnfile_console_readline_command));
}

int mtnfile_console()
{
  int   argc;
  ARG   args;
  char *line;
  STR prompt = newstr("mtn:/> ");
  ctx->remote_path = newstr("/");
  rl_attempted_completion_function = mtnfile_console_readline_callback;
  while((line = readline(prompt))){
    if(!strlen(line)){
      free(line); 
      continue;
    }
    args = splitstr(line, " ");
    argc = cntarg(args);
    if(!argc){
      free(line);
      continue;
    }else if(!strcmp(args[0], "exit")){ break;
    }else if(!strcmp(args[0], "quit")){ break;
    }else if(!strcmp(args[0], "help")){ mtnfile_console_help(args[1]);
    }else if(!strcmp(args[0], "ls"  )){ mtnfile_console_ls(argc,  args);
    }else if(!strcmp(args[0], "cd"  )){ mtnfile_console_cd(argc,  args);
    }else if(!strcmp(args[0], "lcd" )){ mtnfile_console_lcd(argc, args);
    }else if(!strcmp(args[0], "cat" )){ mtnfile_console_cat(argc, args);
    }else if(!strcmp(args[0], "get" )){ mtnfile_console_get(argc, args);
    }else if(!strcmp(args[0], "put" )){ mtnfile_console_put(argc, args);
    }else if(!strcmp(args[0], "del" )){ mtnfile_console_del(argc, args);
    }else{ mtnlogger(mtn, 0, "%s: no such command.\n", args[0]); }
    add_history(line);
    free(line);
    line = NULL;
    args = clrarg(args);
    prompt = modstr(prompt, "mtn:");
    prompt = catstr(prompt, ctx->remote_path);
    prompt = catstr(prompt, "> ");
  }
  return(0);
}

static struct option opts[]={
  {"help",      0, NULL, 'h'},
  {"version",   0, NULL, 'v'},
  {"info",      0, NULL, 'i'},
  {"choose",    0, NULL, 'c'},
  {"put",       1, NULL, 'P'},
  {"get",       1, NULL, 'G'},
  {"delete",    0, NULL, 'D'},
  {"rdonly",    1, NULL, 'R'},
  {"no-rdonly", 1, NULL, 'W'},
  {NULL,        0, NULL, 0}
};

int init(int argc, char *argv[])
{
  int i;
  int r;
  mtn = mtn_init(MODULE_NAME);
  ctx = calloc(1,sizeof(CTX));
  if(!mtn){
    return(MTNTOOL_ERROR);
  }
  if(!ctx){
    mtnlogger(NULL, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(MTNTOOL_ERROR);
  }
  mtn->logtype = 0;
  mtn->logmode = MTNLOG_STDERR;
  ctx->mode    = (argc == 1) ? MTNTOOL_CONSOLE : MTNTOOL_LIST;
  while((r = getopt_long(argc, argv, "R:W:P:G:Du:m:p:hvicd", opts, NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'i':
        if(ctx->mode != MTNTOOL_LIST){
          usage();
          exit(1);
        }
        ctx->mode = MTNTOOL_INFO;
        break;

      case 'c':
        if(ctx->mode != MTNTOOL_LIST){
          usage();
          exit(1);
        }
        ctx->mode = MTNTOOL_CHOOSE;
        break;

      case 'P':
        if(ctx->mode != MTNTOOL_LIST){
          usage();
          exit(1);
        }
        ctx->mode = MTNTOOL_PUT;
        ctx->local_path = newstr(optarg);
        break;

      case 'G':
        if(ctx->mode != MTNTOOL_LIST){
          usage();
          exit(1);
        }
        ctx->mode = MTNTOOL_GET;
        ctx->local_path = newstr(optarg);
        break;

      case 'D':
        if(ctx->mode != MTNTOOL_LIST){
          usage();
          exit(1);
        }
        ctx->mode = MTNTOOL_DEL;
        break;

      case 'u':
        mtn->choose.use = atoikmg(optarg);
        if(!mtn->choose.use){
          usage();
          exit(1);
        }
        break;

      case 'd':
        mtn->loglevel++;
        break;

      case 'm':
        strcpy(mtn->mcast_addr, optarg);
        break;

      case 'p':
        mtn->mcast_port = atoi(optarg);
        break;

      case 'R':
        ctx->remote_host = newstr(optarg);
        ctx->mode = MTNTOOL_RDONLY;
        break;

      case 'W':
        ctx->remote_host = newstr(optarg);
        ctx->mode = MTNTOOL_NORDONLY;
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  if(optind < argc){
    ctx->remote_path = newstr(argv[optind]);
  }else{
    if(ctx->local_path){
      ctx->remote_path = ctx->local_path;
      ctx->local_path = newstr(basename(ctx->local_path));
    }
  }
  mtnlogger(mtn, 1, "local : %s\n", ctx->local_path);
  mtnlogger(mtn, 1, "remote: %s\n", ctx->remote_path);
  return(ctx->mode);
}

int main(int argc, char *argv[])
{
  int r = 0;
  switch(init(argc, argv)){
    case MTNTOOL_ERROR:
      r = 1;
      break;

    case MTNTOOL_CONSOLE:
      r = mtnfile_console();
      break;

    case MTNTOOL_INFO:
      r = mtnfile_info();
      break;

    case MTNTOOL_CHOOSE:
      r = mtnfile_choose();
      break;

    case MTNTOOL_LIST:
      r = mtnfile_list(ctx->remote_path);
      break;

    case MTNTOOL_PUT:
      r = mtnfile_put(ctx->remote_path, ctx->local_path);
      break;

    case MTNTOOL_GET:
      r = mtnfile_get(ctx->remote_path, ctx->local_path);
      break;

    case MTNTOOL_DEL:
      r = mtnfile_del(ctx->remote_path);
      break;

    case MTNTOOL_RDONLY:
      r = mtnfile_rdonly(ctx->remote_host, 1);
      break;

    case MTNTOOL_NORDONLY:
      r = mtnfile_rdonly(ctx->remote_host, 0);
      break;
  }
  exit(r);
}

