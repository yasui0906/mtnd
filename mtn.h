/*
 * mtn.h
 * Copyright (C) 2011 KLab Inc.
 */
#define MTN_DEBUG
#define MTN_VERSION "0.9"
#include <unistd.h>
#include <stdint.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>

#ifdef MTN_DEBUG
#define MTNDEBUG(...) mtndebug(__func__, __VA_ARGS__)
#define MTNDUMPARG(a) mtndumparg(__func__, a)
#else
#define MTNDEBUG(...)
#define MTNDUMPARG(a)
#endif

typedef char *STR;
typedef STR  *ARG;
typedef enum {
  MTNLOG_NOTUSE,
  MTNLOG_STDERR,
  MTNLOG_SYSLOG,
} MTNLOG;

typedef struct mtnaddr
{
  socklen_t len;
  union {
    struct sockaddr addr;
    struct sockaddr_in in;
    struct sockaddr_storage storage;
  } addr;
} MTNADDR;

typedef struct mtnsvr
{
  struct mtnsvr *prev;
  struct mtnsvr *next;
  MTNADDR  addr;
  char    *host;
  uint8_t  mark;
  uint32_t bsize;
  uint32_t fsize;
  uint64_t dsize;
  uint64_t dfree;
  uint64_t limit;
  uint64_t vsz;
  uint64_t res;
  uint32_t cpu_num;
  uint32_t loadavg;
  uint32_t pscount;
  uint64_t memsize;
  uint64_t memfree;
  int malloccnt;
  int membercnt;
  struct timeval tv;
} MTNSVR;

typedef struct mtnstat
{
  struct mtnstat *prev;
  struct mtnstat *next;
  struct stat     stat;
  char           *name;
  MTNSVR          *svr;
} MTNSTAT;

typedef struct mtndir
{
  struct mtndir *prev;
  struct mtndir *next;
  struct mtnstat  *st;
  struct timeval   tv;
  uint32_t       flag;
  char path[PATH_MAX];
} MTNDIR;

typedef struct
{
  pid_t      pid;
  uint8_t  state;
  uint64_t utime;
  uint64_t stime;
  struct timeval tv;
} MTNPROCSTAT;

typedef struct mtnjob
{
  int       id;
  pid_t    pid;
  uid_t    uid;
  gid_t    gid;
  int      cid; // CPU-ID
  int      cpu; // CPU使用率(整数で処理するために10倍)
  int      lim; // CPU使用率の上限値
  uint64_t ctm; // CPU時間
  uint64_t rtm; // 経過時間
  int      cct; // 起動した子プロセスの数
  int      con; // mtndと接続するソケット
  int      efd; // epoll用のディスクリプタ
  int      fin; // コマンドが終了したら1になる
  int      pfd; // プロセス間通信用ディスクリプタ
  int     conv;
  STR      cmd;
  ARG      std;
  ARG     args;
  ARG     argl;
  ARG     argc;
  ARG   putarg;
  ARG   getarg;
  MTNSVR  *svr;
  MTNPROCSTAT   *pstat;
  struct timeval start;
  struct {
    size_t buffsize;
    size_t datasize;
    char   *stdbuff;
  }stdout;
} MTNJOB; 

struct mtnstatus
{
  struct {
    size_t size;
    char  *buff;
  } members;
  struct {
    size_t size;
    char  *buff;
  } debuginfo;
  struct {
    size_t  size;
    char   *buff;
  } loglevel;
};

struct mtnmutex
{
  pthread_mutex_t member;
  pthread_mutex_t status;
  pthread_mutex_t loglevel;
};

struct mtncount
{
  int svr;
  int dir;
  int stat;
  int malloc;
};

struct mtnmembers
{
  MTNSVR *svr;
  struct timeval tv;
};

typedef struct mtn_context
{
  char     host[HOST_NAME_MAX];
  char     module_name[64];
  uint16_t max_packet_size;
  uint32_t max_open;
  uint16_t mcast_port;
  char     mcast_addr[INET_ADDRSTRLEN];
  MTNLOG   logmode;
  uint16_t loglevel;
  uint16_t logverbose;
  size_t  *sendsize;
  uint8_t **sendbuff;
  struct mtnmutex   mutex;
  struct mtncount   count;
  struct mtnstatus  status;
  struct mtnmembers members;
} MTN;

/*=========================================================================*/
int is_numeric(STR str);
int getpscount();
int getprocstat(MTNPROCSTAT *ps);
int getjobusage(MTNJOB *job);
int scanprocess(MTNJOB *job, int job_max);
int scheprocess(MTN *mtn, MTNJOB *job, int job_max, int cpu_lim, int cpu_num);
int job_close(MTNJOB *job);
int getwaittime(MTNJOB *job, int job_max);

/*-------------------------------------------------------------------------*/
void     mtnlogger(MTN *mtn, int l, char *fmt, ...);
void     mtndebug(const char *func, char *fmt, ...);

MTN     *mtn_init(const char *name);
void     mtn_destroy(MTN *mtn);
MTNSVR  *mtn_hello(MTN *mtn);
MTNSTAT *mtn_list(MTN *mtn, const char *path);
MTNSTAT *mtn_stat(MTN *mtn, const char *path);
MTNSVR  *mtn_info(MTN *mtn);
void     mtn_break(void);

int mtn_open(MTN *mtn, const char *path, int flags, MTNSTAT *st);
int mtn_open_file(MTN *mtn, int s, const char *path, int flags, MTNSTAT *st);
int mtn_close(MTN *mtn, int s);
int mtn_close_file(MTN *mtn, int s);
int mtn_get(MTN *mtn, int f, char *path);
int mtn_get_data(MTN *mtn, int s, int f);
int mtn_put(MTN *mtn, int f, char *path);
int mtn_put_data(MTN *mtn, int s, int f);
int mtn_fgetattr(MTN *mtn, int s, struct stat *st);
int mtn_fchown(MTN *mtn, int s, uid_t uid, gid_t gid);
int mtn_mkdir(MTN *mtn, const char *path, uid_t uid, gid_t gid);
int mtn_rm(MTN *mtn, const char *path);
int mtn_rename(MTN *mtn, const char *opath, const char *npath);
int mtn_symlink(MTN *mtn, const char *oldpath, const char *newpath);
int mtn_readlink(MTN *mtn, const char *path, char *buff, size_t size);
int mtn_chmod(MTN *mtn, const char *path, mode_t mode);
int mtn_chown(MTN *mtn, const char *path, uid_t uid, gid_t gid);
int mtn_utime(MTN *mtn, const char *path, time_t act, time_t mod);
int mtn_truncate(MTN *mtn, const char *path, off_t offset);
int mtn_read(MTN *mtn, int s, char *buf, size_t size, off_t offset);
int mtn_write(MTN *mtn, int s, const char *buf, size_t size, off_t offset);
int mtn_flush(MTN *mtn, int s);
int mtn_exec(MTN *mtn, MTNJOB *job);

MTNDIR  *newdir(const char *path);
MTNDIR  *deldir(MTNDIR *md);

MTNSTAT *newstat(const char *name);
MTNSTAT *delstat(MTNSTAT *mst);
MTNSTAT *cpstat(MTNSTAT *mst);
MTNSTAT *mgstat(MTNSTAT *mrt, MTNSTAT *mst);
void     clrstat(MTNSTAT *mst);

MTNSVR  *newsvr(void);
MTNSVR  *addsvr(MTNSVR *svr, MTNADDR *addr, char *host);
MTNSVR  *cpsvr(MTNSVR *svr);
MTNSVR  *getsvr(MTNSVR *svr, MTNADDR *addr);
MTNSVR  *delsvr(MTNSVR *svr);
MTNSVR  *clrsvr(MTNSVR *svr);
int      cmpsvr(MTNSVR *s1, MTNSVR *s2);

STR newstr(char *str);
STR modstr(STR str, char *n);
STR clrstr(STR str);
STR catstr(STR str1, STR str2);
ARG splitstr(STR str, STR delim);

ARG newarg(int c);
ARG addarg(ARG arg, STR str);
ARG clrarg(ARG args);
STR joinarg(ARG args);
ARG copyarg(ARG args);
STR poparg(ARG args);
STR convarg(STR arg, ARG argl);
ARG cmdargs(MTNJOB *job);

size_t set_mtnstatus_members(MTN *mtn);
size_t set_mtnstatus_debuginfo(MTN *mtn);
size_t set_mtnstatus_loglevel(MTN *mtn);
char *get_mtnstatus_members(MTN *mtn);
char *get_mtnstatus_debuginfo(MTN *mtn);
char *get_mtnstatus_loglevel(MTN *mtn);
void free_mtnstatus_members(char *buff);
void free_mtnstatus_debuginfo(char *buff);
void free_mtnstatus_loglevel(char *buff);

void mtndebug(const char *func, char *fmt, ...);
void mtndumparg(const char *func, ARG arg);

