/*
 * libmtnfs.h
 * Copyright (C) 2011 KLab Inc.
 */
#define PROTOCOL_VERSION 1
#define MTN_MAX_DATASIZE 32768
#define MTN_TCP_BUFFSIZE (1024 * 1024)

#define MTNCMD_STARTUP   0
#define MTNCMD_SHUTDOWN  1
#define MTNCMD_HELLO     2
#define MTNCMD_INFO      3
#define MTNCMD_STAT      4
#define MTNCMD_LIST      5
#define MTNCMD_PUT       6
#define MTNCMD_GET       7
#define MTNCMD_DEL       8
#define MTNCMD_DATA      9
#define MTNCMD_OPEN     10
#define MTNCMD_READ     11
#define MTNCMD_WRITE    12
#define MTNCMD_CLOSE    13
#define MTNCMD_TRUNCATE 14
#define MTNCMD_MKDIR    15
#define MTNCMD_RMDIR    16
#define MTNCMD_UNLINK   17
#define MTNCMD_RENAME   18
#define MTNCMD_CHMOD    19
#define MTNCMD_CHOWN    20
#define MTNCMD_GETATTR  21
#define MTNCMD_SETATTR  22
#define MTNCMD_SYMLINK  23
#define MTNCMD_READLINK 24
#define MTNCMD_UTIME    25
#define MTNCMD_RESULT   26
#define MTNCMD_CONNECT  27
#define MTNCMD_EXEC     28
#define MTNCMD_STDIN    29
#define MTNCMD_STDOUT   30
#define MTNCMD_STDERR   31
#define MTNCMD_ERROR    97
#define MTNCMD_SUCCESS  98
#define MTNCMD_MAX      99

typedef struct meminfo
{
  uint64_t   vsz;
  uint64_t   res;
  uint64_t share;
  uint64_t  text;
  uint64_t  data;
  long page_size;
} meminfo;

typedef struct
{
  uint8_t  ver;
  uint8_t  fin;
  uint8_t  type;
  uint8_t  flag;
  uint16_t sqno;
  uint16_t size;
}__attribute__((packed)) MTNHEAD;

typedef struct kdata
{
  MTNHEAD head;
  union {
    uint8_t  data8;
    uint16_t data16;
    uint32_t data32;
    uint64_t data64;
    uint8_t  data[MTN_MAX_DATASIZE];
  } data;
  void   *option;
  uint32_t opt32;
  uint64_t opt64;
}__attribute__((packed)) MTNDATA;

typedef struct mtntask
{
  int      fd;
  int     con;
  int     res;
  int  std[3];
  pid_t   pid;
  DIR    *dir;
  uint8_t type;
  uint8_t fin;
  uint8_t create;
  struct  timeval tv;
  struct  stat  stat;
  MTNADDR addr;
  MTNDATA send;
  MTNDATA recv;
  MTNDATA keep;
  MTNSTAT *kst;
  struct mtntask *prev;
  struct mtntask *next;
  char  path[PATH_MAX];
}__attribute__((packed)) MTNTASK;

extern char *mtncmdstr[];

MTNTASK *newtask();
MTNTASK *cuttask(MTNTASK *t);
MTNTASK *deltask(MTNTASK *t);
int create_lsocket(MTN *mtn);
int create_msocket(MTN *mtn);
int cmpaddr(MTNADDR *a1, MTNADDR *a2);
int mtn_get_svrhost(MTNSVR *svr, MTNDATA *kd);
int mtn_get_string(char *str, MTNDATA *kd);
int mtn_get_int(void *val, MTNDATA *kd, int size);
int mtn_get_stat(struct stat *st, MTNDATA *kd);
int mtn_set_string(char *str, MTNDATA *kd);
int mtn_set_int(void *val, MTNDATA *kd, int size);
int mtn_set_stat(struct stat *st, MTNDATA *kd);
int mtn_set_data(void *buff, MTNDATA *kd, size_t size);
uint32_t get_members_count(MTNSVR *mb);
int get_meminfo(meminfo *m);
int malloccnt(void);
int send_dgram(MTN *mtn, int s, MTNDATA *data, MTNADDR *addr);
int recv_dgram(MTN *mtn, int s, MTNDATA *data, struct sockaddr *addr, socklen_t *alen);
int send_data_stream(MTN *mtn, int s, MTNDATA *data);
int recv_data_stream(MTN *mtn, int s, MTNDATA *kd);
int send_recv_stream(MTN *mtn, int s, MTNDATA *sd, MTNDATA *rd);

char *v4addr(MTNADDR *addr, char *buff, socklen_t size);
int v4port(MTNADDR *addr);
void mtn_startup(MTN *mtn, int f);
void mtn_shutdown(MTN *mtn);

