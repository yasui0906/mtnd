/*
 * mtnfs.h
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#define PACKAGE_VERSION "0.5"
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <limits.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <utime.h>
#include <errno.h>
#include <signal.h>
#include <getopt.h>
#include <syslog.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <sys/syscall.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <dirent.h>
#include <libgen.h>
#include <pwd.h>
#include <grp.h>
#include <pthread.h>

#define MTNRES_SUCCESS   0
#define MTNRES_ERROR     1
#define MTNCMD_NONE      0
#define MTNCMD_HELLO     1
#define MTNCMD_INFO      2
#define MTNCMD_STAT      3
#define MTNCMD_LIST      4
#define MTNCMD_SET       5
#define MTNCMD_GET       6
#define MTNCMD_DEL       7
#define MTNCMD_DATA      8
#define MTNCMD_OPEN      9
#define MTNCMD_READ     10
#define MTNCMD_WRITE    11
#define MTNCMD_CLOSE    12
#define MTNCMD_TRUNCATE 13
#define MTNCMD_MKDIR    14
#define MTNCMD_RMDIR    15
#define MTNCMD_UNLINK   16
#define MTNCMD_RENAME   17
#define MTNCMD_CHMOD    18
#define MTNCMD_CHOWN    19
#define MTNCMD_GETATTR  20
#define MTNCMD_SETATTR  21
#define MTNCMD_MAX      99

#define MTNTYPE_STRING   1
#define MTNTYPE_UINT8    2
#define MTNTYPE_UINT16   3
#define MTNTYPE_UINT32   4
#define MTNTYPE_UINT64   5

#define PROTOCOL_VERSION 1
#define MAX_DATASIZE 32768

typedef struct
{
  uint8_t  ver;
  uint8_t  fin;
  uint8_t  type;
  uint8_t  dummy;
  uint16_t size;
}__attribute__((packed)) khead;

typedef struct kdata
{
  khead head;
  union {
    uint8_t  data8;
    uint16_t data16;
    uint32_t data32;
    uint64_t data64;
    uint8_t  data[MAX_DATASIZE];
  } data;
  void *option;
}__attribute__((packed)) kdata;

typedef struct kaddr
{
  socklen_t len;
  union {
    struct sockaddr addr;
    struct sockaddr_in in;
    struct sockaddr_storage storage;
  } addr;
}__attribute__((packed)) kaddr;


typedef struct kmember
{
  kaddr    addr;
  uint8_t *host;
  uint8_t  mark;
  uint32_t free;
  struct kmember *next;
}__attribute__((packed)) kmember;

typedef struct kstat
{
  uint8_t        *name;
  struct kstat   *prev;
  struct kstat   *next;
  struct stat     stat;
  struct kmember *member;
}__attribute__((packed)) kstat;

typedef struct ktask
{
  int     fd;
  int     con;
  int     res;
  DIR    *dir;
  uint8_t type;
  uint8_t fin;
  uint8_t create;
  struct  stat  stat;
  struct  kaddr addr;
  struct  kdata send;
  struct  kdata recv;
  struct  kstat *kst;
  struct  ktask *prev;
  struct  ktask *next;
  uint8_t path[PATH_MAX];
}__attribute__((packed)) ktask;

typedef struct kdir
{
  char path[PATH_MAX];
  struct timeval tv;
  struct kstat *kst;
  struct kdir *prev;
  struct kdir *next;
}__attribute__((packed)) kdir;

typedef struct
{
  uint16_t max_packet_size; // 1パケットに格納する最大サイズ
  uint32_t freesize;        // 空容量
  uint32_t datasize;        // 使用量
  uint32_t limitsize;       //
  uint32_t debuglevel;      //
  uint16_t mcast_port;      //
  uint8_t  mcast_addr[16];  //
  uint8_t  host[64];        //
  uint8_t *cwd;             //
  uint8_t  field_size[4];   // 表示幅
  ktask   *task;            // 積みタスク
  kdir    *dircache;        // ディレクトリキャッシュ
}__attribute__((packed)) koption;


extern int is_loop;
extern koption kopt;
extern kmember *members;
void lprintf(int l, char *fmt, ...);
void kinit_option();
char *mtn_get_v4addr(kaddr *addr);
kstat *mtn_stat(const char *path);
kstat *mtn_list(const char *path);
kdir *mkkdir(const char *, kstat *, kdir *);
int mtn_open(const char *, int, mode_t);
int mtn_close(int);
int recv_stream(int, void *, size_t);
int recv_data_stream(int, kdata *);
int send_data_stream(int, kdata *);
kmember *mtn_choose(const char *);
kstat *mtn_find(const char *, int);
int mtn_set_int(void *, kdata *, int);
int mtn_get_int(void *, kdata *, int);
void dirbase(const char *, char *, char *);
/*
int send_readywait(int s);
int create_socket(int port, int mode);
int create_lsocket(int port);
int create_msocket(int port);
*/

