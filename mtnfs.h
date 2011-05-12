/*
 * mtnfs.h
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#define PACKAGE_VERSION "0.1"
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
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>

#define MTNRES_SUCCESS 0
#define MTNRES_ERROR   1
#define MTNCMD_NONE    0
#define MTNCMD_INFO    1
#define MTNCMD_LIST    2
#define MTNCMD_SAVE    3
#define MTNCMD_LOAD    4
#define MTNCMD_DATA    9

#define PROTOCOL_VERSION 1
#define MAX_CONNECTION   8
#define MAX_DATASIZE 32768

typedef struct
{
  uint8_t  ver;
  uint8_t  type;
  uint16_t size;
}__attribute__((packed)) khead;

typedef struct
{
  uint8_t *host;
  uint64_t free;
}__attribute__((packed)) kinfo;

typedef struct
{
  khead head;
  union {
    kinfo   info;
    uint8_t data[MAX_DATASIZE];
  } data;
}__attribute__((packed)) kdata;

typedef struct
{
  int fd;
  int type;
  int socket;
  int h_size;
  int d_size;
  kdata data;
  union {
    uint8_t file_name[PATH_MAX];
  } opt;
}__attribute__((packed)) kstream;

typedef struct
{
  uint16_t max_packet_size;               // 1パケットに格納する最大サイズ
  uint64_t diskfree;                      // 空容量
  uint64_t datasize;                      // 使用量
  uint16_t mcast_port;                    //
  uint8_t  mcast_addr[16];                //
  uint8_t  host[64];                      //
  int      accept_socket[MAX_CONNECTION]; //
}__attribute__((packed)) koption;

extern int is_loop;
extern koption kopt;
void lprintf(int l, char *fmt, ...);
void kinit_option();
int send_readywait(int s);
int create_socket(int port, int mode);
int create_lsocket(int port);
int create_msocket(int port);

