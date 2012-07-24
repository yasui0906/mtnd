/*
 * libmtnfs.h
 * Copyright (C) 2011 KLab Inc.
 */
#define PROTOCOL_VERSION 1
#define MTN_MAX_DATASIZE 32768
#define MTN_TCP_BUFFSIZE (1024 * 1024)
#define MTN_DEFAULT_ADDR "224.1.0.110"
#define MTN_DEFAULT_PORT 6000

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
#define MTNCMD_INIT     27
#define MTNCMD_EXIT     28
#define MTNCMD_EXEC     29
#define MTNCMD_STDIN    30
#define MTNCMD_STDOUT   31
#define MTNCMD_STDERR   32
#define MTNCMD_ERROR    97
#define MTNCMD_SUCCESS  98
#define MTNCMD_MAX      99

#define MTNCOUNT_TASK   0
#define MTNCOUNT_SAVE   1
#define MTNCOUNT_SVR    2
#define MTNCOUNT_DIR    3
#define MTNCOUNT_STAT   4
#define MTNCOUNT_STR    5
#define MTNCOUNT_ARG    6
#define MTNCOUNT_MALLOC 7
#define MTNCOUNT_MPS    8
#define MTNCOUNT_MAX    9

#define MTNMODE_EXPORT  1
#define MTNMODE_EXECUTE 2

typedef struct statm
{
  uint64_t   vsz;
  uint64_t   res;
  uint64_t share;
  uint64_t  text;
  uint64_t  data;
  long page_size;
} statm;

typedef struct
{
  int     init;
  int     mode;
  uid_t    uid;
  gid_t    gid;
  uint64_t use;
} MTNINIT;

typedef struct
{
  uint8_t  ver;  // プロトコルバージョン
  uint8_t  fin;  // 処理が完了したら1になる
  uint8_t  type; // MTNCMD_* をセットする
  uint8_t  flag; // 連続してデータを送信したいときに1をセットする
  uint16_t sqno; // シーケンス番号
  uint16_t size; // データサイズ
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
  int       fd;       // ファイル操作用ディスクリプタ
  int      rpp;       // プロセス間通信用ディスクリプタ
  int      wpp;       // プロセス間通信用ディスクリプタ
  int      con;       // クライアント通信用ソケット
  int   std[3];       //
  int      res;       //
  pid_t    pid;       //
  DIR     *dir;       //
  uint8_t  fin;       //
  uint8_t type;       //
  uint8_t create;     //
  struct  timeval tv; //
  struct  stat  stat; //
  MTNINIT init;
  MTNADDR addr;
  MTNDATA send;
  MTNDATA recv;
  MTNDATA keep;
  MTNSTAT *kst;
  struct mtntask *prev;
  struct mtntask *next;
  char  path[PATH_MAX];
  char  work[PATH_MAX];
}__attribute__((packed)) MTNTASK;

typedef struct mtnsavetask
{
  uint16_t sqno;
  MTNADDR  addr;
  struct timeval tv;
  struct mtnsavetask *prev;
  struct mtnsavetask *next;
}__attribute__((packed)) MTNSAVETASK;

extern char *mtncmdstr[];

MTNSVR *mtn_choose(MTN *mtn);
MTNTASK *newtask();
MTNTASK *cuttask(MTNTASK *t);
MTNTASK *deltask(MTNTASK *t);
MTNSAVETASK *newsavetask(MTNTASK *t);
MTNSAVETASK *cutsavetask(MTNSAVETASK *t);
MTNSAVETASK *delsavetask(MTNSAVETASK *t);
int create_lsocket(MTN *mtn);
int create_msocket(MTN *mtn);
int cmpaddr(MTNADDR *a1, MTNADDR *a2);
int mtndata_get_svrhost(MTNSVR *svr, MTNDATA *kd);
int mtndata_get_string(char *str, MTNDATA *kd);
int mtndata_get_int(void *val, MTNDATA *kd, int size);
int mtndata_get_stat(struct stat *st, MTNDATA *kd);
int mtndata_set_string(char *str, MTNDATA *kd);
int mtndata_set_int(void *val, MTNDATA *kd, int size);
int mtndata_set_stat(struct stat *st, MTNDATA *kd);
int mtndata_set_data(void *buff, MTNDATA *kd, size_t size);
uint32_t get_task_count(MTNTASK *kt);
uint32_t get_members_count(MTNSVR *mb);
int getstatm(statm *m);
int getmeminfo(uint64_t *size, uint64_t *free);
int getcount(int id);
int send_dgram(MTN *mtn, int s, MTNDATA *data, MTNADDR *addr);
int recv_dgram(MTN *mtn, int s, MTNDATA *data, struct sockaddr *addr, socklen_t *alen);
int send_data_stream(MTN *mtn, int s, MTNDATA *data);
int recv_data_stream(MTN *mtn, int s, MTNDATA *kd);
int send_recv_stream(MTN *mtn, int s, MTNDATA *sd, MTNDATA *rd);

char *get_mode_string(mode_t mode);
char *v4addr(MTNADDR *addr, char *buff);
int  v4port(MTNADDR *addr);
char *v4apstr(MTNADDR *addr);
void mtn_startup(MTN *mtn, int f);
void mtn_shutdown(MTN *mtn);
