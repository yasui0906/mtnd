#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(mtnd, 0.9.9, yasui0906@gmail.com)
AC_CONFIG_SRCDIR([mtn.h])
AC_CONFIG_HEADER([config.h])
AM_INIT_AUTOMAKE

# Checks for programs.
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_RANLIB

# Checks for libraries.

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS([arpa/inet.h fcntl.h limits.h netinet/in.h stddef.h stdint.h stdlib.h string.h sys/socket.h sys/statvfs.h sys/time.h syslog.h unistd.h utime.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_UID_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_STRUCT_ST_BLOCKS
AC_HEADER_TIME
AC_STRUCT_TM
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_CHOWN
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_FORK
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC
AC_FUNC_STAT
AC_FUNC_UTIME_NULL
AC_CHECK_FUNCS([dup2 getcwd gethostname gettimeofday memmove memset mkdir rmdir socket strerror utime])

AC_ARG_ENABLE(debug,
  AS_HELP_STRING([--enable-debug],[compile with debugging flags]),
  [],
  [enable_debug=no]
)
AM_CONDITIONAL([ENABLE_DEBUG], [test x$enable_debug = xyes])

AC_ARG_ENABLE(mtnfs,
  AS_HELP_STRING([--disable-mtnfs],[mtnfs disable]),
  [],
  [enable_mtnfs=yes]
)
AM_CONDITIONAL([ENABLE_MTNFS], [test x$enable_mtnfs = xyes])

LIBFUSEPATH="/usr"
AC_ARG_WITH(libfuse,
AC_HELP_STRING([--with-libfuse=PATH],[path to libfuse installation]),
  [
    LIBFUSEPATH="$withval"
    FUSE_LDFLAGS="-L$LIBFUSEPATH/lib -Wl,-rpath -Wl,$LIBFUSEPATH/lib $LDFLAGS"
    FUSE_CFLAGS="-I$LIBFUSEPATH/include $CFLAGS"
  ]
)
AC_SUBST(FUSE_LDFLAGS)
AC_SUBST(FUSE_CFLAGS)
AC_CONFIG_FILES([Makefile])
AC_OUTPUT

