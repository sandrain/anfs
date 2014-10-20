#
# Copyright © 2014	UT-Battelle, LLC. 
#			All rights reserved.
#
# See COPYING in top-level directory.
# 
# Additional copyrights may follow
# 
# $HEADER$
#

AC_INIT([anFS], [0.1], [an_email@domain.dom])
AC_CONFIG_SRCDIR([src/anfs_osd.c])

AM_INIT_AUTOMAKE([dist-bzip2 subdir-objects foreign tar-ustar -Wall -Werror])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_RANLIB
AC_PROG_MKDIR_P
AM_PROG_CC_C_O

CPPFLAGS="$CPPFLAGS -I$srcdir/../include"

# LIBOSD_CHECK_WITHDIR(with_option_name, dir_value, file_in_dir)
# ----------------------------------------------------
AC_DEFUN([LIBOSD_CHECK_WITHDIR],[
    AC_MSG_CHECKING([--with-$1 value])
    AS_IF([test "$2" = "yes" -o "$2" = "no" -o "x$2" = "x"],
          [AC_MSG_RESULT([simple ok (unspecified)])],
          [AS_IF([test ! -d "$2"],
                 [AC_MSG_RESULT([not found])
                  AC_MSG_WARN([Directory $2 not found])
                  AC_MSG_ERROR([Cannot continue])],
                 [AS_IF([test "x`ls $2/$3 2> /dev/null`" = "x"],
                        [AC_MSG_RESULT([not found])
                         AC_MSG_WARN([Expected file $2/$3 not found])
                         AC_MSG_ERROR([Cannot continue])],
                        [AC_MSG_RESULT([sanity check ok ($2)])]
                       )
                 ]
                )
          ]
         )
])dnl

AC_ARG_WITH([libosd], [AS_HELP_STRING([--with-libosd=DIR],
            [Specify the location of the libosd installation.])])
LIBOSD_CHECK_WITHDIR([libosd],[$with_libosd], [include/open-osd/libosd.h])

AS_IF([test ! -z "$with_libosd" -a "$with_libosd" != "yes"],
      [libosd_dir="$with_libosd"])

CPPFLAGS_save="$CPPFLAGS"
AS_IF([test ! -z "$libosd_dir"],
      AC_SUBST([CPPFLAGS],["$CPPFLAGS_save -I$libosd_dir/include -I$libosd_dir/include/open-osd"]))
LDFLAGS_save="$LDFLAGS"
AS_IF([test ! -z "$libosd_dir"],
      AC_SUBST([LDFLAGS],(["$LDFLAGS_save -L$libosd_dir/lib -Wl,-rpath,$libosd_dir/lib"])))

AC_CHECK_LIB([osd], [osd_open], , AC_MSG_ERROR([Unable to locate libosd installation]))

# Check for some additional packages
# ----------------------------------

PKG_CHECK_MODULES(FUSE, fuse, , AC_MSG_ERROR('Unable to find libfuse. Please make sure library and header files are installed.'))
PKG_CHECK_MODULES(SQLITE3, sqlite3, , AC_MSG_ERROR('Unable to find sqlite3. Please make sure library and header files are installed.'))
PKG_CHECK_MODULES(LIBCONFIG, libconfig, , AC_MSG_ERROR('Unable to find libconfig. Please make sure library and header files are installed.'))

# Some required kernel headers
# ----------------------------
#AC_DEFUN([LINUX_HEADERS_WITHDIR],[
#	AC_MSG_CHECKING([--with-$1 value])
#	AS_IF([test "$2" = "yes" -o "$2" = "no" -o "x$2" = "x"],
#	 [AC_MSG_RESULT([simple ok (unspecified)])],
#          [AS_IF([test ! -d "$2"],
#                 [AC_MSG_RESULT([not found])
#                  AC_MSG_WARN([Directory $2 not found])
#                  AC_MSG_ERROR([Cannot continue])],
#                 [AS_IF([test "x`ls $2/$3 2> /dev/null`" = "x"],
#                        [AC_MSG_RESULT([not found])
#                         AC_MSG_WARN([Expected file $2/$3 not found])
#                         AC_MSG_ERROR([Cannot continue])],
#                        [AC_MSG_RESULT([sanity check ok ($2)])]
#                       )
#                 ]
#                )
#          ]
#         )
#])dnl
#AC_ARG_WITH([linux-headers], [AS_HELP_STRING([--with-linux-kernel=DIR],
#	[Blablabla.])])
#LINUX_HEADERS_WITHDIR([linux-headers],[$with_linux_headers],[include/asm/unaligned.h])
#
#maybe_linux_headers_dir="/usr/src/linux-headers"
#AS_IF([test ! -z "$with_linux_headers" -a "$with_linux_headers" != "yes"],
#	[LINUX_HEADERS_DIR=$with_linux_headers],[LINUX_HEADERS_DIR=$maybe_linux_headers_dir])
#AC_MSG_WARN([Test: $LINUX_HEADERS_DIR])
#AC_CHECK_FILE([$LINUX_HEADERS_DIR/include/asm/unaligned.h],
#	[CPPFLAGS_save="${CPPFLAGS}"
#		AC_SUBST([CPPFLAGS],["-I$LINUX_HEADERS_DIR/include $CPPFLAGS_save"])
#		AC_MSG_WARN([cppflags: $CCPFLAGS])
#	]
#)

# Add some default flags
# ----------------------

CPPFLAGS_save="$CPPFLAGS"
AC_SUBST([CPPFLAGS],["$CPPFLAGS_save -fPIC -foptimize-sibling-calls -D_GNU_SOURCE -D_BSD_SOURCE"])

AC_CONFIG_FILES([Makefile src/Makefile])

AC_OUTPUT

