all: lib mtnd fuse tool test

mtnd: mtnd.c lib
	gcc -DMODULE_NAME=\"mtnd\" -o mtnd mtnd.c libmtnfs.a

fuse: mtnmount.c lib
	gcc -DMODULE_NAME=\"mtnmount\" -lfuse -o mtnmount mtnmount.c libmtnfs.a -I/usr/klab/app/fuse/include -L/usr/klab/app/fuse/lib

tool: mtntool.c lib
	gcc -DMODULE_NAME=\"mtntool\" -o mtntool mtntool.c libmtnfs.a

lib: libmtnfs.c common.c mtnfs.h common.h
	gcc -c common.c
	gcc -c libmtnfs.c
	ar r libmtnfs.a libmtnfs.o common.o

test: mtntest.c mtnd fuse
	gcc -DMODULE_NAME=\"mtntest\" -o mtntest mtntest.c libmtnfs.a

clean:
	rm -f mtnd mtntool mtnmount libmtnfs.a mtntest *.o

sync:
	rsync -a *.c *.h Makefile adc:LOCAL/mtnfs/
	ssh adc "cd LOCAL/mtnfs/;make"

install:
	rsync -a mtnd     /usr/klab/sbin/mtnd
	rsync -a mtnmount /usr/klab/sbin/mtnmount
	rsync -a mtntool  /usr/klab/sbin/mtntool
	chown root:root   /usr/klab/sbin/mtnd
	chown root:root   /usr/klab/sbin/mtnmount
	chown root:root   /usr/klab/sbin/mtntool
