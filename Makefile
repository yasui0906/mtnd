all: mtnfs.c mtntool.c mtnfs.h libmtnfs.a libmtnfs fuse
	gcc -DMODULE_NAME=\"mtntool\" -o mtntool mtntool.c libmtnfs.a
	gcc -DMODULE_NAME=\"mtnfs\"   -o mtnfs   mtnfs.c   libmtnfs.a

libmtnfs: libmtnfs.c
	gcc -c libmtnfs.c
	ar r libmtnfs.a libmtnfs.o

fuse: mtnmount.c mtnfs.h
	gcc -DMODULE_NAME=\"mtnmount\" -lfuse -o mtnmount mtnmount.c libmtnfs.a

clean:
	rm -f mtnfs mtntool mtnmount *.o

sync:
	rsync -a *.c *.h Makefile adc:LOCAL/mtnfs/
	ssh adc "cd LOCAL/mtnfs/;make"

install:
	rsync -a mtnfs   /usr/klab/sbin/mtnfs
	rsync -a mtntool /usr/klab/sbin/mtntool
