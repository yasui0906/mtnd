all: mtnfs.c mtntool.c mtnfs.h
	gcc -DMODULE_NAME=\"mtntool\" -o mtntool mtntool.c common.c
	gcc -DMODULE_NAME=\"mtnfs\"   -o mtnfs   mtnfs.c   common.c

clean:
	rm -f mtnfs mtntool *.o

sync:
	rsync -av --exclude='*.swp' --exclude='.git' ../mtnfs adc:LOCAL/
