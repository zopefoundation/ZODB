# Rules to convert the documentation to a single PDF file.

.PHONY:	default all pdf ps

default:  pdf
all:	  pdf ps

pdf:	zconfig.pdf
ps:	zconfig.ps

zconfig.pdf:  zconfig.tex
	/home/srichter/Python/python-head/dist/src/Doc/tools/mkhowto --pdf  $<

zconfig.ps:  zconfig.tex
	/home/srichter/Python/python-head/dist/src/Doc/tools/mkhowto --postscript $<
