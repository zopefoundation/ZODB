# Rules to convert the documentation to a single PDF file.

.PHONY:	default all pdf ps

default:  pdf
all:	  pdf ps

pdf:	zconfig.pdf
ps:	zconfig.ps

zconfig.pdf:  zconfig.tex
	mkhowto --pdf $<

zconfig.ps:  zconfig.tex
	mkhowto --postscript $<
