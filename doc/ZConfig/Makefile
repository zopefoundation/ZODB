# Rules to convert the documentation to a single PDF file.
#
# See the README.txt file for information on the mkhowto program used
# to generate the PDF and PostScript versions of the documentation.

.PHONY:	default all pdf ps

default:  pdf
all:	  pdf ps

pdf:	zconfig.pdf
ps:	zconfig.ps

zconfig.pdf:  zconfig.tex
	mkhowto --pdf $<

zconfig.ps:  zconfig.tex
	mkhowto --postscript $<
