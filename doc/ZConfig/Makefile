# Rules to convert the documentation to a single PDF file.
#
# PostScript and HTML output are also supported, though PDF is the
# default.
#
# See the README.txt file for information on the mkhowto program used
# to generate the PDF and PostScript versions of the documentation.

.PHONY:	default all html pdf ps text

default:  pdf
all:	  html pdf ps text

html:   zconfig/zconfig.html
pdf:	zconfig.pdf
ps:	zconfig.ps
text:   zconfig.txt

zconfig/zconfig.html:  zconfig.tex schema.dtd xmlmarkup.perl
	mkhowto --html $<

zconfig.pdf:  zconfig.tex schema.dtd xmlmarkup.sty
	mkhowto --pdf $<

zconfig.ps:  zconfig.tex schema.dtd xmlmarkup.sty
	mkhowto --postscript $<

zconfig.txt: zconfig.tex schema.dtd xmlmarkup.sty
	mkhowto --text $<

clean:
	rm -f zconfig.l2h zconfig.l2h~

clobber:  clean
	rm -f zconfig.pdf zconfig.ps zconfig.txt
	rm -rf zconfig
