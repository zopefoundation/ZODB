##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

# Rules to convert the documentation to a single PDF file.
#
# PostScript, HTML, and plain text output are also supported, though
# PDF is the default.
#
# See the README.txt file for information on the mkhowto program used
# to generate the formatted versions of the documentation.

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
