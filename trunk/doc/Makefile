MKHOWTO=mkhowto

MKHTML=$(MKHOWTO) --html --iconserver=. --split=4 --dvips-safe

ZODBTEX = guide/gfdl.tex guide/introduction.tex guide/modules.tex \
	  guide/prog-zodb.tex guide/storages.tex guide/transactions.tex \
	  guide/zeo.tex guide/zodb.tex 

default: pdf
all:	 pdf ps html

pdf:	storage.pdf zodb.pdf
ps:	storage.ps zodb.ps

html:	storage/storage.html zodb/zodb.html

storage.pdf: storage.tex
	$(MKHOWTO) --pdf $<

storage.ps: storage.tex
	$(MKHOWTO) --ps $<

storage/storage.html: storage.tex
	$(MKHTML) storage.tex

zodb.pdf: $(ZODBTEX)
	$(MKHOWTO) --pdf guide/zodb.tex

zodb.ps: $(ZODBTEX)
	$(MKHOWTO) --ps guide/zodb.tex

zodb/zodb.html: $(ZODBTEX)
	$(MKHTML) guide/zodb.tex

clobber:
	rm -rf storage.pdf storage.ps storage/ zodb.pdf zodb.ps zodb/
