
PYTHONSRC=$$HOME/projects/python

TEX_FILES = gfdl.tex introduction.tex  modules.tex  prog-zodb.tex  \
            storages.tex  transactions.tex  zeo.tex  zodb.tex 

MKHOWTO=$(PYTHONSRC)/Doc/tools/mkhowto

zodb.dvi: $(TEX_FILES)
	$(MKHOWTO) --dvi zodb.tex

html:
	-rm -rf zodb
	$(MKHOWTO) --html --iconserver=/python/writing/icons zodb.tex
#	rm -f /home/amk/www/zodb/guide/*
#	cp -p zodb/* /home/amk/www/zodb/guide/



