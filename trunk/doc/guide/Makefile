
TEX_FILES = gfdl.tex introduction.tex  modules.tex  prog-zodb.tex  \
            storages.tex  transactions.tex  zeo.tex  zodb.tex 

MKHOWTO=mkhowto

all: pdf html

pdf: zodb.pdf

zodb.pdf: $(TEX_FILES)
	$(MKHOWTO) --pdf zodb.tex

zodb.dvi: $(TEX_FILES)
	$(MKHOWTO) --dvi zodb.tex

html: $(TEX_FILES)
	$(MKHOWTO) --html --dvips-safe --iconserver=. zodb.tex
