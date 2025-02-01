ifeq (,$(BIBINPUTS))
  $(warning "BIBINPUTS not set")
endif

export BIBINPUTS := $(BIBINPUTS):$(CURDIR)

NAME = thesis
INPUTS = $(wildcard sections/*.tex includes/*.tex)
FIGS = $(wildcard figures/*[^~])
BIBS = $(wildcard *.bib)
RESULTS = $(wildcard benchmark/results/*.csv)
TEX_MACROS = ie,eg,verb,fi,iffalse,crefrange,linebreak
LATEXMK_FLAGS = -pdf

ifeq (1,$(FORCE))
  LATEXMK_FLAGS += -f
endif

.PHONY: all latexdiff cleanall clean clean-sources-zip textidote textidote-cli check

all: $(NAME).pdf

latexdiff: latexdiff.pdf

%.pdf: %.tex $(INPUTS) $(FIGS) $(BIBS) $(RESULTS)
	latexmk $(LATEXMK_FLAGS) $<

build/%.fls: %.pdf

cleanall: clean-sources-zip
	-rm -rf build $(patsubst %.tex,%.pdf,$(wildcard *.tex)) $(patsubst %.tex,%-textidote.html,$(wildcard *.tex))

clean:
	latexmk -c

sources.zip: build/$(NAME).fls $(NAME).tex $(INPUTS) $(FIGS) $(BIBS) $(RESULTS)
	zip $@ $(shell grep 'INPUT\s*\./' "$<" | grep -v '$(NAME)\.\(out\|aux\)' | sed -e 's/INPUT\s*//'  -e 's#//#/#' | sort -u) $(NAME).tex

clean-sources-zip:
	-rm -rf sources.zip

TEXTIDOTE_DICT = textidote-dict.txt
TEXTIDOTE_IGNORE = sh:008,sh:secskip,sh:seclen,sh:stacked,sh:nobreak,sh:nsubdiv

textidote: $(NAME)-textidote.html

textidote-cli: $(NAME).tex $(INPUTS) $(FIGS) $(BIBS) $(RESULTS) $(TEXTIDOTE_DICT)
	textidote --check en --firstlang de --dict $(TEXTIDOTE_DICT) --ignore $(TEXTIDOTE_IGNORE) --remove-macros "$(TEX_MACROS)" --type tex --output singleline $<

%-textidote.html: %.tex $(INPUTS) $(FIGS) $(BIBS) $(RESULTS) $(TEXTIDOTE_DICT)
	-textidote --check en --firstlang de --dict $(TEXTIDOTE_DICT) --ignore $(TEXTIDOTE_IGNORE) --remove-macros "$(TEX_MACROS)" --type tex --output html $< > $@

check: $(patsubst %.tex,%-textidote.html,$(wildcard *.tex))
