#######################################################################
# Generic make script for compiling erlang code                       #
# The environment variable $ERLHOME has to be set to where erlang/OTP #
# is installed                                                        #
# Compiles the code into a ebin dir. relative to the source dir.      #
####################################################################### 
#Compiles the code into a ebin dir. relative to the source dir. 
include vsn.mk

ERLC    := erlc
ERL     := erl
GEN     := beam

EFLAGS := +debug_info
INCLUDE := include
EBIN    := ebin
EUNIT_EBIN := eunit/ebin
EUNIT_INCLUDE := eunit/include
SRC     := $(wildcard src/*.erl)
HEADERS := $(wildcard $(INCLUDE)/*.hrl)
CODE    := $(patsubst src/%.erl, ebin/%.beam, $(SRC))
DOT_REL_SRC  := $(wildcard ./*.rel.src)
DOT_APP_SRC  := $(wildcard src/*.app.src)
DOTAPP  := $(patsubst src/%.app.src, ebin/%.app, $(DOT_APP_SRC))
DOTREL  := $(patsubst ./%.rel.src, ./%.rel, $(DOT_REL_SRC))
RELBASE := $(notdir $(basename $(DOTREL)))


.PHONY: clean all

$(EBIN)/%.beam: src/%.erl
	$(ERLC) -I$(INCLUDE)  -I$(EUNIT_INCLUDE) -W -pz $(EUNIT_EBIN) -b beam -o $(EBIN) $(EFLAGS) $(WAIT) $<

all: $(CODE) $(DOTAPP) $(DOTREL)

$(DOTAPP): $(DOT_APP_SRC)
	echo "created $(DOTAPP)"
	@sed 's/%VSN%/$(VSN)/g' <$(DOT_APP_SRC) >$(DOTAPP)

clean:
	rm -f $(EBIN)/* 

