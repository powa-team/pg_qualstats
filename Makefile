EXTENSION    = pg_qualstats
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
DOCS         = $(wildcard doc/*.md)
MODULES      = $(patsubst %.c,%,$(wildcard *.c))
PG_CONFIG    = pg_config

all: $(EXTENSION)--$(EXTVERSION).sql

$(EXTENSION)--$(EXTVERSION).sql: $(EXTENSION).sql
	cp $< $@


DATA = $(wildcard *--*.sql) $(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = $(EXTENSION)--$(EXTVERSION).sql
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
