EXTENSION    = pg_qualstats
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
DOCS         = $(wildcard doc/*.md)
MODULES      = $(patsubst %.c,%,$(wildcard *.c))
PG_CONFIG    = pg_config

all:

release-zip: all
	git archive --format zip --prefix=pg_qualstats-$(EXTVERSION)/ --output ./pg_qualstats-$(EXTVERSION).zip HEAD
	unzip ./pg_qualstats-$(EXTVERSION).zip
	rm ./pg_qualstats-$(EXTVERSION).zip
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./pg_qualstats-$(EXTVERSION)/META.json
	zip -r ./pg_qualstats-$(EXTVERSION).zip ./pg_qualstats-$(EXTVERSION)/
	rm ./pg_qualstats-$(EXTVERSION) -rf


DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
