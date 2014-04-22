LMDB_ROOT ?= deps/mdb/libraries/liblmdb

RUSTC ?= rustc
RUSTDOC ?= rustdoc
RUST_LIB_FLAGS = -L $(LMDB_ROOT)
RUSTC_FLAGS = $(RUST_LIB_FLAGS)

SRC = $(wildcard src/*.rs)
CRATE_MAIN = src/lib.rs

all: mdb lib tests doc

mdb:
	cd $(LMDB_ROOT) && make liblmdb.a

lib: $(SRC)
	$(RUSTC) $(RUSTC_FLAGS) $(CRATE_MAIN)

doc: $(SRC)
	mkdir -p doc
	$(RUSTDOC) -o doc $(CRATE_MAIN)

tests: $(SRC)
	$(RUSTC) $(RUSTC_FLAGS) --test $(CRATE_MAIN) -o test_runner
	@echo "=============================================="
	./test_runner
	@echo "=============================================="

clean:
	cd $(LMDB_ROOT) && make clean
	rm -f *.a *.rlib test_runner
