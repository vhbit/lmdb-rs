LMDB_ROOT ?= deps/mdb/libraries/liblmdb

RUSTC ?= rustc
RUSTDOC ?= rustdoc
RUST_LIB_FLAGS = -L $(LMDB_ROOT)
RUSTC_FLAGS = $(RUST_LIB_FLAGS) -g

SRC = $(wildcard src/*.rs)
CRATE_MAIN = src/lib.rs
BUILD_DIR ?= build
CRATE_NAME = $(RUSTC)

all: mdb lib tests doc

mdb:
	cd $(LMDB_ROOT) && make liblmdb.a

lib: mdb $(SRC)
	$(RUSTC) $(RUSTC_FLAGS) --out-dir $(BUILD_DIR) $(CRATE_MAIN)

doc: $(SRC)
	mkdir -p doc
	$(RUSTDOC) -o doc $(CRATE_MAIN)

tests: mdb $(SRC)
	mkdir -p $(BUILD_DIR)
	$(RUSTC) $(RUSTC_FLAGS) --test $(CRATE_MAIN) -o $(BUILD_DIR)/test_runner
	@echo "=============================================="
	$(BUILD_DIR)/test_runner
	@echo "=============================================="

clean:
	cd $(LMDB_ROOT) && make clean
	rm -f *.a *.rlib test_runner
