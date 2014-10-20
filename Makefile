LMDB_ROOT ?= deps/mdb/libraries/liblmdb

RUSTC ?= rustc
RUSTDOC ?= rustdoc
RUST_LIB_FLAGS = -L $(LMDB_ROOT)
RUSTC_FLAGS = $(RUST_LIB_FLAGS) -g

SRC = $(wildcard src/*.rs)
CRATE_MAIN = src/lib.rs
BUILD_DIR ?= build
LIBOUT = $(BUILD_DIR)/$(shell rustc --crate-file-name src/lib.rs)
TEST_RUNNER = $(BUILD_DIR)/test_runner

ifeq ($(TARGET),i386-apple-ios)
	CFG_SDK = $(shell xcrun --show-sdk-path -sdk iphonesimulator 2>/dev/null)
	CFG_FLAGS = -target i386-apple-ios -isysroot $(CFG_SDK) -mios-simulator-version-min=7.0
	XCFLAGS = $(CFG_FLAGS)
endif

ifeq ($(TARGET),arm-apple-ios)
	CFG_SDK = $(shell xcrun --show-sdk-path -sdk iphoneos 2>/dev/null)
	CFG_FLAGS = -arch armv7 -target arm-apple-ios -isysroot $(CFG_SDK) -mios-version-min=7.0
	XCFLAGS = $(CFG_FLAGS)
endif

.PHONY: all mdb lib doc tests clean

all: mdb lib tests doc

mdb:
	@git submodule update --init --recursive
	make -C $(LMDB_ROOT) clean
	XCFLAGS="$(XCFLAGS)" make -C $(LMDB_ROOT) liblmdb.a

mdb_for_cargo: mdb
	@echo "Target is $(TARGET)"
	@mkdir -p $(DEPS_DIR)
	@cp $(LMDB_ROOT)/liblmdb.a $(DEPS_DIR)

$(LIBOUT): $(SRC)
	@mkdir -p $(BUILD_DIR)
	$(RUSTC) $(RUSTC_FLAGS) --out-dir $(BUILD_DIR) $(CRATE_MAIN)

lib: mdb $(LIBOUT)

doc: $(SRC)
	@mkdir -p doc
	$(RUSTDOC) -o doc $(CRATE_MAIN)

tests: $(TEST_RUNNER)
	@echo "=============================================="
	$<
	@echo "=============================================="

$(TEST_RUNNER): $(SRC) mdb
	@mkdir -p $(BUILD_DIR)
	$(RUSTC) $(RUSTC_FLAGS) --test $(CRATE_MAIN) -o $@

clean:
	cd $(LMDB_ROOT) && make clean
	rm -f *.a *.rlib test_runner
	rm -rf doc $(BUILD_DIR)
