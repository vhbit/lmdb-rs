LOCAL_PATH := $(call my-dir)

include $(CLEAR_VARS)
LOCAL_MODULE := liblmdb
LOCAL_SRC_FILES += \
      deps/mdb/libraries/liblmdb/mdb.c \
      deps/mdb/libraries/liblmdb/midl.c

LOCAL_CFLAGS := -std=gnu99 -DMDB_DSYNC=O_SYNC
LOCAL_EXPORT_CFLAGS += -I$(LOCAL_PATH)
include $(BUILD_STATIC_LIBRARY)
