lmdb-rs
=======

Rust bindings for [LMDB](http://symas.com/mdb/)

Basic functionality like creating/opening environment and use
transactions, cursors works.

[![Build status (master)](https://travis-ci.org/vhbit/lmdb-rs.svg?branch=master)](https://travis-ci.org/vhbit/lmdb-rs)
[Documentation (master branch)](http://vhbit.github.io/lmdb-rs/lmdb-rs/)


Building
========

LMDB is bundled as submodule so update submodules first:

`git submodule update --init`

And then

`cargo build`

Feedback
========

Feel free to ping me if you have a question or a suggestion how to
make it better and idiomatic.
