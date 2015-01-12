#!/bin/sh

set -e
[ $TRAVIS_BRANCH = master ]
[ $TRAVIS_PULL_REQUEST = false ]
echo '<meta http-equiv=refresh content=0;url=lmdb/index.html>' > target/doc/index.html
pip install ghp-import --user $USER
$HOME/.local/bin/ghp-import -n target/doc
git push -q -f https://${TOKEN}@github.com/${TRAVIS_REPO_SLUG}.git gh-pages > /dev/null 2>&1
echo 'Pushed to gh-pages succesfully'
rm target/doc/index.html
mv target/doc .
curl http://www.rust-ci.org/artifacts/put?t=$RUSTCI_TOKEN | sh
