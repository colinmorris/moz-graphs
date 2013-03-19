#!/bin/bash

# We don't want to keep our big data files under version control, so this script sets up soft links to them.
# The default place to search for them is in a particular Dropbox subdir, but you may have to modify this.

targets='data src/alch.db chat_logs'

orig="${HOME}/Dropbox/Joel/debuggers/"
dest="./"

for t in $targets
do
  echo "Linking from ${orig}${t} to ${dest}${t}"
  ln -s ${orig}${t} ${dest}${t}
done

echo 'Hey, have you changed ROOT_DIR in config.py? You should probably do that.'
