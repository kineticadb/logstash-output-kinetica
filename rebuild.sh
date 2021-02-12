#!/bin/bash
set -o xtrace

bundle install
gem build logstash-output-kinetica.gemspec
./install.sh
sudo ${LS_HOME}/bin/logstash -f example/pipeline.conf --path.settings /etc/logstash
