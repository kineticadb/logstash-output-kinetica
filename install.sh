set -o xtrace
GEM_PATH=`pwd`
cd ${LS_HOME}
sudo ${LS_HOME}/bin/logstash-plugin install ${GEM_PATH}/logstash-output-kinetica-0.1.0.gem

