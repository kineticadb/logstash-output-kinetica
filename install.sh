set -o xtrace
GEM_PATH=`pwd`
GEM=$(ls -l logstash-output-kinetica-*.gem | awk {'print $9'} | tail -1)
cd ${LS_HOME}
sudo ${LS_HOME}/bin/logstash-plugin install ${GEM_PATH}/${GEM}
