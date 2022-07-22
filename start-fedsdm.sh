#!/bin/bash

cd /database
echo -e "GRANT SPARQL_UPDATE to \x22SPARQL\x22;" >> /opt/virtuoso-opensource/initdb.d/sparql_update.sql
/virtuoso-entrypoint.sh &

cd /FedSDM

# wait for Virtuoso to be available
echo 'Waiting for Virtuoso'
until $(curl --output /dev/null --silent --head --fail http://localhost:$VIRT_HTTPSERVER_SERVERPORT); do
    echo -n '.'
    sleep 1s
done
echo -e '\nVirtuoso is up and running'

export FLASK_ENV=development
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
export FLASK_APP=FedSDM
flask run --host=0.0.0.0 --port=5003
