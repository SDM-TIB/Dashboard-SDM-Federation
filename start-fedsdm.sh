#!/bin/bash

cd /database
echo -e "GRANT SPARQL_UPDATE to \x22SPARQL\x22;" >> /opt/virtuoso-opensource/initdb.d/sparql_update.sql
/virtuoso-entrypoint.sh &

cd /FedSDM
if ! test -f "instance/fedsdm.sqlite"; then
  mkdir -p instance
  chmod 775 instance
  cat fedsdm/schema.sql | sqlite3 instance/fedsdm.sqlite
fi

# wait for Virtuoso to be available
printf 'Waiting for Virtuoso'
until $(curl --output /dev/null --silent --head --fail http://localhost:$VIRT_HTTPSERVER_SERVERPORT); do
    printf '.'
    sleep 1s
done
echo -e '\nVirtuoso is up and running'

export FLASK_ENV=development
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
export FLASK_APP=fedsdm
flask run --host=0.0.0.0 --port=5003
