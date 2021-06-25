#!/usr/bin/env bash

cd /usr/local/virtuoso-opensource/var/lib/virtuoso/db



echo -e  "GRANT SPARQL_UPDATE to \x22SPARQL\x22;" >> /FedSDM/sql-query.sql
virtuoso-t +wait && isql-v 1111 dba dba /FedSDM/sql-query.sql
kill "$(ps aux | grep '[v]irtuoso-t' | awk '{print $2}')"

virtuoso-t -f &
cd /FedSDM

export FLASK_ENV=development
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
export FLASK_APP=fedsdm
flask run --host=0.0.0.0 --port=5003
# python3 fedsdm_service.py