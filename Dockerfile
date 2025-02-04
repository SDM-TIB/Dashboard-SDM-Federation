FROM python:3.12.8-slim-bookworm

# Define environment variables
ENV METADATA_ENDPOINT="http://localhost:9000/sparql" \
    METADATA_ENDPOINT_UPDTAE="http://localhost:9000/sparql-auth" \
    METADATA_ENDPOINT_USER="dba" \
    METADATA_ENDPOINT_PASSWORD="dba" \
    DEFAULT_GRAPH="http://ontario.tib.eu" \
    APP_PREFIX="/" \
    VIRT_HTTPSERVER_SERVERPORT="9000" \
    VIRT_URIQA_DEFAULTHOST="localhost:9000" \
    VIRT_PARAMETERS_NUMBEROFBUFFERS="340000" \
    VIRT_PARAMETERS_MAXDIRTYBUFFERS="250000" \
    VIRT_PARAMETERS_MAXQUERYMEM="2G" \
    VIRTUOSO_HOME="/opt/virtuoso-opensource"

# Set up prerequisites
COPY --from=prohde/virtuoso-opensource-7:7.2.13 /opt/virtuoso-opensource /opt/virtuoso-opensource
RUN ln -s /opt/virtuoso-opensource/database /database &&\
    ln -s /opt/virtuoso-opensource/bin/virtuoso-entrypoint.sh /virtuoso-entrypoint.sh &&\
    useradd -l -M virtuoso &&\
    chown -R virtuoso:virtuoso /opt/virtuoso-opensource &&\
    apt-get update &&\
    apt-get install -y --no-install-recommends curl &&\
    apt-get clean

# Set the working directory to /FedSDM
WORKDIR /FedSDM

VOLUME /database
VOLUME /FedSDM/instance

COPY . /FedSDM
# Install any needed packages specified in requirements.txt
RUN python -m pip install --upgrade pip==25.0.* &&\
    python -m pip install --no-cache-dir -r /FedSDM/requirements.txt

EXPOSE 5003 9000

# Run Virtuoso and Flask
ENTRYPOINT ["/FedSDM/start-fedsdm.sh"]
