FROM prohde/virtuoso-opensource-7:7.2.7

# Define environment variables
ENV METADATA_ENDPOINT="http://localhost:9000/sparql" \
    DEFAULT_GRAPH="http://ontario.tib.eu" \
    APP_PREFIX="/" \
    VIRT_HTTPSERVER_SERVERPORT="9000" \
    VIRT_URIQA_DEFAULTHOST="localhost:9000" \
    VIRT_PARAMETERS_NUMBEROFBUFFERS="340000" \
    VIRT_PARAMETERS_MAXDIRTYBUFFERS="250000" \
    VIRT_PARAMETERS_MAXQUERY_MEM="2G"

# Install Python3 and SQLite
RUN apt-get update &&\
    apt-get install -y --no-install-recommends software-properties-common &&\
    add-apt-repository -y ppa:deadsnakes/ppa &&\
    apt-get autoremove -y software-properties-common  &&\
    apt-get install -y --no-install-recommends python3.9 python3.9-venv curl &&\
    link /usr/bin/python3.9 /usr/bin/python3  &&\
    python3 -m ensurepip &&\
    apt-get clean

# Set the working directory to /FedSDM
WORKDIR /FedSDM

VOLUME /database

COPY . /FedSDM
# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r /FedSDM/requirements.txt

EXPOSE 5003 9000

# Run virtuoso-t and fedsdm_service.py when the container launches
ENTRYPOINT ["/FedSDM/start-fedsdm.sh"]
