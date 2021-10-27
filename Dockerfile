FROM kemele/virtuoso:7-stable

# Define environment variables
ENV METADATA_ENDPOINT="http://localhost:8890/sparql" \
    DEFAULT_GRAPH="http://ontario.tib.eu" \
    APP_PREFIX="/" \
    LC_ALL="C.UTF-8" \
    LANG="C.UTF-8"

# Install Virtuoso prerequisites and crudini Python lib
RUN apt-get update &&\
    apt-get install -y python3.5 python3-pip python3-setuptools sqlite3 &&\
    apt-get clean

# Set the working directory to /FedSDM
WORKDIR /FedSDM

VOLUME /data

COPY . /FedSDM
# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r /FedSDM/requirements.txt

EXPOSE 5003 8890

# Run virtuoso-t and fedsdm_service.py when the container launches
CMD ["/FedSDM/start-fedsdm.sh"]
