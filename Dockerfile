FROM kemele/virtuoso:7-stable

# Install Virtuoso prerequisites and crudini Python lib
RUN apt-get update \
        && apt-get install -y python3.5 python3-pip python3-setuptools

# Set the working directory to /FedSDM
WORKDIR /FedSDM

VOLUME /data

ADD . /FedSDM
# Install any needed packages specified in requirements.txt
RUN pip3 install -r /FedSDM/requirements.txt


EXPOSE 5003
EXPOSE 8890

# Define environment variable
ENV METADATA_ENDPOINT http://localhost:8890/sparql
ENV DEFAULT_GRAPH http://ontario.tib.eu
ENV APP_PREFIX /

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
# Run virtuoso-t and fedsdm_service.py when the container launches
CMD ["/FedSDM/start-fedsdm.sh"]