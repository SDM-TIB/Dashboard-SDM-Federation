from __future__ import annotations  # Python 3.10 still has issues with typing when using classes from the same module

import datetime
import time
import urllib.parse as urlparse
from base64 import b64encode
from enum import Enum
from typing import List, Optional

import requests

from FedSDM.rdfmt.prefixes import MT_ONTO, MT_RESOURCE
from FedSDM.rdfmt.utils import contact_rdf_source


class RDFMT(object):
    """Provides an abstract representation of an RDF Molecule Template.

    The :class:`RDFMT` holds all the information about an RDF Molecule
    Template, like the RDF class, sources, and predicates it is associated
    with. It might also hold a description.

    """

    def __init__(self,
                 rid: str,
                 name: str = None,
                 mt_type: int = 0,
                 sources: List[Source] = None,
                 subclass_of: list = None,
                 properties: List[MTProperty] = None,
                 desc: str = None):
        """Initializes an instance of :class:`RDFMT`.

        The RDF Molecule Template is created based on the passed parameters.

        Parameters
        ----------
        rid : str
            The identifier of the RDF Molecule Template.
        name : str, optional
            A human-readable name for the RDF Molecule Template. If no name is provided, the *rid* will be used.
        mt_type : int, optional
            The Molecule Template type of the RDF Molecule Template. The default value is 0 and represents a
            typed RDF Molecule Template. Currently, there are no other types available.
        sources : List[Source], optional
            A list of :class:`Source` instances this RDF Molecule Template was collected from.
        subclass_of : list, optional
            A list containing the superclasses of the class associated with this RDF Molecule Template.
            By default, it will be set to an empty list, meaning there are no superclasses.
        properties : List[MTProperty], optional
            A list of :class:`MTProperty` instances representing the properties (and additional data like range)
            of the properties belonging to the class that is associated with this RDF Molecule Template.
            By default, it will be set to an empty list, meaning there are no associated properties.
        desc : str, optional
            A human-readable description of the RDF Molecule Template, e.g., what the RDF class represents.

        """
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.sources = [] if sources is None else sources
        self.subClassOf = [] if subclass_of is None else subclass_of
        self.properties = [] if properties is None else properties
        if desc is not None and '^^' in desc:
            desc = desc[:desc.find('^^')]
        if name is not None and '^^' in name:
            name = name[:name.find('^^')]
        self.name = name if name is not None else rid
        self.desc = desc if desc is not None else ''
        self.mt_type = mt_type

    def to_rdf(self) -> List[str]:
        """Semantifies the RDF Molecule Template.

        The internal representation of the RDF Molecule Template is semantified by creating
        a list of RDF triples that describe the RDF Molecule Template.

        Returns
        -------
        List[str]
            A list of RDF triples in the form a string that represent the RDF Molecule Template.

        """
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'RDFMT> ']
        if self.mt_type == 0:
            data.append('<' + self.rid + '> a <' + MT_ONTO + 'TypedRDFMT> ')
        for s in self.sources:
            data.extend(s.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'source> <' + s.rid + '>')
        for s in self.subClassOf:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'subClassOf> <' + s + '> ')
        if self.desc is not None and self.desc != '':
            self.desc = self.desc.replace('"', "'").replace('\n', ' ')
            data.append('<' + self.rid + '> <' + MT_ONTO + 'desc> "' + self.desc + '" ')
        if self.name is not None and self.name != '':
            self.name = self.name.replace('"', "'").replace('\n', ' ')
            data.append('<' + self.rid + '> <' + MT_ONTO + 'name> "' + self.name + '" ')
        else:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'name> "' + self.rid + '" ')

        for r in self.properties:
            data.extend(r.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'hasProperty> <' + r.rid + '> ')

        today = str(datetime.datetime.now())
        data.append('<' + self.rid + '>  <http://purl.org/dc/terms/created> "' + today + '"')
        data.append('<' + self.rid + '>  <http://purl.org/dc/terms/modified> "' + today + '"')
        return data


class MTProperty(object):
    """Abstract representation of a property in an RDF Molecule Template.

    This class is used to represent a property (predicate) associated with
    an RDF class in a federation of datasources.

    """

    def __init__(self,
                 rid: str,
                 predicate: str,
                 sources: List[Source],
                 cardinality: int = -1,
                 ranges: List[PropRange] = None,
                 policies: List[ACPolicy] = None,
                 label: str = None):
        """Initializes an instance of :class:`MTProperty`.

        The Molecule Template property is created based on the passed parameters.

        Parameters
        ----------
        rid : str
            The identifier of the Molecule Template property.
        predicate : str
            The predicate this Molecule Template property represents.
        sources : List[Source]
            The list of :class:`Source` instances that serve the predicate.
        cardinality : int, optional
            The number of triples in which the predicate occurs. Only triples of the RDF class the
            RDF Molecule Template represents should be considered. By default, the value -1 will
            be assigned to signal the absence of this information.
        ranges : List[PropRange], optional
            A list of :class:`PropRange` instances representing the possible ranges of the predicate
            this Molecule Template property represents. By default, it will be set to an empty list,
            meaning the predicate does not link to any other RDF Molecule Templates.
        policies : List[ACPolicy], optional
            A list of :class:`ACPolicy` instances regulating the access to the information stored
            using this property; if any policies need to be applied.
        label : str, optional
            A human-readable description explaining what the predicate stands for.

        """
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.predicate = urlparse.quote(predicate, safe='/:#-')
        self.sources = sources
        self.cardinality = cardinality
        self.ranges = [] if ranges is None else ranges
        self.policies = [] if policies is None else policies

        if label is not None and '^^' in label:
            label = label[:label.find('^^')]
        self.label = label if label is not None else ''

    def to_rdf(self) -> List[str]:
        """Semantifies the Molecule Template property.

        The internal representation of the Molecule Template property is semantified by creating
        a list of RDF triples that describe the Molecule Template property.

        Returns
        -------
        List[str]
            A list of RDF triples in the form a string that represent the Molecule Template property.

        """
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'MTProperty> ']
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'cardinality> ' + str(self.cardinality))
        for s in self.sources:
            data.extend(s.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'propSource> <' + s.rid + '> ')

        if self.label is not None and self.label != '':
            self.label = self.label.replace('"', "'").replace('\n', ' ')
            data.append('<' + self.rid + '> <' + MT_ONTO + 'label> "' + self.label + '" ')
        if self.predicate is not None:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'predicate> <' + self.predicate + '> ')
        for r in self.ranges:
            data.extend(r.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'linkedTo> <' + r.rid + '> ')

        for p in self.policies:
            data.extend(p.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'policies> <' + p.rid + '> ')
        return data


class PropRange(object):
    """Abstract representation of the range of a property of an RDF Molecule Template.

    This class is used to represent the range of a property of an RDF Molecule Template.

    """

    def __init__(self,
                 rid: str,
                 prange: str,
                 source: DataSource,
                 range_type: int = 0,
                 cardinality: int = -1):
        """Initializes an instance of :class:`PropRange`.

        The property range is created based on the passed parameters.

        Parameters
        ----------
        rid : str
            The identifier of the property range.
        prange : str
            The RDF class the property links to.
        source : DataSource
            The :class:`DataSource` instance in which the property links to the RDF class *prange*.
        range_type : int, optional
            The range type specifies if the range is a class or a data type. The default value of
            0 represents the range being a class. If the range is a data type, the value should
            be set to 1.
        cardinality : int, optional
            The number of triples in *source* in which the predicate appears and the object is of
            type *prange*. By default, the value -1 will be assigned to signal the absence of
            this information.

        """
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.prange = prange
        self.source = source
        self.cardinality = cardinality
        self.range_type = range_type

    def to_rdf(self) -> List[str]:
        """Semantifies the property range.

        The internal representation of the property range is semantified by creating
        a list of RDF triples that describe the property range.

        Returns
        -------
        List[str]
            A list of RDF triples in the form a string that represent the property range.

        """
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'PropRange> ',
                '<' + self.rid + '> <' + MT_ONTO + 'datasource> <' + self.source.rid + '> ',
                '<' + self.rid + '> <' + MT_ONTO + 'name> <' + self.prange + '> ']
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'cardinality> ' + str(self.cardinality))
        if self.range_type == 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'rdfmt> <' + self.prange + '> ')
        else:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'xsdtype> <' + self.prange + '> ')
        return data


class Source(object):
    """A wrapper around :class:`DataSource` to hide some information.

    An instance of :class:`Source` basically holds the identifier of the actual datasource
    as well as a context specific cardinality. All other information is hidden.

    """

    def __init__(self,
                 rid: str,
                 source: DataSource,
                 cardinality: int = -1):
        """Initializes an instance of :class:`Source`.

        The source is created passed on the parameters passed.

        Parameters
        ----------
        rid : str
            The identifier of the :class:`DataSource` this source is wrapping.
        source : DataSource
            The datasource that is wrapped by this instance.
        cardinality : int
            The context specific cardinality, e.g., for a Molecule Template property.

        """
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.source = source
        self.cardinality = cardinality

    def to_rdf(self) -> List[str]:
        """Semantifies the source.

        The internal representation of the source is semantified by creating
        a list of RDF triples that describe the source.

        Returns
        -------
        List[str]
            A list of RDF triples in the form a string that represent the source.

        """
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'Source> ',
                '<' + self.rid + '> <' + MT_ONTO + 'datasource> <' + self.source.rid + '> ']
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'cardinality> ' + str(self.cardinality))
        return data


class DataSource(object):
    """An abstract representation of a datasource.

    This class represents an actual datasource. However, only metadata about the source is
    stored. The information about the data itself is represented using different other classes.

    """

    def __init__(self,
                 rid: str,
                 url: str,
                 ds_type: str | DataSourceType,
                 name: str = None,
                 desc: str = None,
                 params: str = None,
                 keywords: str = None,
                 homepage: str = None,
                 version: str = None,
                 organization: str = None,
                 ontology_graph: str = None,
                 triples: int = -1):
        """Initializes an instance of :class:`DataSource`.

        The datasource is created based on the passed parameters.

        Parameters
        ----------
        rid : str
            The identifier of the datasource.
        url : str
            The URL of the datasource, i.e., where is can be accessed.
        ds_type : str | DataSourceType
            The type of the datasource, e.g., SPARQL endpoint.
        name : str, optional
            A human-readable name for the datasource. If none is provided, the URL will be used as a name.
        desc : str, optional
            A short description explaining what the datasource is about. If none is provided,
            an empty string will be used.
        params : str, optional
            A string with parameters to access the datasource. The string holds key-value pairs.
            The pairs are separated by semicolon (;). The delimiter of key and value is a colon (:).
        keywords : str, optional
            A string containing all keywords associated with the datasource.
        homepage : str, optional
            The URL of the homepage describing the datasource if any.
        version : str, optional
            The version of the dataset that is being accessed with this datasource if any.
        organization : str, optional
            The organization who published the dataset if any.
        ontology_graph : str, optional
            An optional URL for an ontology endpoint serving the ontology of the datasource.
            This can be used to speed up the extraction of metadata about the data in the source.
        triples : int, optional
            The number of triples stored in the datasource. If no value is provided, it defaults
            to -1 to signal the absence of the information.

        """
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.url = url
        self.ds_type = ds_type if isinstance(ds_type, DataSourceType) else DataSourceType.from_str(ds_type)
        self.name = self.url if name is None else name.replace('"', "'")
        self.desc = desc if desc is not None else ''
        self.params = {} if params is None else params
        self.keywords = keywords if keywords is not None else ''
        self.homepage = homepage if homepage is not None else ''
        self.version = version if version is not None else ''
        self.organization = organization if organization is not None else ''
        self.triples = triples
        self.ontology_graph = ontology_graph
        self.auth_token = None
        self.auth_token_valid_until = None

    @staticmethod
    def __get_auth_token(server, username, password):
        payload = 'grant_type=client_credentials&client_id=' + username + '&client_secret=' + password
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        start = time.time()
        response = requests.request('POST', server, headers=headers, data=payload)
        if response.status_code != 200:
            raise Exception(str(response.status_code) + ': ' + response.text)
        return response.json()['access_token'], start + response.json()['expires_in']

    def get_auth(self):
        """
        DeTrusty v0.6.0 introduced the use of private endpoints.
        The dashboard currently does not, but this method is needed for DeTrusty to run.
        """
        params = self.params_to_dict()
        if params is not None and 'username' in params and 'password' in params:
            if 'keycloak' in params:
                valid_token = False
                if self.auth_token is not None and self.auth_token_valid_until is not None:
                    current = time.time()
                    if self.auth_token_valid_until > current:
                        valid_token = True

                if valid_token:
                    token = self.auth_token
                else:
                    token, valid_until = self.__get_auth_token(
                        params['keycloak'],
                        params['username'],
                        params['password']
                    )
                    self.auth_token = token
                    self.auth_token_valid_until = valid_until
                return 'Bearer ' + token
            else:
                credentials = params['username'] + ':' + params['password']
                return 'Basic ' + b64encode(credentials.encode()).decode()
        return None

    def is_accessible(self) -> bool:
        """Performs an accessibility check for the datasource.

        This method checks whether the datasource can be accessed from FedSDM.
        Currently, only SPARQL endpoints are supported. Hence, this method
        only checks the accessibility of such datasources.

        Returns
        -------
        bool
            A Boolean indicating whether the datasource is accessible. Obviously,
            true represents an accessible datasource. False indicates otherwise.

        """
        ask = 'ASK {?s ?p ?o}'
        e = self.url
        if self.ds_type == DataSourceType.SPARQL_ENDPOINT:
            print('checking endpoint accessibility', e)
            val, c = contact_rdf_source(ask, self)
            if c == -2:
                print(e, ' -> is not accessible. Hence, will not be included in the federation!')
            if val:
                return True
            else:
                print(e, ' -> is returning empty results. Hence, will not be included in the federation!')
        return False

    def to_rdf(self, update: bool = False) -> List[str]:
        """Semantifies the datasource.

        The internal representation of the datasource is semantified by creating
        a list of RDF triples that describe the datasource.

        Returns
        -------
        List[str]
            A list of RDF triples in the form a string that represent the datasource.

        """
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'DataSource> ',
                '<' + self.rid + '> <' + MT_ONTO + 'dataSourceType> <' + MT_RESOURCE + 'DatasourceType/' + str(self.ds_type.value) + '> ',
                '<' + self.rid + '> <' + MT_ONTO + 'url> "' + urlparse.quote(self.url, safe='/:') + '" ']
        if self.name is not None and self.name != '':
            self.name = self.name.replace('"', "'").replace('\n', ' ')
            data.append('<' + self.rid + '> <' + MT_ONTO + 'name> "' + self.name + '" ')
        if self.version is not None and self.version != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'version> "' + self.version + '" ')
        if self.keywords is not None and self.keywords != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'keywords> "' + self.keywords + '" ')
        if self.organization is not None and self.organization != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'organization> "' + self.organization + '" ')
        if self.homepage is not None and self.homepage != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'homepage> "' + self.homepage + '" ')
        if self.params is not None and len(self.params) > 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'params> "' + str(self.params) + '" ')
        if self.desc is not None and self.desc != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'desc> "' + self.desc.replace('"', "'").replace('`', "'") + '"')
        if self.triples != '' and int(self.triples) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'triples> ' + str(self.triples))

        today = str(datetime.datetime.now())
        if not update:
            data.append('<' + self.rid + '>  <http://purl.org/dc/terms/created> "' + today + '"')
        data.append('<' + self.rid + '>  <http://purl.org/dc/terms/modified> "' + today + '"')
        return data

    def __repr__(self) -> str:
        """Creates a simple and human-readable representation of a datasource.

        This method creates a simple and human-readable representation of a datasource.
        This representation includes the identifier, URL, datasource type, and parameters.

        Returns
        -------
        str
            A simple and human-readable representation of a datasource.

        """
        return '{\n' + \
               '  rid: ' + self.rid + ',\n' \
               '  url: ' + self.url + ',\n' \
               '  dstype: ' + str(self.ds_type) + ',\n' \
               '  params: ' + str(self.params) + '\n' \
               '}'

    def params_to_dict(self):
        result = {}
        pairs = self.params.split(';')
        for pair in pairs:
            param = pair.split(':', 1)
            result[param[0]] = param[1]
        return result


class DataSourceType(Enum):
    """An enum to describing a datasource's type.

    This enum holds many datasource types. However, FedSDM is currently only supporting SPARQL endpoints.

    """
    SPARQL_ENDPOINT = 'SPARQL_Endpoint'
    MONGODB = 'MongoDB'
    NEO4J = 'Neo4j'
    MYSQL = 'MySQL'
    SPARK_CSV = 'SPARK_CSV'
    SPARK_TSV = 'SPARK_TSV'
    SPARK_JSON = 'SPARK_JSON'
    SPARK_XML = 'SPARK_XML'
    REST_SERVICE = 'REST'
    LOCAL_CSV = 'LOCAL_CSV'
    LOCAL_TSV = 'LOCAL_TSV'
    LOCAL_JSON = 'LOCAL_JSON'
    LOCAL_XML = 'LOCAL_XML'

    @staticmethod
    def from_str(value: str) -> Optional[DataSourceType]:
        """Create an instance of :class:`DataSourceType` from a string.

        Transforms a string to an instance of :class:`DataSourceType`.
        This is done internally by string matching and is dependent
        on the implementation by Python.

        Note
        ----
        The string might have the prefix for resource in the Molecule Template description.
        This method can handle the prefix and still return the correct instance.
        There is no need to remove the prefix beforehand.

        Parameters
        ----------
        value : str
            The string that should be transformed to an instance of :class:`DataSourceType`.

        Returns
        -------
        DataSourceType | None
            Returns the corresponding instance of :class:`DataSourceType` if one can be found.
            Otherwise, None will be returned to indicate that no such type is available.

        """
        if value is None:
            return None
        try:
            if MT_RESOURCE in value:
                value = value.split(MT_RESOURCE + 'DatasourceType/')[1]
            return DataSourceType(value)
        except KeyError:
            return DataSourceType.SPARQL_ENDPOINT


class ACPolicy(object):
    # TODO: This class is currently unused and needs to be checked.

    def __init__(self,
                 authorized_by: AppUser,
                 operations: List[ACOperation],
                 authorized_to: AppUser,
                 valid_from: datetime = None,
                 valid_until: datetime = None,
                 desc: str = ''):
        self.authorized_by = authorized_by
        self.authorized_to = authorized_to
        self.operations = operations
        self.valid_from = valid_from
        self.valid_until = valid_until
        self.desc = desc


class ACOperation(object):
    # TODO: This class is currently unused and needs to be checked.

    def __init__(self, name: str, desc: str = None):
        self.name = name
        self.desc = '' if desc is None else desc


class AppUser(object):
    # TODO: This class is currently unused and needs to be checked.

    def __init__(self, name: str, username: str, passwd: str = None, params: dict = None):
        self.name = name
        self.username = username
        self.passwd = passwd
        self.params = {} if params is None else params
