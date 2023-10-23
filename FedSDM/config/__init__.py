from abc import ABC

from DeTrusty.Molecule.MTManager import Config

from FedSDM.rdfmt import MTManager


class ConfigSimpleStore(Config, ABC):
    """Represents a federation of endpoints and serves to retrieve their metadata.

    This class is inspired by :class:`DeTrusty.Molecule.MTManager.Config` and wraps its functionality
    so that it works with the RDF representation of the RDF Molecule Templates used by FedSDM.

    """

    def __init__(self, graph: str, endpoint: str, update: str, user: str, passwd: str):
        """Creates a new *ConfigSimpleStore* instance.

        The *ConfigSimpleStore* object can be used as an alternative configuration object
        for DeTrusty in order to execute queries over a given federation.
        The instance holds a :class:`FedSDM.rdfmt.MTManager` object to access the metadata
        knowledge graph used by FedSDM. Additionally, it holds the metadata of the federation,
        a list of the sources, and a map from predicates to RDF Molecule Templates.

        Parameters
        ----------
        graph : str
            The identifier of the federation this instance will be used for.
        endpoint : str
            The URL of the query endpoint for metadata knowledge graph of the federation.
        update : str
            The URL of the update endpoint for metadata knowledge graph of the federation.
        user : str
            The username required for updating the metadata of the federation.
        passwd : str
            The user's password required for updating the metadata of the federation.

        """
        super().__init__()
        self.federation = graph
        self.endpoint = endpoint
        self.update = update
        self.user = user
        self.password = passwd
        self.mgr = MTManager(endpoint, user, passwd, graph)
        self.metadata = self.mgr.get_rdfmts()
        self.sources = self.mgr.sources_dict
        self.predidx = {}

    def get_auth(self, endpoint: str) -> str | None:
        """Gets the authentication information about an endpoint.

        DeTrusty v0.6.0 introduced the use of private endpoints. FedSDM implements the authentication
        within the instances of :class:`FedSDM.rdfmt.model.DataSource`. This method searches for the
        endpoint of interest in the dictionary holding the sources of the federation. If it is found,
        the source is asked for its authentication information.

        Parameters
        ----------
        endpoint : str
            The URL of the endpoint of interest.

        Returns
        -------
        str | None
            If the source is found and needs authentication, the authentication string (to be
            included in the header of the request) is returned. If the source cannot be found
            or is open, this method returns None.

        """
        source = self.sources.get(endpoint, None)
        if source is None:
            return None
        else:
            return source.get_auth()

    def createPredicateIndex(self) -> dict:
        """Creates a predicate index indicating which RDF Molecule Templates include the predicate.

        By iterating over the metadata of the federation, an index for the predicates is created, i.e.,
        a map from the predicates in the federation to the RDF Molecule Templates in which they appear.

        Returns
        -------
        dict
            A dictionary with the predicates of the federation as keys. The value for each key is a list
            with all the RDF Molecule Templates in which it appears.

        """
        pidx = {}
        for m in self.metadata:
            preds = self.metadata[m]['predicates']
            for p in preds:
                if p['predicate'] not in pidx:
                    pidx[p['predicate']] = set()
                    pidx[p['predicate']].add(m)
                else:
                    pidx[p['predicate']].add(m)

        return pidx

    def findbypreds(self, preds: list) -> dict:
        """Finds RDF Molecule Templates that include a list of predicates.

        This method uses the predicate index and the :class:`FedSDM.rdfmt.MTManager` in order to find
        all RDF Molecule Templates in the federation that serve all the specified predicates.

        Parameters
        ----------
        preds : list
            A list containing all the predicates that need to be covered by the RDF Molecule Template.

        Returns
        -------
        dict
            A dictionary with all RDF Molecule Templates that cover the specified predicates.

        """
        if len(self.metadata) > 0:
            res = []
            if len(self.predidx) == 0:
                self.predidx = self.mgr.get_preds_mt()
            for p in preds:
                if p in self.predidx:
                    res.append(self.predidx[p])
            if len(res) != len(preds):
                return {}
            for r in res[1:]:
                res[0] = set(res[0]).intersection(set(r))
            if len(res) > 0:
                mols = list(res[0])
                return {m: self.mgr.get_rdfmt(m) for m in mols}
            else:
                return {}
        else:
            return self.mgr.get_rdfmts_by_preds(preds)

    def findbypred(self, pred: str) -> list:
        """Finds RDF Molecule Templates that include a specific predicate.

        This method makes use of the :class:`FedSDM.rdfmt.MTManager` in order to find all
        RDF Molecule Templates in the federation that cover the specific predicate.

        Parameters
        ----------
        pred : str
            The predicate that needs to be covered by the RDF Molecule Templates.

        Returns
        -------
        list
            A list with all RDF Molecule Templates that cover the specified predicate.

        """
        res = self.mgr.get_rdfmts_by_preds([pred])
        return list(res.keys())

    def findMolecule(self, molecule: str) -> dict:
        """Searches the metadata of the federation for a specific RDF Molecule Template.

        This method searches for a specific RDF Molecule Template in the metadata of the federation.
        If the RDF Molecule Template cannot be found in the metadata kept in memory by the *ConfigSimpleStore*
        instance, the :class:`FedSDM.rdfmt.MTManager` is asked to look for it.

        Parameters
        ----------
        molecule : str
            The root type of the RDF Molecule Template, i.e., the RDF class, of interest.

        Returns
        -------
        dict
            The dictionary representing the RDF Molecule Template of interest. The dictionary
            might be empty if no matching RDF Molecule Template was found.

        """
        if molecule in self.metadata:
            return self.metadata[molecule]
        rdfmt = self.mgr.get_rdfmt(molecule)
        return rdfmt
