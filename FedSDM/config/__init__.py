__author__ = 'Kemele M. Endris'

from FedSDM.rdfmt import MTManager


class ConfigSimpleStore(object):

    def __init__(self, graph, endpoint, update, user, passwd):
        self.federation = graph
        self.endpoint = endpoint
        self.update = update
        self.user = user
        self.password = passwd
        self.mgr = MTManager(endpoint, user, passwd, graph)
        self.metadata = self.mgr.get_rdfmts()
        self.predidx = {}

    def getEndpointToken(self, endpoint):
        """
        DeTrusty v0.6.0 implements the use of private endpoints.
        The dashboard currently does not but this method is needed for DeTrusty to run.
        """
        # TODO: implement private endpoints in the dashboard?
        return None

    def createPredicateIndex(self):
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

    def findbypreds(self, preds):
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

    def findbypred(self, pred):
        res = self.mgr.get_rdfmts_by_preds([pred])
        return res.keys()

    def findMolecule(self, molecule):
        if molecule in self.metadata:
            return self.metadata[molecule]
        rdfmt = self.mgr.get_rdfmt(molecule)
        return rdfmt

    def load_rdfmt(self, rdfclass):
        if rdfclass in self.metadata:
            return self.metadata[rdfclass]
        rdfmt = self.mgr.get_rdfmt(rdfclass)
        return rdfmt
