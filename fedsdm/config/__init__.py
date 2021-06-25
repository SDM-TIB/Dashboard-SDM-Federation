
__author__ = 'kemele'
import abc
import json
from fedsdm.rdfmt import MTManager


class ConfigSimpleStore(object):

    def __init__(self, graph, endpoint, update, user, passwd):
        self.federation = graph
        self.endpoint = endpoint
        self.update = update
        self.user = user
        self.password = passwd
        self.mgr = MTManager(endpoint, user, passwd, graph)
        self.rdfmts = {}
        self.predidx = {}
        self.predidx = {}

    def createPredicateIndex(self):
        pidx = {}
        for m in self.rdfmts:
            preds = self.rdfmts[m]['predicates']
            for p in preds:
                if p['predicate'] not in pidx:
                    pidx[p['predicate']] = set()
                    pidx[p['predicate']].add(m)
                else:
                    pidx[p['predicate']].add(m)

        return pidx

    def findbypreds(self, preds):
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

    def find_rdfmt_by_preds(self, preds):
        if len(self.rdfmts) > 0:
            return self.findbypred(preds)
        res = self.mgr.get_rdfmts_by_preds(preds)
        return res

    def findbypred(self, pred):
        res = self.mgr.get_rdfmts_by_preds([pred])
        return res.keys()

    def findMolecule(self, molecule):
        if molecule in self.rdfmts:
            return self.rdfmts[molecule]
        rdfmt = self.mgr.get_rdfmt(molecule)
        return rdfmt

    def load_rdfmt(self, rdfclass):
        if rdfclass in self.rdfmts:
            return self.rdfmts[rdfclass]
        rdfmt = self.mgr.get_rdfmt(rdfclass)
        return rdfmt

    # def findMolecules(self, preds):
