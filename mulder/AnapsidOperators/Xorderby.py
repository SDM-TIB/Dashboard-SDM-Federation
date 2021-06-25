'''
Created on Mar 03, 2014

Implements the Xorderby operator.
The intermediate results are represented in a queue. 

@author: Maribel Acosta Deibe
'''
from multiprocessing import Queue
import datetime

data_types = {
        'integer' : (int, 'numerical'),
        'decimal' : (float, 'numerical'),
        'float'   : (float, 'numerical'),
        'double'  : (float, 'numerical'),
        'string'  : (str, str),
        'boolean' : (bool, bool),
        'dateTime' : (datetime, datetime),
        'nonPositiveInteger' : (int, 'numerical'),
        'negativeInteger' : (int, 'numerical'),
        'long'    : (int, 'numerical'),
        'int'     : (int, 'numerical'),
        'short'   : (int, 'numerical'),
        'byte'    : (bytes, bytes),
        'nonNegativeInteger' : (int, 'numerical'),
        'unsignedLong' : (int, 'numerical'),
        'unsignedInt'  : (int, 'numerical'),
        'unsignedShort' : (int, 'numerical'),
        'unsignedByte' : (bytes, bytes), # TODO: this is not correct
        'positiveInteger' : (int, 'numerical')
        }


class Xorderby(object):
    
    def __init__(self, args):
        self.input = Queue()
        self.qresults = Queue()
        self.args = args        # List of type Argument.
        #print "self.args", self.args
        
    def execute(self, left, dummy, out, processqueue=Queue()):
        # Executes the Xorderby.
        self.left = left
        self.qresults = out
        results = []
        results_copy = []


        # Read all the results.
        tuple = self.left.get(True)
        #print "tuple", tuple
        tuple_id = 0
        orderargs = []
        while (tuple != "EOF"):
            results_copy.append(tuple)
            res = {}
            res.update(tuple)
            #print "tuple", tuple
            for arg in self.args:
                if "Argument" not in str(arg.__class__.__name__):
                    argname = self.extractName(arg)
                    if argname.name[1:] in tuple:
                        orderargs.append(argname)
                else:
                    argname = arg
                    if argname.name[1:] in tuple:
                        orderargs.append(arg)
                if argname.name[1:] in tuple:
                    res.update({argname.name[1:]: self.extractValue(tuple[argname.name[1:]])})
            res.update({'__id__' : tuple_id})
            results.append(res)
            tuple_id = tuple_id + 1
            tuple = self.left.get(True)
        
        # Sorting.
        self.args.reverse()
        #print "en order by ",self.args
        for arg in orderargs:
            if arg.name[1:] in results:
                order_by = "lambda d: (d['"+arg.name[1:]+"'])"
                results = sorted(results, key=eval(order_by), reverse=arg.desc)
              
        # Add results to output queue.
        for tuple in results: 
            self.qresults.put(results_copy[tuple['__id__']])
        
        # Put EOF in queue and exit. 
        self.qresults.put("EOF")
        return
    
    def extractValue(self, val):
        pos = val.find("^^")

        # Handles when the literal is typed.
        if (pos > -1):
            for t in data_types.keys():
                if (t in val[pos]):
                    (python_type, general_type) = data_types[t]

                    if (general_type == bool):
                        return val[:pos]

                    else:
                        return python_type(val[:pos])

        # Handles non-typed literals.
        else:
            return str(val)

    def extractName(self, arg):
        if "Argument" in str(arg.__class__.__name__):
            return arg
        else:
            if "Argument" not in str(arg.__class__.__name__):
                arg = self.extractName(arg.left)
            return arg



