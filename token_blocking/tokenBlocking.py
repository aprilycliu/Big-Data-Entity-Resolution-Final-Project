
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from io import StringIO
import csv
import re

class TokenBlocking(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_final)
        ]

    def mapper(self, _, line):
        import os
        rd = csv.reader(StringIO(line),delimiter=";")
        row = next(rd)
        eid = row.pop(0)
        stop_words = r'the|and|:|,|&|\?|!'
        if eid != "id":
            for r in row:
                words = re.sub(stop_words," ",r.lower())
                for word in words.split():
                    file_name = os.getenv('mapreduce_map_input_file')
                    if("first" in file_name):
                        if(word != "none"):
                            yield (word,("s1",int(eid)))
                    else:
                        if(word != "none"):
                            yield (word,("s2",int(eid)))
    
    def reducer(self, token, eids):
        es1 = []
        es2 = []
        for var in eids:
            if(var[0] == "s1"):
                es1.append(var[1])
            else:
                es2.append(var[1])
        if (len(es1) > 0) & (len(es2) > 0):
            yield (None,{token:{"s1":sorted(list(set(es1))),"s2":sorted(list(set(es2)))}})
    
    def reducer_final(self,key,value):
        yield None,list(value)

if __name__ == '__main__':
    TokenBlocking.run()
