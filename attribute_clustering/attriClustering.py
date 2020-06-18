
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from io import StringIO
import csv
import re

class AttriCluster(MRJob):
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
        jointCol = ["joint"+str(i) for i in range(7)]
        if eid != "id":
            file_name = os.getenv('mapreduce_map_input_file')
            for index in range(len(row)):
                words = re.sub(stop_words," ",row[index].lower())
                fn = "s1" if "first" in file_name else "s2"
                for word in words.split():
                    if word != "none":
                        yield (jointCol[index]+"-"+word,(fn,int(eid)))
    
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
    AttriCluster.run()
