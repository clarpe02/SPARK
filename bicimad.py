
from pyspark import SparkContext, SparkConf

from random import random

FILES_NAVIDAD=['diciembre.json','enero.json']
FILES_PRIMAVERA=['marzo.json','abril.json','mayo.json']
FILES_VERANO=['agosto.json','junio.json','julio.json']
FILES_AÑO=['enero.json','febrero.json','marzo.json','abril.json','mayo.json','agosto.json','junio.json','julio.json','septiembre.json','octubre.json','noviembre.json','diciembre.json']
    
with SparkContext() as sc:
   
    import json
    def mapper(line):
        data = json.loads(line)
        u_t = data['user_type']
        u_c = data['user_day_code']
        start = data['idunplug_station']
        end = data['idplug_station']
        return u_t, u_c, start, end
        
    def union(lista):
        rddlst=[]
        for f in lista:
            rddlst.append(sc.textFile(f))
        rdd = sc.union(rddlst)
        return rdd
    
    def ordenar_bicis(rdd):
        rdd = rdd.map(mapper)
        selected_type = 1
        rdd_users = rdd.filter(lambda x: x[0]==selected_type).map(lambda x: x[1:])
        rdd_users_salida = rdd_users.map(lambda x: (x[1],x[2]))
        rdd_users_entrada = rdd_users.map(lambda x: (x[2],x[1]))
        rdd_entrada=rdd_users_entrada.groupByKey().mapValues(list).mapValues(len)
        rdd_salida=rdd_users_salida.groupByKey().mapValues(list).mapValues(len).map(lambda x: (x[0],-x[1]))
        lista=[rdd_entrada, rdd_salida]
        rdd_total= sc.union(lista)
        rdd_total=rdd_total.groupByKey().mapValues(list).map(lambda x: (x[0],sum(x[1])))
        rdd_ord=rdd_total.sortBy(lambda x:x[1])
        return rdd_ord
    
    rdd_verano=ordenar_bicis(union(FILES_VERANO))
    rdd_primavera=ordenar_bicis(union(FILES_PRIMAVERA))
    rdd_navidad=ordenar_bicis(union(FILES_NAVIDAD))
    rdd_año=ordenar_bicis(union(FILES_AÑO))
    lista_verano=rdd_verano.collect()
    lista_primavera=rdd_primavera.collect()
    lista_navidad=rdd_navidad.collect()
    lista_año=rdd_año.collect()
    print(rdd_primavera.take(rdd_primavera.count()))
    print(rdd_navidad.take(rdd_navidad.count()))
    print(rdd_año.take(rdd_año.count()))
