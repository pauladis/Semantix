# foi utilizado o python 3.7 em um contexto com hadoop 2.7 e spark 2.2.1, ambiente montando em uma maquina virtual com apenas 1 core do processador

from pyspark import SparkContext
from pyspark.sql import Row
import heapq, pyspark

sc = SparkContext.getOrCreate()

# Datasets:
# 	July -> ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
# 	August -> ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz

path_jul = 'C:\\Users\\spark\\Desktop\\access_log_Jul95'
path_aug = 'C:\\Users\\spark\\Desktop\\access_log_Aug95'

july = sc.textFile(path_jul)
july = july.cache()

august = sc.textFile(path_aug)
august = august.cache()

dados_arquivo = august.union(july)
dados_arquivo = dados_arquivo.flatmap(lambda x: (x.split(" ")[0], x.split(" ")[3][1:12], x.split(" ")[6], x.split(" ")[8], x.split(" ")[9]))


#1. Número de hosts únicos.

hosts = dados_arquivo.flatmap(lambda x: x[0]).distinct().count()
print("O numero de hosts unicos nos 2 meses juntos foi de : {hosts}").format(hosts=hosts)
# 137979


#2. O total de erros 404.

errors = dados_arquivo.filter(lambda l: '404' in l[:-1]).cache()
print("O numero de erros 404 nos 2 meses juntos foi de : {errors}").format(errors=errors.count())
# 28003


#3. Os 5 URLs que mais causaram erro 404.

urls = {}
for line in dados_arquivo.collect():
   line = line.split(" ")
   if len(a) < 4:
       continue
   if line[-2] == '404':
       if line[-4] in urls.keys():
           urls[line[-4]] = urls.get(line[-4]) + 1
       else:
           urls[line[-4]] = 1
top5 = heapq.nlargest(5, urls, key=urls.get)
print("Os endpoints com o maior numero de erros 404 foram: {endpoint}").format(endpoint=top5)
#['/pub/winvn/readme.txt', '/pub/winvn/release.txt', '/shuttle/missions/STS-69/mission-STS-69.html', '/shuttle/missions/sts-68/ksc-upclose.gif', '/history/apollo/a-001/a-001-patch-small.gif']


###4. Quantidade de erros 404 por dia.

notFoundByDay = {}

def addNotFound(x):
    if x[:-1] == '404':
        if x[3] in notFoundByDay.keys():
            urls[x[3]] = notFoundByDay.get(x[x]) + 1
        else:
            urls[x[3]] = 1

dados_arquivo.flatmap(addNotFound)

for day,count in notFoundByDay.items():
    print("{day} : {count)").format(day=day,count=count)

'''
'01/Jul/1995': 316,
'02/Jul/1995': 291,
'03/Jul/1995': 474,
'04/Jul/1995': 359,
'05/Jul/1995': 497,
'06/Jul/1995': 640,
'07/Jul/1995': 570,
'08/Jul/1995': 302,
'09/Jul/1995': 348,
'10/Jul/1995': 398,
'11/Jul/1995': 471,
'12/Jul/1995': 471,
'13/Jul/1995': 532,
'14/Jul/1995': 413,
'15/Jul/1995': 254,
'16/Jul/1995': 257,
'17/Jul/1995': 406,
'18/Jul/1995': 465,
'19/Jul/1995': 639,
'20/Jul/1995': 428,
'21/Jul/1995': 334,
'22/Jul/1995': 192,
'23/Jul/1995': 233,
'24/Jul/1995': 328,
'25/Jul/1995': 461,
'26/Jul/1995': 336,
'27/Jul/1995': 336,
'28/Jul/1995': 94,
'01/Aug/1995': 243,
'03/Aug/1995': 304,
'04/Aug/1995': 346,
'05/Aug/1995': 236,
'06/Aug/1995': 373,
'07/Aug/1995': 537,
'08/Aug/1995': 391,
'09/Aug/1995': 279,
'10/Aug/1995': 315,
'11/Aug/1995': 263,
'12/Aug/1995': 196,
'13/Aug/1995': 216,
'14/Aug/1995': 287,
'15/Aug/1995': 327,
'16/Aug/1995': 259,
'17/Aug/1995': 271,
'18/Aug/1995': 256,
'19/Aug/1995': 209,
'20/Aug/1995': 312,
'21/Aug/1995': 305,
'22/Aug/1995': 288,
'23/Aug/1995': 345,
'24/Aug/1995': 420,
'25/Aug/1995': 415,
'26/Aug/1995': 366,
'27/Aug/1995': 370,
'28/Aug/1995': 410,
'29/Aug/1995': 420,
'30/Aug/1995': 571,
'31/Aug/1995': 526
'''


###5. O total de bytes retornados.

total = sum(dados_arquivo.flatmap(lambda x: x[-1] if x[-1].isDigit() else 0))
print("Os total de bytes retornados foi de: {total}").format(total=total)
