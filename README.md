# Semantix

Qual o objetivo do comando cache em Spark?

A maior parte das operações em um RDD são lazy, o que significa que, na prática, resultam apenas em uma abstração para um conjunto de instruções a serem executadas. Essas operações só são realmente executadas através de ações, que não são operações lazy, pois só podem ser avaliadas a partir da obtenção de valores do RDD. Isso pode, por exemplo, tornar códigos iterativos mais ineficientes, pois ações executadas repetidamente sobre um mesmo conjunto de dados disparam a ação de todas as operações lazy necessárias em cada uma das iterações, mesmo que resultados intermediários sejam iguais, como no caso da leitura de um arquivo. O uso do comando cache ajuda a melhorar a eficiência do código nesse tipo de cenário, pois permite que resultados intermediários de operações lazy possam ser armazenados e reutilizados repetidamente.

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

Existem alguns fatores no desenho dessas ferramentas que tornam as aplicações desenvolvidas em MapReduce geralmente mais lentas que aquelas que utilizam Spark. Um desses fatores é o uso de memória. É comum a necessidade de rodar vários jobs MapReduce em sequência em vez de um único job. Ao usar MapReduce, o resultado de cada job é escrito em disco, e precisa ser lido novamente do disco quando passado ao job seguinte. Spark, por outro lado, permite que resultados intermediários sejam passados diretamente entre as operações a serem executadas através do caching desses dados em memória, ou até mesmo que diversas operações possam ser executadas sobre um mesmo conjunto de dados em cache, reduzindo a necessidade de escrita/leitura em disco. Adicionalmente, mesmo em cenários onde ocorre a execução de apenas um job, o uso de Spark tende a ter desempenho superior ao MapReduce. Isso ocorre porque jobs Spark podem ser iniciados mais rapidamente, pois para cada job MapReduce uma nova instância da JVM é iniciada, enquanto Spark mantém a JVM em constantemente em execução em cada nó, precisando apenas iniciar uma nova thread, que é um processo extremamente mais rápido.

Qual é a função do SparkContext ?

O SparkContext funciona como um cliente do ambiente de execução Spark. Através dele, passam-se as configurações que vão ser utilizadas na alocação de recursos, como memória e processadores, pelos executors. Também usa-se o SparkContext para criar RDDs, colocar jobs em execução, criar variáveis de broadcast e acumuladores.

Explique com suas palavras o que é Resilient Distributed Datasets (RDD)

RDDs são a principal abstração de dados do Spark. Eles são chamados Resilient por serem tolerantes à falha, isto é, são capazes de recomputar partes de dados perdidas devido a falhas nos nós e são Distributed porque podem estar divididos em partições através de diferentes nós em um cluster. Além dessas características, outras que podem ser destacadas são: RDDs são imutáveis, são objetos para leitura apenas, e só podem ser mudados através de transformações que resultam na criação de novos RDDs; Eles podem ser operados em paralelo, isto é, operações podem ser executadas sobre diferentes partições de um mesmo RDD ao mesmo tempo; RDDs são avaliados de forma "preguiçosa", de forma que os dados só ficam acessíveis e só são transformados quando alguma ação é executada (como mencionado na primeira questão); além disso RDDs têm seus valores categorizados em tipos, como números inteiros ou de ponto flutuante, strings, pares...

GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

Quando fazendo uma agregação utilizando reduceByKey, Spark sabe que pode realizar a operação passada como parâmetro em todos os elementos de mesma chave em cada partição para obter um resultado parcial antes de passar esses dados para os executores que vão calcular o resultado final, resultando em um conjunto menor de dados sendo transferido. Por outro lado, ao usar groupByKey e aplicar a agregação em seguida, o cálculo de resultados parciais não é realizado, dessa forma um volume muito maior de dados é desnecessariamente transferido através dos executores podendo, inclusive, ser maior que a quantidade de memória disponível para o mesmo, o que cria a necessidade de escrita dos dados em disco e resulta em um impacto negativo bastante significante na performance.

** Explique o que o código Scala abaixo faz **

1. val textFile = sc . textFile ( "hdfs://..." )
2. val counts = textFile . flatMap ( line => line . split ( " " ))
3.           . map ( word => ( word , 1 ))
4.           . reduceByKey ( _ + _ )
5. counts . saveAsTextFile ( "hdfs://..." )
Nesse código, um arquivo-texto é lido (linha 1). Em seguida, cada linha é "quebrada" em uma sequência de palavras e as sequencias correspondentes a cada linha são transformadas em uma única coleção de palavras (2). Cada palavra é então transformada em um mapeamente de chave-valor, com chave igual à própria palavra e valor 1 (3). Esses valores são agregados por chave, através da operação de soma (4). Por fim, o RDD com a contagem de cada palavra é salvo em um arquivo texto (5).



HTTP requests to the NASA Kennedy Space Center WWW server
Fonte oficial do dateset: http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
Dados:
● Jul 01 to Jul 31, ASCII format, 20.7MB gzip compressed, 205.2MB.
● Aug 04 to Aug 31, ASCII format, 21.8MB gzip compressed, 167.8MB.
Sobre o dataset: Esses dois conjuntos de dados possuem todas as requisições HTTP para o servidor da NASA Kennedy


Questões
Responda as seguintes questões devem ser desenvolvidas em Spark utilizando a sua linguagem de preferência.


from pyspark import SparkContext
import heapq

sc = SparkContext.getOrCreate()

path_jul = 'C:\\Users\\spark\\Desktop\\access_log_Jul95'
path_aug = 'C:\\Users\\spark\\Desktop\\access_log_Aug95'

dados_arquivo_jul = sc.textFile(path_jul)
dados_arquivo_aug = sc.textFile(path_aug)
dados_arquivo = dados_arquivo_aug.union(dados_arquivo_jul)

###1. Número de hosts únicos.

hosts = set()
for l in dados_arquivo_aug.collect():
    l = l.split(" ")
    hosts.add(l[0])


for l in dados_arquivo_jul.collect():
    l = l.split(" ")
    hosts.add(l[0])

print(len(hosts))
137979


###2. O total de erros 404.

print(dados_arquivo.filter(lambda l: '404' in l[:-1]).count())
28003

###3. Os 5 URLs que mais causaram erro 404.

urls = {}

for a in dados_arquivo_jul.collect():
    a = a.split(" ")
    if len(a) < 4:
        continue
    if a[-2] == '404':
        if a[-4] in urls.keys():
            urls[a[-4]] = urls.get(a[-4]) + 1
        else:
            urls[a[-4]] = 1

for a in dados_arquivo_aug.collect():
    a = a.split(" ")
    if len(a) < 4:
        continue
    if a[-2] == '404':
        if a[-4] in urls.keys():
            urls[a[-4]] = urls.get(a[-4]) + 1
        else:
            urls[a[-4]] = 1

print(heapq.nlargest(5, urls, key=urls.get))
['/pub/winvn/readme.txt', '/pub/winvn/release.txt', '/shuttle/missions/STS-69/mission-STS-69.html', '/shuttle/missions/sts-68/ksc-upclose.gif', '/history/apollo/a-001/a-001-patch-small.gif']



###4. Quantidade de erros 404 por dia.

notFoundByday = {}

for a in dados_arquivo_jul.collect():
    a = a.split(" ")
    if len(a) < 4:
        continue
    if a[-2] == '404':
        if a[3][1:12] in notFoundByDay.keys():
            notFoundByDay[a[3][1:12]] = notFoundByDay.get(a[3][1:12]) + 1
        else:
            notFoundByDay[a[3][1:12]] = 1

for a in dados_arquivo_aug.collect():
    a = a.split(" ")
    if len(a) < 4:
        continue
    if a[-2] == '404':
        if a[3][1:12] in notFoundByDay.keys():
            notFoundByDay[a[3][1:12]] = notFoundByDay.get(a[3][1:12]) + 1
        else:
            notFoundByDay[a[3][1:12]] = 1


print(notFoundByDay)

{
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
}


###5. O total de bytes retornados.

total = 0

for a in dados_arquivo_jul.collect():
    a = a.split(" ")
    if a[-1].isdigit():
        total += int(a[-1])
        
for a in dados_arquivo_aug.collect():
    a = a.split(" ")
    if a[-1].isdigit():
        total += int(a[-1])
        
print(total)
65524314915
