# Semantix

##Qual o objetivo do comando cache em Spark?

Grande parte das operações de um RDD são apenas em uma abstração para um conjunto de instruções a serem realizadas, para essas operações nod damos o nome de lazy, estas operações só serão executadas após uma instrução de ação, que só podem ser validadas depois dos valores da RDD, com isso, alguns codigos iterativos se tornam ineficientes, pois um comando de ação encadeado irá disparar todas as operações lazy repetitivamente, para isto utilizamos o comando cache que permite que o resultado de operações lazy sejam armazenados e reutilizados


##O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

Para cada execução do MapReduce é criado uma nova instancia do JVM alem do reultado ser escrito em disco, necessitando da leitura quando passado para um job seguinte, enquanto no Spark, a JVM está constantemente em execução e permite que os resultados sejam passados diretamente entre as proximas operações a serem executadas por meio do cache em memoria, oque possibilita que diversas operações seguintes utilizem um mesmo conjunto de dados em cache, não sendo necessario a escrita e leitura do mesmo


##Qual é a função do SparkContext ?

SparkContext é um cliente do ambiente de execução do Spark, nele passamos alguns parametros de alocação de recursos, e atraves dele que criamos as nossas RDDs


##Explique com suas palavras o que é Resilient Distributed Datasets (RDD)

RDDs são uma abstração dos dados em Spark, são tolerantes a falha, através da recomputação das partes afetadas em eventuais problemas em algum nó, devido ao fato de serem distribuidas atraves do cluster em quantas partes forem necessarias, e para cada fração da RDD em um nó, a mesma é replicada em outro nó, a fim de manter a integralidade, RDDs são imutaveis e são operados em paralelo sobre todas as partições


##GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

Ao realizar uma agragação por reduceByKey, o Spark realiza a operação passada como parametro para todos os elementos com a mesma chave em cada partição, a fim de obter um resultado parcial que vai ser passado para o executor realizar o calculo final, já o groupByKey vai realizar a aplicação e agragação dos dados e passar esses dados para o executor, resultando num grande volume de dados a ser transferido


##** Explique o que o código Scala abaixo faz **

1. val textFile = sc . textFile ( "hdfs://..." )
2. val counts = textFile . flatMap ( line => line . split ( " " ))
3.           . map ( word => ( word , 1 ))
4.           . reduceByKey ( _ + _ )
5. counts . saveAsTextFile ( "hdfs://..." )

primeiro lemos um arquivo de texto do hadoop, logo em seguida cada linha do arquivo passa por um split que separa o conteudo atraves de cada espaço em branco, então cada palavra é mapeada em um dicionario de key/value, sendo que a key é a propria palavra e seu value é 1, após isso os valores são agregados de acordo com a key somando seus valores, e tudo isto é armazenado na variavel counts, que no final é salvo em um arquivo de texto contendo a contagem de cada palavra



HTTP requests to the NASA Kennedy Space Center WWW server
Fonte oficial do dateset: http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
Dados:
● Jul 01 to Jul 31, ASCII format, 20.7MB gzip compressed, 205.2MB.
● Aug 04 to Aug 31, ASCII format, 21.8MB gzip compressed, 167.8MB.
Sobre o dataset: Esses dois conjuntos de dados possuem todas as requisições HTTP para o servidor da NASA Kennedy

Questões
Responda as seguintes questões devem ser desenvolvidas em Spark utilizando a sua linguagem de preferência.


-- ambiente montando em uma maquina virtual com apenas 1 core do processador
-- foi utilizado o anaconda 3.4 com python 3.6 em um contexto com hadoop 2.7 e spark 2.2.1


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
for l in dados_arquivo.collect():
    l = l.split(" ")
    hosts.add(l[0])
print(len(hosts))
137979


###2. O total de erros 404.

print(dados_arquivo.filter(lambda l: '404' in l[:-1]).count())
28003


###3. Os 5 URLs que mais causaram erro 404.

urls = {}
for a in dados_arquivo.collect():
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
for a in dados_arquivo.collect():
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
for a in dados_arquivo.collect():
    a = a.split(" ")
    if a[-1].isdigit():
        total += int(a[-1])
print(total)
65524314915
