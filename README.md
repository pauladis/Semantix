<h2>Semantix</h2>

<b>Qual o objetivo do comando cache em Spark?</b>

Grande parte das operações de um RDD são apenas em uma abstração para um conjunto de instruções a serem realizadas, para essas operações nod damos o nome de lazy, estas operações só serão executadas após uma instrução de ação, que só podem ser validadas depois dos valores da RDD, com isso, alguns codigos iterativos se tornam ineficientes, pois um comando de ação encadeado irá disparar todas as operações lazy repetitivamente, para isto utilizamos o comando cache que permite que o resultado de operações lazy sejam armazenados e reutilizados


<b>O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?</b>

Para cada execução do MapReduce é criado uma nova instancia do JVM alem do reultado ser escrito em disco, necessitando da leitura quando passado para um job seguinte, enquanto no Spark, a JVM está constantemente em execução e permite que os resultados sejam passados diretamente entre as proximas operações a serem executadas por meio do cache em memoria, oque possibilita que diversas operações seguintes utilizem um mesmo conjunto de dados em cache, não sendo necessario a escrita e leitura do mesmo


<b>Qual é a função do SparkContext ?</b>

SparkContext é um cliente do ambiente de execução do Spark, nele passamos alguns parametros para alocação de recursos, e atraves dele que criamos as nossas RDDs, executar jobs e criar acumuladores


<b>Explique com suas palavras o que é Resilient Distributed Datasets (RDD)</b>

RDDs são uma abstração dos dados em Spark, são tolerantes a falha, através da recomputação das partes afetadas em eventuais problemas em algum nó, devido ao fato de serem distribuidas atraves do cluster em quantas partes forem necessarias, e para cada fração da RDD em um nó, a mesma é replicada em outro nó, a fim de manter a integralidade, RDDs são imutaveis e são operados em paralelo possibilitando a execução de diferentes partes de um RDD ao mesmo tempo


<b>GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?</b>

Ao realizar uma agragação por reduceByKey, o Spark realiza a operação passada como parametro para todos os elementos com a mesma chave em cada partição, a fim de obter um resultado parcial que vai ser passado para o executor realizar o calculo final, já o groupByKey vai realizar a aplicação e agragação dos dados sem calcular um resultado parcial, sendo assim a quantidade de dados a serem transmitidos é maior, e pode ocorrer o overflow da memoria necessitando da escrita em disco, danificando assim a performance


<b>** Explique o que o código Scala abaixo faz **</b>

1. val textFile = sc . textFile ( "hdfs://..." )
2. val counts = textFile . flatMap ( line => line . split ( " " ))
3.           . map ( word => ( word , 1 ))
4.           . reduceByKey ( _ + _ )
5. counts . saveAsTextFile ( "hdfs://..." )

primeiro lemos um arquivo de texto do hadoop, logo em seguida cada linha do arquivo passa por um split que separa o conteudo atraves de cada espaço em branco, então cada palavra é mapeada em um dicionario de key/value, sendo que a key é a propria palavra e seu value é 1, após isso os valores são agregados de acordo com a key somando seus valores, e tudo isto é armazenado na variavel counts, que no final é salvo em um arquivo de texto contendo a contagem de cada palavra
