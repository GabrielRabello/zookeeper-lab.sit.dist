## Preparação local
1. Ativar o Apache Zookeeper
   * Instalar: https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz
   * Extrair o pacote e ir até a pasta pelo terminal
   * Definir o arquivo de configuração:
     * `cp conf/zoo_sample.cfg conf/zoo.cfg`
   * Ativar o servidor:
     * `bin/zkServer.sh start`
   * Checar se o a servidor foi ativado:
     * `bin/zkCli.sh -server localhost:2181`

2. Executar o código
   * Clonar o repositório
   * Compilar o projeto:
      * `mvn compile -f "/path/to/directory/zookeeper-lab.sit.dist./pom.xml"`
      * Alternativamente, pode usar a extensão Maven da sua IDE
   * Executar o código na IDE
      * Verificar o comando que aparecer no terminal, e então adicionar ao final os parâmetros necessários (especificados mais pra frente)
      * Exemplo ilustrativo:
         * Comando: `/usr/bin/env /path/to/.vscode/extensions/redhat.java-1.39.0-linux-x64/jre/21.0.5-linux-x86_64/bin/java @/tmp/cp_f3chrtah423t64ckmeklrkamt.argfile br.ufpa.SyncPrimitive`
         * Parâmetros: `qTest localhost:2181 3 c`

</br>

## Funcionamento 

### SyncPrimitive
### Barreira (Barrier)
### Fila Produtor-Consumidor (Queue)
