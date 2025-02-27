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


2. Instalar o Java 21
   * Instale qualquer versão do Java 21
   * Adicione o caminho do executável ao PATH
   * Verifique se o Java foi instalado corretamente:
     * `java -version`
     * `javac -version`
   * Usuários Linux podem utilizar o sdkman, que facilita a instalação e uso de diversas versões do Java:
     * `curl -s "https://get.sdkman.io" | bash`
     * Em um novo terminal: `source "$HOME/.sdkman/bin/sdkman-init.sh"`
     * `sdk install java 21.0.6-tem`
     * `sdk use java java 21.0.6-tem`


3. Executar o código
   * Clonar o repositório
   * Na pasta raiz, execute: `java -jar ./target/barreira_{barreira}.jar {argumentos...}` (substitua {...} pelo input que você deseja/precisa)
   * Note que para testar as barreiras adequadamente será necessário executar mais de uma instância do programa.
   
   3.1 Compilar/Executar o projeto (VS Code):
      * `mvn compile -f "/path/to/directory/zookeeper-lab.sit.dist./pom.xml"`
      * Alternativamente, pode usar a extensão Maven da sua IDE
   * Executar o código na IDE
      * Verificar o comando que aparecer no terminal, e então adicionar ao final os parâmetros necessários (especificados mais pra frente)
      * Exemplo ilustrativo:
         * Comando: `/usr/bin/env /path/to/.vscode/extensions/redhat.java-1.39.0-linux-x64/jre/21.0.5-linux-x86_64/bin/java @/tmp/cp_f3chrtah423t64ckmeklrkamt.argfile br.ufpa.OriginalBarrier`
         * Parâmetros: `localhost:2181 3`
        
   3.2 Compilar/Executar o projeto (IntelliJ):
      * Abra o projeto a partir da raiz pela IDE
      * Certifique-se que a IDE está configurada para utilizar o Java 21
      * Configure os executáveis no menu de configurações no canto superior direito
      * Execute cada executável com os parâmetros necessários (especificados mais pra frente)
      * OBS: O projeto já está com algumas configurações de execução prontas para o Intellij, basta clicar no botão de play na configuração desejada no canto superior direito

