(ns chrondb.api.sql.protocol.messages
  "Funções para criar e enviar mensagens do protocolo PostgreSQL"
  (:require [chrondb.api.sql.protocol.constants :as constants]
            [chrondb.util.logging :as log])
  (:import [java.io OutputStream InputStream DataOutputStream DataInputStream]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]))

(defn write-bytes
  "Escreve um array de bytes para um stream de saída e realiza flush.
   Parâmetros:
   - out: O stream de saída
   - data: O array de bytes para escrever
   Retorna: nil"
  [^OutputStream out ^bytes data]
  (.write out data 0 (alength data))
  (.flush out))

(defn write-message
  "Escreve uma mensagem do protocolo PostgreSQL para um stream de saída.
   Parâmetros:
   - out: O stream de saída
   - type: O tipo da mensagem (um byte)
   - data: O conteúdo da mensagem como um array de bytes
   Retorna: nil"
  [^OutputStream out type ^bytes data]
  (try
    (let [msg-length (+ 4 (alength data))
          dos (DataOutputStream. out)]
      ;; Tipo da mensagem (1 byte)
      (.writeByte dos (int type))

      ;; Tamanho da mensagem (int - 4 bytes) em formato Big Endian (padrão Java)
      (.writeInt dos msg-length)

      ;; Escreve o conteúdo da mensagem
      (.write dos data 0 (alength data))
      (.flush dos))
    (catch Exception e
      (log/log-error (str "Erro ao escrever mensagem: " (.getMessage e))))))

(defn string-to-bytes
  "Converte uma string para um array de bytes terminado em null.
   Parâmetros:
   - s: A string a ser convertida
   Retorna: Um array de bytes contendo os bytes da string seguido por um terminador null"
  [s]
  (let [s-bytes (.getBytes s StandardCharsets/UTF_8)
        result (byte-array (inc (alength s-bytes)))]
    ;; Copia a string original
    (System/arraycopy s-bytes 0 result 0 (alength s-bytes))
    ;; Adiciona o terminador null
    (aset-byte result (alength s-bytes) (byte 0))
    result))

(defn read-startup-message
  "Lê a mensagem de inicialização de um cliente PostgreSQL.
   Parâmetros:
   - in: O stream de entrada para ler
   Retorna: Um mapa contendo versão do protocolo, nome do banco de dados e nome de usuário"
  [^InputStream in]
  (try
    (let [dis (DataInputStream. in)
          ;; Lê o tamanho da mensagem (primeiro campo - 4 bytes)
          msg-length (.readInt dis)]
      (when (> msg-length 0)
        (let [content-length (- msg-length 4)
              buffer (byte-array content-length)]
          ;; Lê o resto da mensagem
          (.readFully dis buffer 0 content-length)

          ;; Registra os bytes recebidos para debug
          (log/log-debug (str "Bytes da mensagem de inicialização recebidos: " (count buffer) " bytes"))

          ;; Sempre retorna valores padrão para evitar falhas de análise
          {:protocol-version constants/PG_PROTOCOL_VERSION
           :database "chrondb"
           :user "chrondb"})))
    (catch Exception e
      (log/log-error (str "Erro ao ler mensagem de inicialização: " (.getMessage e)))
      ;; Retorna valores padrão para permitir que o processo continue
      {:protocol-version constants/PG_PROTOCOL_VERSION
       :database "chrondb"
       :user "chrondb"})))

(defn send-authentication-ok
  "Envia uma mensagem de autenticação OK para o cliente.
   Parâmetros:
   - out: O stream de saída
   Retorna: nil"
  [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 4)]
    (.putInt buffer 0)  ;; Autenticação bem-sucedida (0)
    (write-message out constants/PG_AUTHENTICATION_OK (.array buffer))))

(defn send-parameter-status
  "Envia uma mensagem de status de parâmetro para o cliente.
   Parâmetros:
   - out: O stream de saída
   - param: O nome do parâmetro
   - value: O valor do parâmetro
   Retorna: nil"
  [^OutputStream out param value]
  (let [buffer (ByteBuffer/allocate 1024)
        param-bytes (.getBytes param StandardCharsets/UTF_8)
        value-bytes (.getBytes value StandardCharsets/UTF_8)]

    ;; Adiciona o nome do parâmetro
    (.put buffer param-bytes)
    (.put buffer (byte 0))  ;; Terminador null para o nome

    ;; Adiciona o valor do parâmetro
    (.put buffer value-bytes)
    (.put buffer (byte 0))  ;; Terminador null para o valor

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_PARAMETER_STATUS final-data))))

(defn send-backend-key-data
  "Envia dados de chave de backend para o cliente.
   Parâmetros:
   - out: O stream de saída
   Retorna: nil"
  [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 8)]
    ;; ID do processo (4 bytes)
    (.putInt buffer 12345)
    ;; Chave secreta (4 bytes)
    (.putInt buffer 67890)
    (write-message out constants/PG_BACKEND_KEY_DATA (.array buffer))))

(defn send-ready-for-query
  "Envia uma mensagem de pronto para consulta para o cliente.
   Parâmetros:
   - out: O stream de saída
   Retorna: nil"
  [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 1)]
    ;; Status: 'I' = inativo (pronto para consultas)
    (.put buffer (byte (int \I)))
    (write-message out constants/PG_READY_FOR_QUERY (.array buffer))))

(defn send-error-response
  "Envia uma mensagem de resposta de erro para o cliente.
   Parâmetros:
   - out: O stream de saída
   - message: A mensagem de erro
   Retorna: nil"
  [^OutputStream out message]
  (let [buffer (ByteBuffer/allocate 1024)]
    ;; Adiciona campos de erro
    (.put buffer (byte (int \S)))   ;; Identificador de campo de severidade
    (.put buffer (.getBytes "ERROR" StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte (int \C)))  ;; Identificador de campo de código
    (.put buffer (.getBytes "42501" StandardCharsets/UTF_8))  ;; Permissão negada (para UPDATE/DELETE sem WHERE)
    (.put buffer (byte 0))

    (.put buffer (byte (int \M)))  ;; Identificador de campo de mensagem
    (.put buffer (.getBytes message StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte (int \H)))  ;; Identificador de campo de dica
    (.put buffer (.getBytes "Adicione uma cláusula WHERE com um ID específico" StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte (int \P)))  ;; Identificador de campo de posição
    (.put buffer (.getBytes "1" StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte 0))  ;; Terminador final

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_ERROR_RESPONSE final-data))))

(defn send-command-complete
  "Envia uma mensagem de comando completo para o cliente.
   Parâmetros:
   - out: O stream de saída
   - command: O comando que foi executado (por exemplo, 'SELECT', 'INSERT')
   - rows: O número de linhas afetadas
   Retorna: nil"
  [^OutputStream out command rows]
  (let [buffer (ByteBuffer/allocate 128)
        ;; O formato depende do comando
        command-str (if (= command "INSERT")
                      ;; Para INSERT, o formato deve ser "INSERT 0 1" onde 0 é o OID e 1 é o número de linhas
                      "INSERT 0 1"
                      (str command " " rows))]
    (log/log-debug (str "Enviando comando completo: " command-str))
    (.put buffer (.getBytes command-str StandardCharsets/UTF_8))
    (.put buffer (byte 0))  ;; Terminador null para a string

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_COMMAND_COMPLETE final-data))))

(defn send-row-description
  "Envia uma mensagem de descrição de linha para o cliente.
   Parâmetros:
   - out: O stream de saída
   - columns: Uma sequência de nomes de colunas
   Retorna: nil"
  [^OutputStream out columns]
  (let [buffer (ByteBuffer/allocate 1024)]
    ;; Número de campos (2 bytes)
    (.putShort buffer (short (count columns)))

    ;; Informações para cada coluna
    (doseq [col columns]
      ;; Nome da coluna (string terminada em null)
      (.put buffer (.getBytes col StandardCharsets/UTF_8))
      (.put buffer (byte 0))

      ;; OID da tabela (4 bytes)
      (.putInt buffer 0)

      ;; Número do atributo (2 bytes)
      (.putShort buffer (short 0))

      ;; OID do tipo de dados (4 bytes) - VARCHAR
      (.putInt buffer 25)  ;; TEXT

      ;; Tamanho do tipo (2 bytes)
      (.putShort buffer (short -1))

      ;; Modificador de tipo (4 bytes)
      (.putInt buffer -1)

      ;; Código de formato (2 bytes) - 0=texto
      (.putShort buffer (short 0)))

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_ROW_DESCRIPTION final-data))))

(defn send-data-row
  "Envia uma mensagem de linha de dados para o cliente.
   Parâmetros:
   - out: O stream de saída
   - values: Uma sequência de valores para a linha
   Retorna: nil"
  [^OutputStream out values]
  (let [buffer (ByteBuffer/allocate (+ 2 (* (count values) 256)))]  ;; Tamanho estimado
    ;; Número de valores na linha
    (.putShort buffer (short (count values)))

    ;; Cada valor
    (doseq [val values]
      (if val
        (let [val-str (str val)
              val-bytes (.getBytes val-str StandardCharsets/UTF_8)]
          ;; Tamanho do valor
          (.putInt buffer (alength val-bytes))
          ;; Valor
          (.put buffer val-bytes))
        ;; Valor nulo
        (.putInt buffer -1)))

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_DATA_ROW final-data))))