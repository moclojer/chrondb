(ns chrondb.api.sql.protocol.constants
  "Constants related to the PostgreSQL protocol")

;; Protocol Versions
(def PG_PROTOCOL_VERSION 196608)  ;; Protocol version 3.0

;; Message Types
(def PG_ERROR_RESPONSE (byte (int \E)))
(def PG_NOTICE_RESPONSE (byte (int \N)))  ;; Added for welcome messages
(def PG_READY_FOR_QUERY (byte (int \Z)))
(def PG_ROW_DESCRIPTION (byte (int \T)))
(def PG_DATA_ROW (byte (int \D)))
(def PG_COMMAND_COMPLETE (byte (int \C)))
(def PG_AUTHENTICATION_OK (byte (int \R)))
(def PG_PARAMETER_STATUS (byte (int \S)))
(def PG_BACKEND_KEY_DATA (byte (int \K)))

;; Constants for the SQL Parser
(def RESERVED_WORDS #{"select" "from" "where" "group" "by" "order" "having"
                      "limit" "offset" "insert" "update" "delete" "set" "values"
                      "into" "and" "or" "not" "in" "like" "between" "is" "null"
                      "as" "join" "inner" "left" "right" "outer" "on"})

(def AGGREGATE_FUNCTIONS #{"count" "sum" "avg" "min" "max"})

(def COMPARISON_OPERATORS #{"=" "!=" "<>" ">" "<" ">=" "<=" "like" "in"})

(def LOGICAL_OPERATORS #{"and" "or" "not"})