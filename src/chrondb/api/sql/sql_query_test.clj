(defn get-writer-output [writer-map]
  (.flush (:writer writer-map))
  (str (.getBuffer (:string-writer writer-map))))

(defn prepare-test-data [storage]
  (let [table-name "test"] ; Define table name for _table field
    (doseq [id ["1" "2" "3"]] ; Use plain IDs
      (let [num (Integer/parseInt id) ; Parse the plain ID
            doc {:id id ; Use plain ID
                 :_table table-name ; Add _table field
                 :nome (str "Item " num)
                 :valor (* num 10)
                 :ativo (odd? num)}]
        (storage-protocol/save-document storage doc))))) ; Save the updated doc structure