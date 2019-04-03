(ns jepsen.atomic
  (:import [com.alipay.sofa.jraft.test.atomic.client AtomicClient]
           [com.alipay.sofa.jraft.test.atomic KeyNotFoundException]
           [com.alipay.sofa.jraft.entity PeerId]
           [knossos.model Model]
           [com.alipay.sofa.jraft JRaftUtils])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as cstr]
            [jepsen.checker.timeline :as timeline]
            [jepsen
             [nemesis :as nemesis]
             [checker :as checker]
             [independent :as independent]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [tests :as tests]]
            [knossos.model :as model]
            [jepsen.control.util :as cu]
            [jepsen.os :as os]))

(defonce atomic-path "/home/admin/atomic-server")
(defonce atomic-bin "java")
(defonce atomic-port 8609)
(defonce atomic-control "control.sh")
(defonce atomic-stop "stop.sh")

(defn peer-str [node]
  (str node ":" atomic-port))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo:8609,bar:8609,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (peer-str node)))
       (cstr/join ",")))

(defn- ^PeerId peer [node]
  (PeerId. node atomic-port))

(defn start! [node]
  (info "Start" node)
  (c/cd atomic-path
        (c/exec :sh
                atomic-control
                "start"
                "server.properties")))

(defn stop! [node]
  (info "Stop" node)
  (c/cd atomic-path
        (c/exec :sh
                atomic-stop)))

(defn join! [node test]
  (let [conf (->> test
                  :nodes
                  (remove #(= node %))
                  (map peer-str)
                  (cstr/join ","))
        node (peer-str node)]
    (info "Join" node)
    (c/cd atomic-path
          (c/exec :sh
                  atomic-control
                  "join"
                  node
                  conf)
          (c/exec :sleep 5))))

(defn leave! [node test]
  (let [conf (initial-cluster test)
        node (peer-str node)]
    (info "Join" node)
    (c/cd atomic-path
          (c/exec :sh
                  atomic-control
                  "leave"
                  node
                  conf)
          (c/exec :sleep 5))))

;;DB
(defn db
  "Atomic DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/cd atomic-path
            (c/exec* "cp test_server.properties server.properties"))
      (c/cd atomic-path
            (info (c/exec*
                   "echo serverAddress=`hostname`:8609 >> server.properties"))
            (info (c/exec*
                   (format "echo %s >> server.properties"
                           (str "conf=" (initial-cluster test)))))
            (info (c/exec*
                   (format "echo totalSlots=%d >> server.properties"
                           (:slots test)))))
      (start! node)
      (Thread/sleep 10000))

    (teardown! [_ test node]
      (stop! node)
      (Thread/sleep 5000)
      (c/cd atomic-path
            (c/exec :rm
                    :-rf
                    "data/")))))

;;client
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defonce read-from-quorum? (atom false))

(defn- read
  "Get counter from atomic server by key."
  [client key]
  (try
    (-> client
        :conn
        (.get key @read-from-quorum?))
    (catch KeyNotFoundException _
      0)))

(defn- write
  "write a key/value to atomic server"
  [client key value]
  (when-not (-> client
                :conn
                (.set key value))
    (throw (ex-info "Fail to set" {:key key :value value}))))

(defn- compare-and-set
  "Compare and set value for key"
  [client key old new]
  (-> client
      :conn
      (.compareAndSet key old new)))

(defn- add-and-get
  [client key delta]
  (-> client
      :conn
      (.addAndGet key delta)))

(defn- create-client0 [test]
  (doto (AtomicClient. "atomic"
                       (JRaftUtils/getConfiguration (initial-cluster test)))
    (.start)))

(def create-client (memoize create-client0))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (-> this
        (assoc :node node)
        (assoc  :conn (create-client test))))
  (setup! [this test])
  (invoke! [this test op]
    (let [[kk v] (:value op)
          k (str kk)
          crash (if (= :read (:f op)) :fail :info)]
      (try
        (case (:f op)
          :read (assoc op :type :ok :value (independent/tuple kk (read this k)))
          :write (do
                   (write this k v)
                   (assoc op :type :ok))
          :cas (let [[old new] v]
                 (assoc op :type (if (compare-and-set this k old new)
                                   :ok
                                   :fail))))
        (catch Exception e
          (let [^String msg (.getMessage e)]
            (cond
              (and msg (.contains msg "TIMEOUT")) (assoc op :type crash, :error :timeout)
              :else
              (assoc op :type crash :error (.getMessage e))))))))

  (teardown! [this test])

  (close! [this test]
    #_(-> this
          :conn
          (.shutdown))
    ))


(defrecord SingleKeyClient [k conn]
  client/Client
  (open! [this test node]
    (-> this
        (assoc :node node)
        (assoc  :conn (create-client test))))
  (setup! [this test])
  (invoke! [this test op]
    (let [v (:value op)
          crash (if (= :read (:f op)) :fail :info)]
      (try
        (case (:f op)
          :read (assoc op :type :ok :value (read this k))
          :write (do
                   (write this k v)
                   (assoc op :type :ok))
          :cas (let [[old new] v]
                 (assoc op :type (if (compare-and-set this k old new)
                                   :ok
                                   :fail))))
        (catch Exception e
          (let [^String msg (.getMessage e)]
            (cond
              (and msg (.contains msg "TIMEOUT")) (assoc op :type crash, :error :timeout)
              (and msg (.contains msg "Create connection failed")) (assoc op :type crash :error :disconnected)
              :else
              (assoc op :type crash :error (.getMessage e))))))))

  (teardown! [this test])

  (close! [this test]
    #_(-> this
          :conn
          (.shutdown))
    ))


(defn mostly-small-nonempty-subset
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.

      (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
           repeatedly
           (map count)
           (take 10000)
           frequencies
           sort)
      ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
  [xs]
  (-> xs
      count
      inc
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))))

(def crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  (nemesis/node-start-stopper
   mostly-small-nonempty-subset
   (fn start [test node] (stop! node) [:killed node])
   (fn stop  [test node] (start! node) [:restarted node])))

(def configuration-nemesis
  "A nemesis that add/remove a random node."
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node] (leave! node test) [:removed node])
   (fn stop  [test node] (join! node test) [:added node])))

(defn rand-key
  "Generate a random integer key."
  []
  (rand-int 100))

(defn atomic-test
  "Defaults for testing atomic."
  [name opts]
  (merge tests/noop-test
         {:name name
          :os   os/noop
          :db   (db "0.0.1")
          :client (Client. nil)
          :model      (model/cas-register 0)
          :checker  (checker/compose
                     {:perf  (checker/perf)
                      :indep (independent/checker
                              (checker/compose
                               {:timeline (timeline/html)
                                :linear (checker/linearizable)}))})
          }
         opts))

(defn create-test
  "A generic create test."
  [name opts]
  (atomic-test (str "atomic." name)
               opts))

(defn recover
  "A generator which stops the nemesis and allows some time for recovery."
  []
  (gen/nemesis
   (gen/phases
    (gen/once {:type :info, :f :stop})
    (gen/sleep 20))))

(defn read-once
  "A generator which reads exactly once."
  []
  (gen/clients
   (gen/once {:type :invoke, :f :read})))

(defn partition-test
  "Cuts the network into randomly chosen halves."
  [opts]
  (create-test "partition"
               (merge opts
                      {:nemesis    (nemesis/partition-random-halves)
                       :generator (->> (independent/concurrent-generator
                                        (:concurrency opts 5)
                                        (range)
                                        (fn [k]
                                          (->> (gen/mix [r w cas])
                                               (gen/stagger (/ (:rate opts)))
                                               (gen/limit (:ops-per-key opts)))))
                                       (gen/nemesis
                                        (gen/seq (cycle [(gen/sleep 5)
                                                         {:type :info, :f :start}
                                                         (gen/sleep 5)
                                                         {:type :info, :f :stop}])))
                                       (gen/time-limit (:time-limit opts)))})))

(defn crash-test
  "killing random nodes and restarting them."
  [opts]
  (create-test "crash"
               (merge opts
                      {:nemesis   crash-nemesis
                       :client (SingleKeyClient. "a" nil)
                       :checker    (checker/compose
                                    {:perf      (checker/perf)
                                     :linear    (checker/linearizable)
                                     :timeline  (timeline/html)})
                       :generator (gen/phases
                                   (->> gen/cas
                                        (gen/stagger 1/10)
                                        (gen/delay 1/10)
                                        (gen/nemesis
                                         (gen/seq
                                          (cycle [(gen/sleep 10)
                                                  {:type :info :f :start}
                                                  (gen/sleep 10)
                                                  {:type :info :f :stop}])))
                                        (gen/time-limit 120))
                                   (recover)
                                   (read-once))})))

(defn pause-test
  "pausing random node with SIGSTOP/SIGCONT."
  [opts]
  (create-test "pause"
               (merge
                {:nemesis   (nemesis/hammer-time atomic-bin)
                 :client (SingleKeyClient. "a" nil)
                 :checker    (checker/compose
                              {:perf      (checker/perf)
                               :linear    (checker/linearizable)
                               :timeline  (timeline/html)})
                 :generator (gen/phases
                             (->> gen/cas
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                   (gen/seq
                                    (cycle [(gen/sleep 10)
                                            {:type :info :f :start}
                                            (gen/sleep 10)
                                            {:type :info :f :stop}])))
                                  (gen/time-limit 120))
                             (recover)
                             (read-once))}
                opts)))


(defn bridge-test
  "weaving the network into happy little intersecting majority rings"
  [opts]
  (create-test "bridge"
               (merge
                {:nemesis   (nemesis/partitioner (comp nemesis/bridge shuffle))
                 :client (SingleKeyClient. "a" nil)
                 :checker    (checker/compose
                              {:perf      (checker/perf)
                               :linear    (checker/linearizable)
                               :timeline  (timeline/html)})
                 :generator (gen/phases
                             (->> gen/cas
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                   (gen/seq
                                    (cycle [(gen/sleep 10)
                                            {:type :info :f :start}
                                            (gen/sleep 10)
                                            {:type :info :f :stop}])))
                                  (gen/time-limit 120))
                             (recover)
                             (read-once))}
                opts)))

(defn partition-majority-test
  "Cuts the network into randomly majority groups."
  [opts]
  (create-test "partition-majority"
               (merge
                {:nemesis   (nemesis/partition-majorities-ring)
                 :client (SingleKeyClient. "a" nil)
                 :checker    (checker/compose
                              {:perf      (checker/perf)
                               :linear    (checker/linearizable)
                               :timeline  (timeline/html)})
                 :generator (gen/phases
                             (->> gen/cas
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                   (gen/seq
                                    (cycle [(gen/sleep 10)
                                            {:type :info :f :start}
                                            (gen/sleep 10)
                                            {:type :info :f :stop}])))
                                  (gen/time-limit 120))
                             (recover)
                             (read-once))}
                opts)))

(defn configuration-test
  "remove and add a random node."
  [opts]
  (create-test "configuration"
               (merge {:nemesis   configuration-nemesis
                       :client (SingleKeyClient. "a" nil)
                       :checker    (checker/compose
                                    {:perf      (checker/perf)
                                     :linear    (checker/linearizable)
                                     :timeline  (timeline/html)})
                       :generator (gen/phases
                                   (->> gen/cas
                                        (gen/stagger 1/10)
                                        (gen/delay 1/10)
                                        (gen/nemesis
                                         (gen/seq
                                          (cycle [(gen/sleep 10)
                                                  {:type :info :f :start}
                                                  (gen/sleep 10)
                                                  {:type :info :f :stop}])))
                                        (gen/time-limit 120))
                                   (recover)
                                   (read-once))}
                      opts)))


(defn- parse-long [s]
  (Long/parseLong s))

(defn- parse-boolean [s]
  (Boolean/parseBoolean s))

(def cli-opts
  "Additional command line options."
  [["-s" "--slots SLOTS" "Number of atomic server ranges."
    :default  1
    :parse-fn parse-long
    :validate [pos? "Must be a positive number"]]
   ["-q" "--quorum BOOL" "Whether to read from quorum."
    :default  false
    :parse-fn parse-boolean
    :validate [boolean? "Must be a boolean value."]]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

(def fn-opts
  [[nil "--testfn TEST" "Test function name."]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (ns jepsen.atomic)
  (let [opts (parse-opts args (concat fn-opts cli-opts) :no-defaults true)
        test-fn (if-let [tfs (->> opts :options :testfn)]
                  (->> tfs symbol (resolve))
                  partition-test)]
    (when (->> opts :options :quorum)
      (info "Use read from quorum for test.")
      (reset! read-from-quorum? true))
    (info "Begin to test" test-fn)
    (cli/run! (merge (cli/single-test-cmd {:test-fn test-fn
                                           :opt-spec (conj cli-opts
                                                           [nil "--testfn TEST" "Test function name."
                                                            :default  ""]
                                                           )})
                     (cli/serve-cmd))
              args)))
