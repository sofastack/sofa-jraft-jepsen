(defcluster :jraft
  :clients [{:host "127.0.0.65" :user "root"}
            {:host "127.0.0.102" :user "root"}
            {:host "127.0.0.89" :user "root"}])

(deftask :date "echo date on cluster"  []
  (ssh "date"))

(deftask :build []
  (local
   (run
     (cd "atomic-server"
         (run
           "lein clean ; lein uberjar"))))
  (local (run "rm atomic-server.tar.gz; tar zcvf atomic-server.tar.gz atomic-server/target atomic-server/control.sh atomic-server/stop.sh atomic-server/test_server.properties")))


(deftask :deploy []
  (scp "atomic-server.tar.gz" "/home/admin/")
  (ssh
     (run
       (cd "/home/admin"
           (run "rm -rf atomic-server/")
           (run "tar zxvf atomic-server.tar.gz")))))
