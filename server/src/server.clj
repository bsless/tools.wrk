(ns server
  (:require
   [org.httpkit.server :as http]))

(def resp
  {:status 200
   :body (.getBytes "ok")})

(defn handler
  ([_] resp)
  ([_ r _] (r resp)))

(def default-opts
  {:port 8080
   :ring-async? true
   :allow-virtual? true})

(defn start
  [opts]
  (->> opts
       (conj default-opts)
       http/new-worker
       :pool
       (assoc opts :worker-pool)
       (conj default-opts)
       (http/run-server handler)))

(comment
  (def srv (start nil)))
