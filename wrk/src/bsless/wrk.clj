(ns bsless.wrk
  (:require
   [babashka.process :as p]
   [babashka.fs :as fs]
   [clojure.string :as str]
   [clojure.pprint :as pp]
   [clojure.java.io :as io]))

;; Usage: wrk <options> <url>
;;   Options:
;;     -c, --connections <N>  Connections to keep open
;;     -d, --duration    <T>  Duration of test
;;     -t, --threads     <N>  Number of threads to use

;; -s, --script      <S>  Load Lua script file
;; -H, --header      <H>  Add header to request
;; -L  --latency          Print latency statistics
;; -U  --u_latency        Print uncorrected latency statistics
;;     --timeout     <T>  Socket/request timeout
;; -B, --batch_latency    Measure latency of whole
;;                        batches of pipelined ops
;;                        (as opposed to each op)
;; -v, --version          Print version details
;; -R, --rate        <T>  work rate (throughput)
;;                        in requests/sec (total)
;;                        [Required Parameter]


;; Numeric arguments may include a SI unit (1k, 1M, 1G)
;; Time arguments may include a time unit (2s, 2m, 2h)

(defn debug
  [x {:keys [debug?]}]
  (when debug? (println x)))

(def parse-xf
  (comp (drop-while #(not= "  Detailed Percentile spectrum:" %))
     (drop 3)
     (take-while #(not (str/starts-with? % "#[Mean")))
     (map #(let [[_ v p n p-1] (str/split % #"\s+")]
             {:value (parse-double v)
              :percentile (parse-double p)
              :count (parse-long n)
              :-percentile (or (parse-double p-1) Double/POSITIVE_INFINITY)}))))

(defn parse
  [{:keys [out exit]}]
  (assert (zero? exit))
  (with-meta (into [] parse-xf (str/split-lines out)) {:raw out}))

(defn pprint
  [data writer]
  (pp/pprint data writer))

(defn table
  [data writer]
  (binding [*out* writer]
    (pp/print-table [:value :percentile :count :-percentile] (:results data))))

(defn noop-print [_ _])

(defn raw-print
  [data ^java.io.Writer writer]
  (.write writer (-> data :results meta :raw)))

(defn- write-results [results writer]
  ((case (:printer results)
     :pprint pprint
     :table table
     :noop noop-print
     :raw raw-print
     pprint) (dissoc results :printer) writer))

(defn write* [results opts writer]
  (-> opts
      (dissoc :output)
      (assoc :results results)
      (doto (write-results writer))))

(defn write
  [results {:keys [output] :as opts}]
  (if output
    (do
      (fs/create-dirs (fs/parent output))
      (with-open [fw (io/writer (io/file output))]
        (write* results opts fw)))
    (write* results opts *out*)))

(defn wrk
  [& {:keys [connections duration threads rate wrk url]
      :as opts
      :or {rate 1000
           wrk "wrk2"}}]
  (assert (string? url))
  (assert (nat-int? rate))
  (-> wrk
      (str " --rate " rate)
      (str " --latency ")
      (cond->
          connections (str " --connections " connections)
          duration (str " --duration " duration "s")
          threads (str " --threads " threads))
      (str " " url)
      (doto (debug opts))
      p/sh
      parse
      (write (assoc opts :rate rate))))

(comment
  (def ret (wrk  :url "http://localhost:8080" :output "/tmp/a/b/c/foo.edn")))

(defn avg
  [x y]
  (long (/ (+ x y) 2)))

(defn find-wall
  [res wall-percentile]
  (:value (first (filter #(or (= wall-percentile (:percentile %))
                              (< wall-percentile (:percentile %))) res))))

(defn high?
  [{:keys [results]} {:keys [wall-percentile wall-value]}]
  (<= wall-value (find-wall results wall-percentile)))

(defn low?
  [{:keys [results]} {:keys [wall-percentile wall-value]}]
  (> wall-value (find-wall results wall-percentile)))

(defn good-enough? [rate prev-rate {:keys [good-enough-rate]}]
  (>= good-enough-rate (abs (- 1 (/ rate prev-rate)))))

(comment (good-enough? 10000 12000 {:good-enough-rate 0.1}))

(def default-search-opts
  {:good-enough-rate 0.05
   :wall-percentile 0.99
   :wall-value 10})

(def default-wrk-opts
  {:rate 1000})

(defn search
  "Binary search best rate for server using wrk"
  ([opts]
   (search (merge default-wrk-opts opts) (merge default-search-opts opts)))
  ([{:keys [rate] :as wrk-opts} search-opts]
   (loop [rate rate]
     (let [res (wrk (assoc wrk-opts :rate rate))]
       (if (high? res search-opts)
         (loop [high-res res
                low-res res
                high rate
                low (/ rate 2)]
           (let [med (avg low high)]
             (if (good-enough? high low search-opts)
               low-res
               (let [res (wrk (assoc wrk-opts :rate med))]
                 (cond
                   (low? res search-opts)  (recur high-res res high med)
                   (high? res search-opts) (recur res low-res med low))))))
         (recur (* 2 rate)))))))

(comment
  (def res (search {:url "http://localhost:8080" :debug? true :duration 30})))
