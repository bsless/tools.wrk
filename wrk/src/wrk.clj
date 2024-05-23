(ns wrk
  (:require
   [babashka.process :as p]
   [babashka.fs :as fs]
   [clojure.string :as str]
   [clojure.pprint :as pp]
   [clojure.java.io :as io]
   [clojure.math :as math]))

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

(defn parse
  [{:keys [out exit]}]
  (assert (zero? exit))
  (->> out
       str/split-lines
       (drop-while #(not= "  Detailed Percentile spectrum:" %))
       (drop 3)
       (take-while #(not (str/starts-with? % "#[Mean")))
       (mapv #(let [[_ v p n p-1] (str/split % #"\s+")]
                {:value (parse-double v)
                 :percentile (parse-double p)
                 :count (parse-long n)
                 :-percentile (or (parse-double p-1) Double/POSITIVE_INFINITY)}))))

(defn write
  [results {:keys [output] :as opts}]
  (when output
    (fs/create-dirs (fs/parent output)))
  (with-open [fw (or (and output (io/writer (io/file output)))
                     *out*)]
    (-> opts
        (dissoc :output)
        (assoc :results results)
        (pp/pprint fw)))
  results)

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
      (doto println)
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
  [res {:keys [wall-percentile wall-value]}]
  (< wall-value (find-wall res wall-percentile)))

(defn low?
  [res {:keys [wall-percentile wall-value]}]
  (> wall-value (find-wall res wall-percentile)))

(defn good-enough? [rate prev-rate {:keys [good-enough-rate]}]
  (>= good-enough-rate (abs (- 1 (/ rate prev-rate)))))

(comment (good-enough? 10000 12000 {:good-enough-rate 0.1}))

(defn search
  [wrk-opts search-opts]
  (loop [rate 1000]
    (let [res (wrk (assoc wrk-opts :rate rate))]
      (if (high? res search-opts)
        (loop [high rate
               low (/ rate 2)]
          (let [med (avg low high)
                res (wrk (assoc wrk-opts :rate med))]
            (cond
              (good-enough? high med search-opts) res
              (low? res search-opts) (recur high med)
              (high? res search-opts) (recur med low))))
        (recur (* 2 rate))))))

(comment
  (search {:url "http://localhost:8080"} {:good-enough-rate 0.1 :wall-percentile 0.9 :wall-value 10}))
