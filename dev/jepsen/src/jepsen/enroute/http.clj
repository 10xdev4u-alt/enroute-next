(ns jepsen.enroute.http
  "A small HTTP client on java.net.http."
  (:refer-clojure :exclude [get])
  (:import (java.net URI)
           (java.net.http HttpClient HttpClient$Builder HttpClient$Version
                          HttpRequest HttpRequest$Builder HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.time Duration)
           (com.fasterxml.jackson.databind ObjectMapper)))

(def ^HttpClient client
  (-> (HttpClient/newBuilder)
      (.version HttpClient$Version/HTTP_1_1)
      ;; JAVA_TOOL_OPTIONS can set a proxy; the nodes are local.
      (.proxy HttpClient$Builder/NO_PROXY)
      (.connectTimeout (Duration/ofSeconds 2))
      .build))

(defn- send! [^HttpRequest$Builder b {:keys [timeout headers]}]
  (doseq [[k v] headers] (.header b k v))
  (.timeout b (Duration/ofMillis (or timeout 10000)))
  (let [r (.send client (.build b) (HttpResponse$BodyHandlers/ofByteArray))]
    {:status (.statusCode r), :body (.body r)}))

(defn get [url opts]
  (send! (.GET (HttpRequest/newBuilder (URI/create url))) opts))

(defn post [url ^bytes body opts]
  (send! (.POST (HttpRequest/newBuilder (URI/create url))
                (HttpRequest$BodyPublishers/ofByteArray body))
         opts))

(def ^:private ^ObjectMapper json (ObjectMapper.))

(defn get-json
  "The JSON object at `url`, with keyword keys, or nil if the status is not
  200."
  [url opts]
  (let [res (get url opts)]
    (when (= 200 (:status res))
      (into {} (map (fn [[k v]] [(keyword k) v]))
            (.readValue json ^bytes (:body res) java.util.Map)))))
