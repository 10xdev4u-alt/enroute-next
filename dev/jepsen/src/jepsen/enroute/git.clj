(ns jepsen.enroute.git
  "Just enough of git's wire format to push commits and read refs over smart
  HTTP: object IDs, packs, and pkt-lines."
  (:require [clojure.string :as str])
  (:import (java.io ByteArrayOutputStream)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.security MessageDigest)
           (java.util Arrays HexFormat)
           (java.util.zip Deflater)))

(def empty-tree "4b825dc642cb6eb9a060e54bf8d69288fbee4904")
(def zero-oid (apply str (repeat 40 \0)))

(defn utf8 ^bytes [^String s] (.getBytes s StandardCharsets/UTF_8))
(defn utf8-str [^bytes b] (String. b StandardCharsets/UTF_8))

(defn hex [^bytes b] (.formatHex (HexFormat/of) b))

(defn concat-bytes ^bytes [& chunks]
  (let [out (ByteArrayOutputStream.)]
    (doseq [^bytes c chunks] (.write out c 0 (alength c)))
    (.toByteArray out)))

(defn sha1 ^bytes [^bytes b]
  (.digest (MessageDigest/getInstance "SHA-1") b))

(defn- deflate ^bytes [^bytes b]
  (let [d   (Deflater.)
        out (ByteArrayOutputStream.)
        buf (byte-array 4096)]
    (.setInput d b)
    (.finish d)
    (while (not (.finished d))
      (.write out buf 0 (.deflate d buf)))
    (.end d)
    (.toByteArray out)))

;; Objects

(defn commit-body
  "A commit of the empty tree. The key and value in the message make every
  commit unique, so a commit ID names one append."
  ^bytes [parent k v]
  (utf8 (str "tree " empty-tree "\n"
             (when parent (str "parent " parent "\n"))
             "author jepsen <jepsen@example.com> 0 +0000\n"
             "committer jepsen <jepsen@example.com> 0 +0000\n"
             "\nkey " k " value " v "\n")))

(defn object-id [^String type ^bytes body]
  (hex (sha1 (concat-bytes (utf8 (str type " " (alength body) "\u0000")) body))))

(def type-code {"commit" 1, "tree" 2})

(defn- object-header ^bytes [code size]
  (let [out (ByteArrayOutputStream.)]
    (loop [b (bit-or (bit-shift-left code 4) (bit-and size 0x0f))
           s (unsigned-bit-shift-right size 4)]
      (if (pos? s)
        (do (.write out (int (bit-or 0x80 b)))
            (recur (bit-and s 0x7f) (unsigned-bit-shift-right s 7)))
        (.write out (int b))))
    (.toByteArray out)))

(defn pack
  "A version 2 pack of `objects`, each a [type body] pair."
  ^bytes [objects]
  (let [head (-> (ByteBuffer/allocate 12)
                 (.put (utf8 "PACK")) (.putInt 2) (.putInt (count objects))
                 .array)
        body (apply concat-bytes head
                    (mapcat (fn [[type ^bytes b]]
                              [(object-header (type-code type) (alength b))
                               (deflate b)])
                            objects))]
    (concat-bytes body (sha1 body))))

(defn pack-object-count
  "The object count in a pack's header, or nil if `b` is not a pack."
  [^bytes b]
  (when (and (<= 12 (alength b))
             (= "PACK" (utf8-str (Arrays/copyOfRange b 0 4))))
    (.getInt (ByteBuffer/wrap b) 8)))

;; pkt-lines

(defn pkt ^bytes [^String s]
  (let [b (utf8 s)]
    (concat-bytes (utf8 (format "%04x" (+ 4 (alength b)))) b)))

(def flush-pkt (utf8 "0000"))
(def delim-pkt (utf8 "0001"))

(defn parse-pkts
  "The pkt-lines in `b`: byte arrays, or :flush, :delim, :end. A truncated
  line throws."
  [^bytes b]
  (loop [i 0, out (transient [])]
    (if (<= (alength b) i)
      (persistent! out)
      (let [_   (when (< (alength b) (+ i 4))
                  (throw (ex-info "truncated pkt-line" {:type :truncated})))
            len (Integer/parseInt (utf8-str (Arrays/copyOfRange b i (+ i 4))) 16)]
        (case (int len)
          0 (recur (+ i 4) (conj! out :flush))
          1 (recur (+ i 4) (conj! out :delim))
          2 (recur (+ i 4) (conj! out :end))
          (do (when (< (alength b) (+ i len))
                (throw (ex-info "truncated pkt-line" {:type :truncated})))
              (recur (+ i len)
                     (conj! out (Arrays/copyOfRange b (+ i 4) (+ i len))))))))))

(defn- text-lines [pkts]
  (->> pkts (filter bytes?) (map #(str/replace (utf8-str %) #"\n$" ""))))

;; Requests and responses

(defn ls-refs-request ^bytes []
  (concat-bytes (pkt "command=ls-refs\n") delim-pkt
                (pkt "ref-prefix refs/heads/\n") flush-pkt))

(defn parse-ls-refs
  "A map of branch name to oid from an ls-refs response."
  [^bytes body]
  (let [pkts (parse-pkts body)]
    (when-not (= :flush (last pkts))
      (throw (ex-info "ls-refs response has no flush" {:type :truncated})))
    (into {} (for [l (text-lines pkts)
                   :let [[oid ref] (str/split l #" ")]]
               [ref oid]))))

(defn parse-advertisement
  "A map of branch name to oid from a v0 ref advertisement."
  [^bytes body]
  (let [pkts (parse-pkts body)]
    (when-not (= :flush (last pkts))
      (throw (ex-info "advertisement has no flush" {:type :truncated})))
    (into {} (for [l (text-lines pkts)
                   :when (not (str/starts-with? l "# service="))
                   :let [[oid ref] (str/split (first (str/split l #"\u0000")) #" ")]
                   :when (str/starts-with? ref "refs/heads/")]
               [ref oid]))))

(defn push-request
  "A receive-pack body that moves each ref in `commands` ([old new ref])
  and carries `objects`."
  ^bytes [commands atomic? objects]
  (let [caps (str "report-status" (when atomic? " atomic") " agent=jepsen")
        lines (map-indexed
                (fn [i [old new ref]]
                  (pkt (str old " " new " " ref (when (zero? i) (str "\u0000" caps)) "\n")))
                commands)]
    (apply concat-bytes (concat lines [flush-pkt (pack objects)]))))

(defn parse-report
  "A report-status response as {:unpack s, :refs {ref [:ok] or [:ng reason]}}.
  Nil if the report is missing or truncated."
  [^bytes body]
  (try
    (let [pkts  (parse-pkts body)
          lines (text-lines pkts)]
      (when (and (= :flush (last pkts)) (seq lines)
                 (str/starts-with? (first lines) "unpack "))
        {:unpack (subs (first lines) 7)
         :refs   (into {} (for [l (rest lines)]
                            (if (str/starts-with? l "ok ")
                              [(subs l 3) [:ok]]
                              (let [[_ ref reason] (str/split l #" " 3)]
                                [ref [:ng reason]]))))}))
    (catch clojure.lang.ExceptionInfo _ nil)))

(defn fetch-request ^bytes [wants]
  (apply concat-bytes
         (concat [(pkt "command=fetch\n") delim-pkt (pkt "no-progress\n")]
                 (map #(pkt (str "want " % "\n")) wants)
                 [(pkt "done\n") flush-pkt])))

(defn parse-fetch
  "A fetch response as {:pack bytes} or {:error message}."
  [^bytes body]
  (let [pkts (parse-pkts body)
        err  (some #(when (and (bytes? %) (str/starts-with? (utf8-str %) "ERR "))
                      (str/trim (subs (utf8-str %) 4)))
                   pkts)]
    (if err
      {:error err}
      (let [band (fn [n] (->> pkts
                              (drop-while #(not (and (bytes? %)
                                                     (= "packfile\n" (utf8-str %)))))
                              rest
                              (filter #(and (bytes? %) (pos? (alength ^bytes %))
                                            (= n (aget ^bytes % 0))))
                              (map #(Arrays/copyOfRange ^bytes % 1 (alength ^bytes %)))))
            errs (band 3)]
        (if (seq errs)
          {:error (str/trim (utf8-str (apply concat-bytes errs)))}
          {:pack (apply concat-bytes (band 1))})))))
