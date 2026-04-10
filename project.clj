(defproject mikroservisna-arh-is-speakers "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.h2database/h2 "2.4.240"]
                 [com.github.seancorfield/next.jdbc "1.3.1093"]
                 [com.novemberain/langohr "5.6.0"]
                 [cheshire "6.2.0"]]
  :main ^:skip-aot mikroservisna-arh-is-speakers.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
