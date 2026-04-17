(ns metri.domain.core
  "Core domain logic and validations using Malli.")

(def greeting-schema
  [:map
   [:tenant-id :string]
   [:message :string]])

(defn generate-greeting [tenant-id]
  {:tenant-id tenant-id
   :message (str "Hello from Metri Engine, tenant " tenant-id)})
