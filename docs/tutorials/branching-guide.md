# Branching in ChronDB: A Practical Guide

One of ChronDB's most powerful features is its **branching** capability, inherited from its Git foundation. This guide will teach you how to use branches effectively in your applications.

## What Are Branches?

In ChronDB, branches are isolated environments that contain their own version of the database. Changes in one branch don't affect others until you explicitly merge them. This enables:

- Developing new features without affecting production data
- Testing changes in isolation
- Creating "what-if" scenarios
- Supporting multiple concurrent users with different views
- Providing tenant isolation in multi-tenant applications

## Branching Use Cases

Let's explore practical applications of branching:

### 1. Development Workflows

Create isolated environments for development → testing → production:

```
main (production)
   ↑
  test
   ↑
   dev
```

### 2. Feature Branches

Develop and test each feature in isolation:

```
main
 ↑ ↑ ↑
 │ │ └─ feature-C
 │ └─── feature-B
 └───── feature-A
```

### 3. Tenant Isolation

Give each tenant their own branch for complete isolation:

```
template
 ↑ ↑ ↑
 │ │ └─ tenant-C
 │ └─── tenant-B
 └───── tenant-A
```

### 4. Scenario Analysis

Create "what-if" scenarios for business analysis:

```
current
 ↑ ↑ ↑
 │ │ └─ scenario-high-growth
 │ └─── scenario-medium-growth
 └───── scenario-recession
```

## Getting Started with Branching

Let's walk through some practical examples:

### Setup

Ensure you have ChronDB running:

```bash
docker run -d --name chrondb \
  -p 3000:3000 -p 6379:6379 -p 5432:5432 \
  -v chrondb-data:/data \
  avelino/chrondb:latest
```

## Exercise 1: Creating a Development Branch

Let's create a real-world development workflow with branches:

### Using the Clojure API

```clojure
(require '[chrondb.core :as chrondb])

;; Create a connection to the main branch
(def main-db (chrondb/create-chrondb))

;; Add some production data
(chrondb/save main-db "product:101"
  {:name "Deluxe Widget"
   :price 29.99
   :in_stock 100})

;; Create a development branch
(def dev-db (chrondb/create-branch main-db "dev"))

;; The dev branch starts with all the data from main
(println "Initial dev product:" (chrondb/get dev-db "product:101"))

;; Make changes in the dev branch
(chrondb/save dev-db "product:101"
  {:name "Deluxe Widget Pro"  ;; Name changed
   :price 39.99               ;; Price increased
   :in_stock 100
   :features ["Enhanced UI", "Faster performance"]})  ;; New field

;; Add a new product only in dev
(chrondb/save dev-db "product:102"
  {:name "Super Widget"
   :price 49.99
   :in_stock 50})

;; Compare main and dev branches
(println "Main branch product:" (chrondb/get main-db "product:101"))
(println "Dev branch product:" (chrondb/get dev-db "product:101"))

;; In main, product:102 doesn't exist yet
(println "Product:102 in main:" (chrondb/get main-db "product:102"))
(println "Product:102 in dev:" (chrondb/get dev-db "product:102"))
```

### Using the REST API

```bash
# Create a dev branch
curl -X POST http://localhost:3000/api/v1/branches/dev \
  -H "Content-Type: application/json" \
  -d '{"source": "main"}'

# Add a product to main
curl -X POST http://localhost:3000/api/v1/documents/product:201 \
  -H "Content-Type: application/json" \
  -d '{"name": "Economy Widget", "price": 19.99, "in_stock": 200}'

# Update the product in dev branch
curl -X POST http://localhost:3000/api/v1/branches/dev/documents/product:201 \
  -H "Content-Type: application/json" \
  -d '{"name": "Economy Widget Plus", "price": 24.99, "in_stock": 200, "features": ["Value option"]}'

# Compare the versions
curl http://localhost:3000/api/v1/documents/product:201
curl http://localhost:3000/api/v1/branches/dev/documents/product:201
```

## Exercise 2: Testing with Branches

Let's simulate a testing workflow:

```clojure
;; Create a test branch from dev
(def test-db (chrondb/create-branch dev-db "test"))

;; QA discovers an issue and fixes the price in test
(chrondb/save test-db "product:101"
  (-> (chrondb/get test-db "product:101")
      (assoc :price 34.99)  ;; Price adjusted after review
      (assoc :features ["Enhanced UI", "Faster performance", "Bug fixes"])))

;; All three branches now have different versions
(println "Main price:" (:price (chrondb/get main-db "product:101")))  ;; 29.99
(println "Dev price:" (:price (chrondb/get dev-db "product:101")))    ;; 39.99
(println "Test price:" (:price (chrondb/get test-db "product:101")))  ;; 34.99
```

## Exercise 3: Merging Branches

After testing, let's merge our changes to the main branch:

### Using Clojure API

```clojure
;; First, merge test changes back to dev
(chrondb/merge-branch dev-db "test" "dev")

;; Verify the test fixes are now in dev
(println "Dev price after merge:" (:price (chrondb/get dev-db "product:101")))  ;; Should be 34.99

;; Now, merge dev to main for release
(chrondb/merge-branch main-db "dev" "main")

;; Verify the changes are in main
(println "Main after release:")
(println "Product 101:" (chrondb/get main-db "product:101"))
(println "Product 102:" (chrondb/get main-db "product:102"))
```

### Using REST API

```bash
# Merge test branch into dev
curl -X POST http://localhost:3000/api/v1/branches/test/merge/dev

# Merge dev branch into main (production release)
curl -X POST http://localhost:3000/api/v1/branches/dev/merge/main

# Verify the results
curl http://localhost:3000/api/v1/documents/product:201
```

## Exercise 4: Working with Multiple Users

Branching can isolate changes between different users:

```clojure
;; Create user-specific branches
(def alice-db (chrondb/create-branch main-db "user-alice"))
(def bob-db (chrondb/create-branch main-db "user-bob"))

;; Each user makes their own changes
(chrondb/save alice-db "shared-doc:1" {:title "Project Plan" :content "Alice's version"})
(chrondb/save bob-db "shared-doc:1" {:title "Project Plan" :content "Bob's version"})

;; Each user sees their own changes
(println "Alice sees:" (chrondb/get alice-db "shared-doc:1"))
(println "Bob sees:" (chrondb/get bob-db "shared-doc:1"))

;; Later, alice's changes are chosen as authoritative
(chrondb/merge-branch main-db "user-alice" "main")
(println "Final version:" (chrondb/get main-db "shared-doc:1"))
```

## Exercise 5: "What-If" Analysis for Business Scenarios

```clojure
;; Current financial data
(chrondb/save main-db "finance:2023-forecast"
  {:revenue 1000000
   :expenses 750000
   :growth_rate 0.05})

;; Create scenario branches
(def optimistic-db (chrondb/create-branch main-db "scenario-optimistic"))
(def pessimistic-db (chrondb/create-branch main-db "scenario-pessimistic"))

;; Model different scenarios
(chrondb/save optimistic-db "finance:2023-forecast"
  {:revenue 1200000
   :expenses 800000
   :growth_rate 0.15})

(chrondb/save pessimistic-db "finance:2023-forecast"
  {:revenue 800000
   :expenses 700000
   :growth_rate -0.05})

;; Compare scenarios
(def current (chrondb/get main-db "finance:2023-forecast"))
(def optimistic (chrondb/get optimistic-db "finance:2023-forecast"))
(def pessimistic (chrondb/get pessimistic-db "finance:2023-forecast"))

(println "Current profit:" (- (:revenue current) (:expenses current)))
(println "Optimistic profit:" (- (:revenue optimistic) (:expenses optimistic)))
(println "Pessimistic profit:" (- (:revenue pessimistic) (:expenses pessimistic)))
```

## Advanced Branching Patterns

### Multi-Tenant Architecture

In a multi-tenant SaaS application:

```clojure
;; Create a template with base structure
(def template-db (chrondb/create-chrondb))
(chrondb/save template-db "system:settings" {:default_theme "light", :features {:basic true, :advanced false}})

;; Create tenant-specific branches
(def tenant1-db (chrondb/create-branch template-db "tenant-acme"))
(def tenant2-db (chrondb/create-branch template-db "tenant-globex"))

;; Customize per tenant
(chrondb/save tenant1-db "system:settings"
  (-> (chrondb/get tenant1-db "system:settings")
      (assoc :company_name "Acme Inc")
      (assoc-in [:features :advanced] true)))

(chrondb/save tenant2-db "system:settings"
  (-> (chrondb/get tenant2-db "system:settings")
      (assoc :company_name "Globex Corp")
      (assoc :custom_domain "globex.example.com")))

;; Each tenant gets their own isolated environment
(println "Tenant 1 settings:" (chrondb/get tenant1-db "system:settings"))
(println "Tenant 2 settings:" (chrondb/get tenant2-db "system:settings"))

;; Update the template (affects new tenants but not existing ones)
(chrondb/save template-db "system:settings"
  (-> (chrondb/get template-db "system:settings")
      (assoc :default_theme "dark")))
```

### Feature Flagging with Branches

Test new features in isolation before enabling for all users:

```clojure
;; Main production branch
(def main-db (chrondb/create-chrondb))

;; Branch for testing new UI
(def new-ui-db (chrondb/create-branch main-db "feature-new-ui"))

;; Add some users to both branches
(chrondb/save main-db "user:1" {:name "Alice", :email "alice@example.com"})
(chrondb/save main-db "user:2" {:name "Bob", :email "bob@example.com"})

;; In the feature branch, enable the new UI flag
(chrondb/save new-ui-db "system:flags" {:new_ui_enabled true})

;; Function to decide which branch to use based on user
(defn get-user-db [user-id]
  (if (= user-id "1")
    ;; Alice gets the new UI (beta tester)
    new-ui-db
    ;; Everyone else gets the main branch
    main-db))

;; Simulate serving requests
(defn handle-request [user-id]
  (let [db (get-user-db user-id)
        user (chrondb/get db (str "user:" user-id))
        flags (chrondb/get db "system:flags")]
    {:user user
     :ui_version (if (:new_ui_enabled flags) "new" "classic")}))

(println "Alice's experience:" (handle-request "1"))
(println "Bob's experience:" (handle-request "2"))

;; When ready to launch for everyone, merge the feature branch
(chrondb/merge-branch main-db "feature-new-ui" "main")
```

## Best Practices for Branching

1. **Keep a Clean Main Branch**
   - Main should always contain stable, production-ready data
   - Only merge thoroughly tested changes

2. **Use Descriptive Branch Names**
   - `feature-new-ui`, `bugfix-pricing`, `tenant-acme`
   - Include tracking numbers: `feature-123-new-login`

3. **Regular Merges**
   - Regularly merge from main into development branches to prevent divergence
   - The longer branches exist separately, the harder merging becomes

4. **Delete Stale Branches**
   - Remove branches after merging to keep your branch list clean
   - Consider archiving important historical branches

5. **Automate Branch Management**
   - Use CI/CD pipelines to automate testing and merging
   - Integrate with your development workflow

## Conclusion

Branching in ChronDB offers powerful capabilities for development workflows, testing, and multi-tenant applications. By understanding how to create, modify, and merge branches, you can leverage ChronDB's full potential to create isolated environments while maintaining the ability to consolidate changes when needed.

## Next Steps

- [Time Travel Guide](./time-travel-guide) - Learn how to access historical versions
- [Multi-Tenant Patterns](./multi-tenant-patterns) - Advanced patterns for SaaS applications
- [Performance Considerations](../performance) - Optimize for branching operations

## Community Resources

- **Documentation**: [chrondb.avelino.run](https://chrondb.avelino.run/)
- **Questions & Help**: [Discord Community](https://discord.com/channels/1099017682487087116/1353399752636497992)
- **Share Your Experience**: [GitHub Discussions](https://github.com/avelino/chrondb/discussions)
