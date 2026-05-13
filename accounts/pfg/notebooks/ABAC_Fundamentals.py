# Databricks notebook source
# MAGIC %md
# MAGIC # ABAC Fundamentals
# MAGIC
# MAGIC Attribute-Based Access Control (ABAC) primer on Databricks Unity Catalog —
# MAGIC row filters, column masks, and policy tags driven by user/group attributes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. RBAC, briefly
# MAGIC
# MAGIC Role-Based Access Control is the model most enterprises still run on. It has three moving parts:
# MAGIC
# MAGIC - **Users** — the humans (and service principals) who do work
# MAGIC - **Roles** — named bundles of permissions (`pharmacy_pricing_reader`, `store_manager_bc`, `payroll_admin`)
# MAGIC - **Permissions** — the actual grants on objects (SELECT on `sales.transactions`, MODIFY on `hr.payroll_runs`)
# MAGIC
# MAGIC The contract is simple: a user is **assigned to a role**, the role **holds permissions**, end of story.
# MAGIC
# MAGIC <!-- SVG: triangle diagram — User → Role → Permission, with arrows -->

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why RBAC has been the default
# MAGIC
# MAGIC RBAC stuck around for good reasons:
# MAGIC
# MAGIC - **Auditable** — "who can do what" is a finite, listable set
# MAGIC - **Predictable** — adding a person to a role has obvious blast radius
# MAGIC - **Org-shaped** — roles map roughly to titles, which map roughly to org charts
# MAGIC
# MAGIC It works **when the organization is small enough that the role list stays human-readable.** That assumption is where it falls apart.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The pain — role explosion
# MAGIC
# MAGIC RBAC has a combinatorial problem. Every new dimension of access multiplies the role count.
# MAGIC
# MAGIC At PFG, access decisions depend on at least:
# MAGIC
# MAGIC | Dimension | Examples | Cardinality |
# MAGIC |---|---|---|
# MAGIC | Banner | Save-On-Foods, Urban Fare, PriceSmart, Quality Foods, Buy-Low | ~5–10 |
# MAGIC | Region | BC Lower Mainland, BC Interior, AB, SK, YT | ~6 |
# MAGIC | Store | Individual locations | ~150+ |
# MAGIC | Function | Pricing, payroll, inventory, loss prevention, pharmacy | ~10 |
# MAGIC | Sensitivity tier | Public, internal, confidential, regulated | 4 |
# MAGIC | Action | Read, write, approve, export | 4 |
# MAGIC
# MAGIC A pure-RBAC encoding needs roles for every meaningful combination. Even a conservative cross-product is **tens of thousands of roles** — most of which exist for one person, get rubber-stamped at access reviews, and never get cleaned up.
# MAGIC

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 980 450" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 22px; font-weight: 700; fill: #1B3139; }
    .bar-rbac { fill: #1B3139; }
    .bar-rbac-top { fill: #FF3621; }
    .bar-abac { fill: #2EB67D; }
    .bar-number { font-size: 14px; font-weight: 700; fill: #1B3139; }
    .bar-number-top { font-size: 16px; font-weight: 700; fill: #FF3621; }
    .bar-number-abac { font-size: 14px; font-weight: 700; fill: #2EB67D; }
    .bar-label { font-size: 12px; fill: #1B3139; font-weight: 600; }
    .bar-sub { font-size: 11px; fill: #425563; font-style: italic; }
    .axis { stroke: #1B3139; stroke-width: 1.5; }
    .col-title { font-size: 14px; font-weight: 700; letter-spacing: 1.5px; fill: #1B3139; }
    .col-title-abac { font-size: 14px; font-weight: 700; letter-spacing: 1.5px; fill: #2EB67D; }
    .divider { stroke: #425563; stroke-width: 1; stroke-dasharray: 4 4; }
  </style>

  <text x="490" y="32" class="title" text-anchor="middle">Role explosion — each new dimension multiplies</text>

  <line class="axis" x1="40" y1="385" x2="900" y2="385"/>

  <text x="305" y="62" class="col-title" text-anchor="middle">PURE RBAC</text>
  <text x="820" y="62" class="col-title-abac" text-anchor="middle">ABAC</text>

  <rect class="bar-rbac" x="55" y="355" width="80" height="30"/>
  <text x="95" y="348" class="bar-number" text-anchor="middle">5</text>
  <text x="95" y="407" class="bar-label" text-anchor="middle">5 banners</text>

  <rect class="bar-rbac" x="160" y="305" width="80" height="80"/>
  <text x="200" y="298" class="bar-number" text-anchor="middle">30</text>
  <text x="200" y="407" class="bar-label" text-anchor="middle">× 6 regions</text>

  <rect class="bar-rbac" x="265" y="225" width="80" height="160"/>
  <text x="305" y="218" class="bar-number" text-anchor="middle">4,500</text>
  <text x="305" y="407" class="bar-label" text-anchor="middle">× 150 stores</text>

  <rect class="bar-rbac" x="370" y="165" width="80" height="220"/>
  <text x="410" y="158" class="bar-number" text-anchor="middle">45,000</text>
  <text x="410" y="407" class="bar-label" text-anchor="middle">× 10 functions</text>

  <rect class="bar-rbac-top" x="475" y="95" width="80" height="290"/>
  <text x="515" y="88" class="bar-number-top" text-anchor="middle">180,000</text>
  <text x="515" y="407" class="bar-label" text-anchor="middle">× 4 sensitivities</text>
  <text x="515" y="423" class="bar-sub" text-anchor="middle">(conservative cross-product)</text>

  <line class="divider" x1="635" y1="80" x2="635" y2="395"/>

  <rect class="bar-abac" x="780" y="365" width="80" height="20"/>
  <text x="820" y="358" class="bar-number-abac" text-anchor="middle">~5</text>
  <text x="820" y="407" class="bar-label" text-anchor="middle">policies, total</text>
  <text x="820" y="423" class="bar-sub" text-anchor="middle">dimensions live in attributes</text>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The pain — joiners, movers, leavers
# MAGIC
# MAGIC Every role is a row in a table someone has to maintain. The cost shows up in three lifecycle events:
# MAGIC
# MAGIC - **Joiner** — new hire. Someone has to know which 8 roles the bundle actually needs. They usually copy from another employee, including the stale roles.
# MAGIC - **Mover** — promotion or transfer from Save-On-Foods Surrey to Urban Fare downtown. Old roles stick around because **nobody removes; everyone adds**. This is how a department head ends up with read access to three banners they no longer work at.
# MAGIC - **Leaver** — termination. The roles deprovision, but if the leaver was the **only** person in a custom role, the role becomes orphaned junk.
# MAGIC
# MAGIC The result is **permission creep**: actual access drifts further from intended access every quarter. Access reviews catch a fraction; the rest is invisible.
# MAGIC

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 980 340" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 22px; font-weight: 700; fill: #1B3139; }
    .axis { stroke: #1B3139; stroke-width: 1.5; }
    .staircase { fill: #1B3139; opacity: 0.88; }
    .cliff { stroke: #FF3621; stroke-width: 2; fill: none; stroke-dasharray: 4 3; }
    .event-line { stroke: #425563; stroke-width: 0.75; stroke-dasharray: 2 3; }
    .event-label { font-size: 13px; font-weight: 700; fill: #1B3139; }
    .event-delta { font-size: 11px; font-style: italic; fill: #425563; }
    .count-label { font-size: 13px; font-weight: 700; fill: #FFFFFF; }
    .orphan-label { font-size: 11px; font-weight: 700; fill: #FFFFFF; }
    .caption { font-size: 13px; fill: #425563; font-style: italic; }
  </style>

  <text x="490" y="32" class="title" text-anchor="middle">Joiner / Mover / Leaver — roles only ever accumulate</text>

  <line class="axis" x1="50" y1="290" x2="900" y2="290"/>

  <path class="staircase" d="M 80 290 L 80 270 L 280 270 L 280 240 L 440 240 L 440 200 L 600 200 L 600 170 L 760 170 L 760 290 Z"/>

  <line class="cliff" x1="760" y1="170" x2="760" y2="295"/>

  <line class="event-line" x1="80" y1="290" x2="80" y2="105"/>
  <line class="event-line" x1="280" y1="290" x2="280" y2="105"/>
  <line class="event-line" x1="440" y1="290" x2="440" y2="105"/>
  <line class="event-line" x1="600" y1="290" x2="600" y2="105"/>
  <line class="event-line" x1="760" y1="290" x2="760" y2="105"/>

  <text x="80" y="82" class="event-label" text-anchor="middle">Hire</text>
  <text x="80" y="98" class="event-delta" text-anchor="middle">+3 roles</text>

  <text x="280" y="82" class="event-label" text-anchor="middle">Promotion</text>
  <text x="280" y="98" class="event-delta" text-anchor="middle">+3 roles</text>

  <text x="440" y="82" class="event-label" text-anchor="middle">Banner Transfer</text>
  <text x="440" y="98" class="event-delta" text-anchor="middle">+4 roles · old stays</text>

  <text x="600" y="82" class="event-label" text-anchor="middle">Promotion</text>
  <text x="600" y="98" class="event-delta" text-anchor="middle">+3 roles</text>

  <text x="760" y="82" class="event-label" text-anchor="middle">Leaver</text>
  <text x="760" y="98" class="event-delta" text-anchor="middle">deprovisioned</text>

  <text x="180" y="286" class="count-label" text-anchor="middle">3</text>
  <text x="360" y="262" class="count-label" text-anchor="middle">6</text>
  <text x="520" y="225" class="count-label" text-anchor="middle">10</text>
  <text x="680" y="190" class="count-label" text-anchor="middle">13</text>

  <line stroke="#FF3621" stroke-width="1.5" stroke-dasharray="3 2" x1="780" y1="175" x2="770" y2="180"/>
  <rect x="780" y="155" width="160" height="40" rx="4" fill="#FF3621"/>
  <text x="860" y="172" class="orphan-label" text-anchor="middle">orphan role</text>
  <text x="860" y="187" class="orphan-label" text-anchor="middle" opacity="0.92">if leaver was sole owner</text>

  <text x="490" y="325" class="caption" text-anchor="middle">Roles accumulate across every life event. Access reviews catch a fraction. The rest is invisible.</text>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The pain — granularity ceiling
# MAGIC
# MAGIC RBAC grants are **set-shaped**, not **predicate-shaped**. You can grant SELECT on a table; you can't natively grant "SELECT on rows where `store_id` matches my home store."
# MAGIC
# MAGIC The usual workarounds make things worse:
# MAGIC
# MAGIC - **One view per role** — `sales_surrey_v`, `sales_burnaby_v`, `sales_richmond_v`. Now you have RBAC pain on roles **and** on views.
# MAGIC - **One schema per region** — duplicates pipelines and breaks cross-region analytics
# MAGIC - **Application-layer filtering** — moves the security perimeter into the BI tool, where auditors can't see it
# MAGIC
# MAGIC None of these scale. They just relocate the explosion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The ABAC mental shift
# MAGIC
# MAGIC ABAC stops asking *"who is this user?"* and starts asking **"what's true about this access attempt right now?"**
# MAGIC
# MAGIC Four things describe any access attempt:
# MAGIC
# MAGIC | Term | What it is | PFG example |
# MAGIC |---|---|---|
# MAGIC | **Subject attributes** | Facts about the user | `department=Pharmacy`, `home_banner=Save-On-Foods`, `clearance=Confidential` |
# MAGIC | **Resource attributes** | Facts about the object | `table.classification=PII`, `row.banner=Urban Fare`, `column.tag=SSN` |
# MAGIC | **Action attributes** | What's being attempted | `SELECT`, `MODIFY`, `EXPORT` |
# MAGIC | **Environment attributes** | Context of the attempt | `time_of_day`, `network=corp_vpn`, `cluster.access_mode=shared` |
# MAGIC
# MAGIC A policy is a sentence that combines these. The result is **decided at query time** — no role to mint, nothing to deprovision, nothing to drift.

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 420" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 22px; font-weight: 700; fill: #1B3139; }
    .label { font-size: 13px; fill: #1B3139; }
    .label-white { font-size: 13px; fill: #FFFFFF; font-weight: 600; }
    .caption { font-size: 12px; fill: #425563; font-style: italic; }
    .shift-lbl { font-size: 14px; font-weight: 700; fill: #FF3621; letter-spacing: 1.5px; }
    .box { fill: #F9F7F4; stroke: #1B3139; stroke-width: 1.5; }
    .box-dark { fill: #1B3139; stroke: #1B3139; stroke-width: 1.5; }
    .box-brick { fill: #FF3621; stroke: #FF3621; stroke-width: 1.5; }
    .arrow { stroke: #425563; stroke-width: 1.5; fill: none; }
    .arrow-big { stroke: #FF3621; stroke-width: 3; fill: none; }
  </style>
  <defs>
    <marker id="ah" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
      <polygon points="0 0, 8 4, 0 8" fill="#425563"/>
    </marker>
    <marker id="ah-big" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
      <polygon points="0 0, 10 5, 0 10" fill="#FF3621"/>
    </marker>
  </defs>

  <text x="150" y="40" class="title" text-anchor="middle">RBAC</text>

  <rect class="box" x="90" y="65" width="120" height="40" rx="6"/>
  <text x="150" y="90" class="label" text-anchor="middle">User</text>
  <line class="arrow" x1="150" y1="105" x2="150" y2="135" marker-end="url(#ah)"/>

  <rect class="box-dark" x="90" y="140" width="120" height="40" rx="6"/>
  <text x="150" y="165" class="label-white" text-anchor="middle">Role</text>
  <line class="arrow" x1="150" y1="180" x2="150" y2="210" marker-end="url(#ah)"/>

  <rect class="box" x="90" y="215" width="120" height="40" rx="6"/>
  <text x="150" y="240" class="label" text-anchor="middle">Permission</text>
  <line class="arrow" x1="150" y1="255" x2="150" y2="285" marker-end="url(#ah)"/>

  <rect class="box" x="90" y="290" width="120" height="40" rx="6"/>
  <text x="150" y="315" class="label" text-anchor="middle">Resource</text>

  <text x="150" y="365" class="caption" text-anchor="middle">Static — chain fixed</text>
  <text x="150" y="382" class="caption" text-anchor="middle">at assignment time.</text>

  <line class="arrow-big" x1="240" y1="200" x2="430" y2="200" marker-end="url(#ah-big)"/>
  <text x="335" y="190" class="shift-lbl" text-anchor="middle">SHIFT</text>

  <text x="665" y="40" class="title" text-anchor="middle">ABAC</text>

  <rect class="box" x="470" y="72" width="130" height="36" rx="6"/>
  <text x="535" y="95" class="label" text-anchor="middle">Subject attrs</text>

  <rect class="box" x="470" y="122" width="130" height="36" rx="6"/>
  <text x="535" y="145" class="label" text-anchor="middle">Resource attrs</text>

  <rect class="box" x="470" y="172" width="130" height="36" rx="6"/>
  <text x="535" y="195" class="label" text-anchor="middle">Action</text>

  <rect class="box" x="470" y="222" width="130" height="36" rx="6"/>
  <text x="535" y="245" class="label" text-anchor="middle">Environment</text>

  <path class="arrow" d="M 600 90 C 625 90, 630 175, 645 178" marker-end="url(#ah)"/>
  <path class="arrow" d="M 600 140 C 615 140, 630 175, 645 178" marker-end="url(#ah)"/>
  <path class="arrow" d="M 600 190 C 615 190, 630 182, 645 182" marker-end="url(#ah)"/>
  <path class="arrow" d="M 600 240 C 625 240, 630 185, 645 182" marker-end="url(#ah)"/>

  <rect class="box-brick" x="650" y="145" width="120" height="70" rx="6"/>
  <text x="710" y="175" class="label-white" text-anchor="middle">Policy</text>
  <text x="710" y="195" class="label-white" text-anchor="middle">Engine</text>

  <line class="arrow" x1="770" y1="180" x2="810" y2="180" marker-end="url(#ah)"/>

  <rect class="box-dark" x="815" y="155" width="70" height="50" rx="6"/>
  <text x="850" y="175" class="label-white" text-anchor="middle">Allow</text>
  <text x="850" y="195" class="label-white" text-anchor="middle">/ Mask</text>

  <text x="665" y="365" class="caption" text-anchor="middle">Dynamic — decision</text>
  <text x="665" y="382" class="caption" text-anchor="middle">computed at query time.</text>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Anatomy of an ABAC policy
# MAGIC
# MAGIC A policy is shaped like:
# MAGIC
# MAGIC > **Allow** *action* on *resource* **when** *condition over attributes*
# MAGIC
# MAGIC Examples in plain English, expressed against PFG data:
# MAGIC
# MAGIC - *Allow* SELECT on `sales.transactions` *when* `user.home_banner == row.banner`
# MAGIC - *Mask* column `customer.email` *when* `user.clearance < 'Confidential'`
# MAGIC - *Allow* MODIFY on `hr.payroll_runs` *when* `user.department == 'Payroll'` **and** `environment.network == 'corp_vpn'`
# MAGIC
# MAGIC Notice what's **not** in those policies: no role names, no store IDs hard-coded, no user emails. The policy is a **rule**, and the rule applies as facts change.
# MAGIC
# MAGIC One policy replaces the cross-product of roles it would have taken to express the same intent.

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 290" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 20px; font-weight: 700; fill: #1B3139; }
    .static-text { font-size: 17px; font-family: 'Courier New', Courier, monospace; fill: #1B3139; font-weight: 600; }
    .pill-text { font-size: 15px; font-family: 'Courier New', Courier, monospace; fill: #FFFFFF; font-weight: 700; }
    .legend { font-size: 12px; fill: #425563; font-style: italic; }
    .callout-line { stroke: #425563; stroke-width: 1; fill: none; stroke-dasharray: 2 2; }
    .callout-label { font-size: 13px; fill: #1B3139; font-weight: 700; }
    .callout-sub { font-size: 11px; fill: #425563; }
  </style>

  <text x="450" y="32" class="title" text-anchor="middle">Anatomy of an ABAC policy</text>

  <text x="40" y="98" class="legend">template</text>

  <rect x="120" y="80" width="80" height="30" rx="15" fill="#2EB67D"/>
  <text x="160" y="100" class="pill-text" text-anchor="middle">ALLOW</text>

  <rect x="210" y="80" width="100" height="30" rx="15" fill="#1B3139"/>
  <text x="260" y="100" class="pill-text" text-anchor="middle">[action]</text>

  <text x="320" y="100" class="static-text">on</text>

  <rect x="360" y="80" width="180" height="30" rx="15" fill="#FF3621"/>
  <text x="450" y="100" class="pill-text" text-anchor="middle">[resource]</text>

  <text x="550" y="100" class="static-text">when</text>

  <rect x="615" y="80" width="260" height="30" rx="15" fill="#00A1C9"/>
  <text x="745" y="100" class="pill-text" text-anchor="middle">[condition over attributes]</text>

  <text x="40" y="168" class="legend">example</text>

  <rect x="120" y="150" width="80" height="30" rx="15" fill="#2EB67D"/>
  <text x="160" y="170" class="pill-text" text-anchor="middle">ALLOW</text>

  <rect x="210" y="150" width="100" height="30" rx="15" fill="#1B3139"/>
  <text x="260" y="170" class="pill-text" text-anchor="middle">SELECT</text>

  <text x="320" y="170" class="static-text">on</text>

  <rect x="360" y="150" width="180" height="30" rx="15" fill="#FF3621"/>
  <text x="450" y="170" class="pill-text" text-anchor="middle">sales.transactions</text>

  <text x="550" y="170" class="static-text">when</text>

  <rect x="615" y="150" width="260" height="30" rx="15" fill="#00A1C9"/>
  <text x="745" y="170" class="pill-text" text-anchor="middle">user.banner = row.banner</text>

  <line class="callout-line" x1="160" y1="185" x2="160" y2="210"/>
  <text x="160" y="226" class="callout-label" text-anchor="middle">Effect</text>
  <text x="160" y="244" class="callout-sub" text-anchor="middle">Allow / Deny</text>
  <text x="160" y="258" class="callout-sub" text-anchor="middle">/ Mask</text>

  <line class="callout-line" x1="260" y1="185" x2="260" y2="210"/>
  <text x="260" y="226" class="callout-label" text-anchor="middle">Action</text>
  <text x="260" y="244" class="callout-sub" text-anchor="middle">What's being</text>
  <text x="260" y="258" class="callout-sub" text-anchor="middle">attempted</text>

  <line class="callout-line" x1="450" y1="185" x2="450" y2="210"/>
  <text x="450" y="226" class="callout-label" text-anchor="middle">Resource</text>
  <text x="450" y="244" class="callout-sub" text-anchor="middle">Object identity</text>
  <text x="450" y="258" class="callout-sub" text-anchor="middle">or tag match</text>

  <line class="callout-line" x1="745" y1="185" x2="745" y2="210"/>
  <text x="745" y="226" class="callout-label" text-anchor="middle">Condition</text>
  <text x="745" y="244" class="callout-sub" text-anchor="middle">Predicate over subject,</text>
  <text x="745" y="258" class="callout-sub" text-anchor="middle">resource, env attributes</text>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. RBAC vs ABAC at a glance
# MAGIC
# MAGIC | | RBAC | ABAC |
# MAGIC |---|---|---|
# MAGIC | **Unit of policy** | Role | Predicate over attributes |
# MAGIC | **Granted to** | Users (via role assignment) | Nobody — evaluated per request |
# MAGIC | **Adapts when facts change** | No — admin must re-grant | Yes — attribute change propagates |
# MAGIC | **Row-level filtering** | Workaround (views, schemas) | First-class |
# MAGIC | **Column masking** | Workaround | First-class |
# MAGIC | **Audit story** | "Who is in role X?" | "Why was this row returned?" |
# MAGIC | **Failure mode** | Permission creep | Bad attribute hygiene |
# MAGIC
# MAGIC ABAC doesn't eliminate operational burden — it **moves** it. The new burden is keeping **attributes** accurate (HR feed, group membership, table tags). That's an easier problem because attributes already have owners; roles often don't.
# MAGIC
# MAGIC <!-- SVG: side-by-side comparison panels — RBAC structure on the left, ABAC structure on the right -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Where ABAC lives in Databricks Unity Catalog
# MAGIC
# MAGIC Unity Catalog gives you the primitives to do ABAC without building it yourself:
# MAGIC
# MAGIC - **Subject attributes** → user identity + **group membership** (synced from IdP) + custom group properties
# MAGIC - **Resource attributes** → **tags** on catalogs, schemas, tables, and columns
# MAGIC - **Action attributes** → built into the grant model (SELECT, MODIFY, etc.)
# MAGIC - **Environment attributes** → cluster access mode, workspace, network
# MAGIC
# MAGIC The two enforcement points we'll spend the rest of the notebook on:
# MAGIC
# MAGIC - **Row filters** — SQL UDF that returns a boolean; runs per row at query time
# MAGIC - **Column masks** — SQL UDF that transforms a column value based on caller attributes
# MAGIC
# MAGIC Both are written **once per table**, evaluated **per query**, and **invisible to the user** writing the SQL. That's the ABAC payoff: the model centralizes the policy and decentralizes the decision.

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 980 500" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 24px; font-weight: 700; fill: #1B3139; }
    .attr-title-green { font-size: 16px; font-weight: 700; fill: #2EB67D; letter-spacing: 1.2px; }
    .attr-title-brick { font-size: 16px; font-weight: 700; fill: #FF3621; letter-spacing: 1.2px; }
    .attr-title-navy  { font-size: 16px; font-weight: 700; fill: #1B3139; letter-spacing: 1.2px; }
    .attr-title-cyan  { font-size: 16px; font-weight: 700; fill: #00A1C9; letter-spacing: 1.2px; }
    .attr-sub { font-size: 12px; fill: #425563; }
    .pipe-label { font-size: 16px; fill: #1B3139; font-weight: 600; }
    .pipe-label-white { font-size: 16px; fill: #FFFFFF; font-weight: 700; }
    .pipe-sub-white { font-size: 12px; fill: #FFFFFF; opacity: 0.85; }
    .pipe-caption { font-size: 11px; fill: #425563; font-style: italic; letter-spacing: 1.5px; }
    .box-soft { fill: #F9F7F4; stroke: #1B3139; stroke-width: 1.5; }
    .box-dark { fill: #1B3139; stroke: #1B3139; stroke-width: 1.5; }
    .arrow { stroke: #425563; stroke-width: 1.5; fill: none; }
    .arrow-thin { stroke: #425563; stroke-width: 1; fill: none; stroke-dasharray: 4 3; }
  </style>
  <defs>
    <marker id="ah" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
      <polygon points="0 0, 8 4, 0 8" fill="#425563"/>
    </marker>
  </defs>

  <text x="490" y="38" class="title" text-anchor="middle">Where ABAC lives in Databricks Unity Catalog</text>

  <rect class="box-soft" x="35" y="80" width="270" height="80" rx="6"/>
  <text x="170" y="112" class="attr-title-green" text-anchor="middle">SUBJECT</text>
  <text x="170" y="132" class="attr-sub" text-anchor="middle">User identity + group memberships</text>
  <text x="170" y="148" class="attr-sub" text-anchor="middle">(synced from IdP)</text>

  <rect class="box-soft" x="320" y="80" width="640" height="80" rx="6"/>
  <text x="640" y="112" class="attr-title-brick" text-anchor="middle">RESOURCE</text>
  <text x="640" y="138" class="attr-sub" text-anchor="middle">Tags on catalog / schema / table / column</text>

  <text x="490" y="217" class="pipe-caption" text-anchor="middle">POLICY EVALUATION — PER QUERY</text>

  <rect class="box-soft" x="50" y="235" width="240" height="80" rx="6"/>
  <text x="170" y="280" class="pipe-label" text-anchor="middle">UC Grants Check</text>

  <line class="arrow" x1="290" y1="275" x2="318" y2="275" marker-end="url(#ah)"/>

  <rect class="box-dark" x="320" y="235" width="280" height="80" rx="6"/>
  <text x="460" y="270" class="pipe-label-white" text-anchor="middle">Row Filter</text>
  <text x="460" y="294" class="pipe-sub-white" text-anchor="middle">SQL UDF</text>

  <line class="arrow" x1="600" y1="275" x2="628" y2="275" marker-end="url(#ah)"/>

  <rect class="box-dark" x="630" y="235" width="280" height="80" rx="6"/>
  <text x="770" y="270" class="pipe-label-white" text-anchor="middle">Column Mask</text>
  <text x="770" y="294" class="pipe-sub-white" text-anchor="middle">SQL UDF</text>

  <rect class="box-soft" x="35" y="395" width="270" height="80" rx="6"/>
  <text x="170" y="424" class="attr-title-navy" text-anchor="middle">ACTION</text>
  <text x="170" y="446" class="attr-sub" text-anchor="middle">SELECT, MODIFY, EXPORT</text>
  <text x="170" y="462" class="attr-sub" text-anchor="middle">(built into the grant model)</text>

  <rect class="box-soft" x="320" y="395" width="640" height="80" rx="6"/>
  <text x="640" y="424" class="attr-title-cyan" text-anchor="middle">ENVIRONMENT</text>
  <text x="640" y="450" class="attr-sub" text-anchor="middle">Cluster access mode, workspace, network</text>

  <line class="arrow-thin" x1="170" y1="161" x2="170" y2="233" marker-end="url(#ah)"/>
  <line class="arrow-thin" x1="170" y1="394" x2="170" y2="317" marker-end="url(#ah)"/>

  <line class="arrow-thin" x1="460" y1="161" x2="460" y2="233" marker-end="url(#ah)"/>
  <line class="arrow-thin" x1="770" y1="161" x2="770" y2="233" marker-end="url(#ah)"/>

  <line class="arrow-thin" x1="460" y1="394" x2="460" y2="317" marker-end="url(#ah)"/>
  <line class="arrow-thin" x1="770" y1="394" x2="770" y2="317" marker-end="url(#ah)"/>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 2 — Auto-Tagging Pipeline
# MAGIC
# MAGIC The ABAC theory from Section 1 only works if your **resource attributes** are populated. Tagging hygiene is what makes that real. Databricks gives you two tagging streams:
# MAGIC
# MAGIC 1. **Automatic** — an AI agent classifies columns for sensitive data and applies governed system tags
# MAGIC 2. **Manual** — domain teams apply custom governed tags (banner, data owner, sensitivity tier)
# MAGIC
# MAGIC We'll cover both, then run a live demo against PFG-shaped data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Databricks Data Classification does
# MAGIC
# MAGIC - **AI-driven column scanner.** An agent samples each column and detects PII patterns — SSN, email, phone, street address, credit card, government ID, more.
# MAGIC - **Incremental, no manual config.** New tables in a classification-enabled catalog get scanned within 24 hours. Existing tables get re-scanned on schedule.
# MAGIC - **Applies governed tags.** Results show up as tag key-value pairs like `class_pii: true` and `class_pii_type: email` directly on the columns it identified.
# MAGIC - **No data movement.** Sampling and inference happen inside your workspace — sensitive values don't leave the UC perimeter.
# MAGIC
# MAGIC The output is the raw material for **resource attributes** in your ABAC policies. Row filters and column masks read these tags to decide what to filter or mask.

# COMMAND ----------

# MAGIC %md
# MAGIC ## System tags + custom governed tags
# MAGIC
# MAGIC Two flavors of tags coexist on the same UC objects:
# MAGIC
# MAGIC | | Applied by | Examples | Use case |
# MAGIC |---|---|---|---|
# MAGIC | **System tags** | Data Classification agent | `class_pii: true`, `class_pii_type: ssn` | Auto-discovered sensitivity |
# MAGIC | **Custom governed tags** | Domain teams (manual) | `banner: save-on-foods`, `data_owner: supply_chain`, `sensitivity: internal` | Business context UC can't infer |
# MAGIC
# MAGIC Both are first-class. Both can be read by row filters and column masks. The key is **discipline** — once tags become policy inputs, drift in tagging becomes drift in access.

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 980 460" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 22px; font-weight: 700; fill: #1B3139; }
    .stream-title-auto { font-size: 16px; font-weight: 700; letter-spacing: 1px; fill: #00A1C9; }
    .stream-title-manual { font-size: 16px; font-weight: 700; letter-spacing: 1px; fill: #2EB67D; }
    .stream-sub { font-size: 12px; fill: #425563; font-style: italic; }
    .tag-mono { font-size: 11px; font-family: 'Courier New', Courier, monospace; fill: #1B3139; }
    .center-label-white { font-size: 16px; font-weight: 700; fill: #FFFFFF; }
    .center-sub-white { font-size: 12px; fill: #FFFFFF; opacity: 0.88; }
    .box-soft { fill: #F9F7F4; stroke: #1B3139; stroke-width: 1.5; }
    .box-brick { fill: #FF3621; stroke: #FF3621; stroke-width: 1.5; }
    .box-dark { fill: #1B3139; stroke: #1B3139; stroke-width: 1.5; }
    .arrow-auto { stroke: #00A1C9; stroke-width: 2; fill: none; }
    .arrow-manual { stroke: #2EB67D; stroke-width: 2; fill: none; }
    .arrow-gray { stroke: #425563; stroke-width: 2; fill: none; }
  </style>
  <defs>
    <marker id="ah-auto" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
      <polygon points="0 0, 10 5, 0 10" fill="#00A1C9"/>
    </marker>
    <marker id="ah-manual" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
      <polygon points="0 0, 10 5, 0 10" fill="#2EB67D"/>
    </marker>
    <marker id="ah-gray2" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
      <polygon points="0 0, 10 5, 0 10" fill="#425563"/>
    </marker>
  </defs>

  <text x="490" y="32" class="title" text-anchor="middle">Two tagging streams, one tagged-object surface</text>

  <rect class="box-soft" x="70" y="65" width="260" height="100" rx="6"/>
  <text x="200" y="90" class="stream-title-auto" text-anchor="middle">AUTOMATIC</text>
  <text x="200" y="108" class="stream-sub" text-anchor="middle">Data Classification agent</text>
  <text x="200" y="135" class="tag-mono" text-anchor="middle">class_pii: true</text>
  <text x="200" y="151" class="tag-mono" text-anchor="middle">class_pii_type: email|phone|ssn…</text>

  <rect class="box-soft" x="650" y="65" width="260" height="100" rx="6"/>
  <text x="780" y="90" class="stream-title-manual" text-anchor="middle">MANUAL</text>
  <text x="780" y="108" class="stream-sub" text-anchor="middle">Domain teams (CoE)</text>
  <text x="780" y="135" class="tag-mono" text-anchor="middle">banner, data_owner</text>
  <text x="780" y="151" class="tag-mono" text-anchor="middle">sensitivity, business_unit</text>

  <path class="arrow-auto" d="M 220 168 C 280 200, 380 220, 410 240" marker-end="url(#ah-auto)"/>
  <path class="arrow-manual" d="M 760 168 C 700 200, 600 220, 570 240" marker-end="url(#ah-manual)"/>

  <rect class="box-brick" x="290" y="245" width="400" height="80" rx="6"/>
  <text x="490" y="275" class="center-label-white" text-anchor="middle">Unity Catalog objects</text>
  <text x="490" y="298" class="center-sub-white" text-anchor="middle">catalog · schema · table · column</text>
  <text x="490" y="316" class="center-sub-white" text-anchor="middle">carry both system + custom tags</text>

  <line class="arrow-gray" x1="490" y1="325" x2="490" y2="365" marker-end="url(#ah-gray2)"/>

  <rect class="box-dark" x="290" y="370" width="400" height="60" rx="6"/>
  <text x="490" y="395" class="center-label-white" text-anchor="middle">Row filters + Column masks</text>
  <text x="490" y="416" class="center-sub-white" text-anchor="middle">read these tags at query time</text>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this looks like for PFG
# MAGIC
# MAGIC As your Exadata wage data, POS transactions, and customer loyalty data land in Unity Catalog, the classification agent catches the sensitive columns automatically — **no manual tagging effort**
# MAGIC
# MAGIC Day-zero: emails, phone numbers, payroll amounts, loyalty card IDs get tagged as PII without anyone filing a ticket.
# MAGIC
# MAGIC Day-one onward: The domain owners layer on the *domain* tags — which banner, which data owner, which sensitivity tier — that govern who can see what. Those are the tags the row filters and column masks we'll write in the next section will read.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live demo — tagging a PFG transactions table
# MAGIC
# MAGIC In this demo we will:
# MAGIC
# MAGIC 1. Spin up a PFG-shaped transactions table in a sandbox schema
# MAGIC 2. Show what auto-classification **would** apply (simulating it inline, since the agent runs on a 24h cadence)
# MAGIC 3. Layer on the custom governed tags
# MAGIC 4. Query `information_schema` to see the full tagging state

# COMMAND ----------

# DBTITLE 1,Demo config — edit catalog/schema here
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema",  "pfg_abac_demo", "Schema")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the sandbox catalog (idempotent)
# MAGIC -- CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(:catalog);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the demo schema inside the current catalog
# MAGIC -- CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema);

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA IDENTIFIER(:schema);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PFG-shaped transactions table.  Notice the mix:
# MAGIC --   * Banner / store / region   → row filter inputs
# MAGIC --   * email / phone / loyalty   → column mask candidates (PII)
# MAGIC --   * employee id               → PII for staff
# MAGIC CREATE OR REPLACE TABLE transactions (
# MAGIC   transaction_id    BIGINT,
# MAGIC   banner            STRING        COMMENT 'PFG banner name',
# MAGIC   store_id          STRING        COMMENT 'Store identifier',
# MAGIC   store_region      STRING        COMMENT 'Province / region',
# MAGIC   customer_email    STRING        COMMENT 'Customer contact email',
# MAGIC   customer_phone    STRING        COMMENT 'Customer contact phone',
# MAGIC   loyalty_card_id   STRING        COMMENT 'More Rewards loyalty card identifier',
# MAGIC   cashier_emp_id    STRING        COMMENT 'Employee ID of cashier',
# MAGIC   payment_method    STRING,
# MAGIC   total_amount      DECIMAL(10,2),
# MAGIC   transaction_ts    TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO transactions VALUES
# MAGIC   (1001, 'Save-On-Foods', 'BC-SURREY-014',    'BC', 'jane.kim@example.com',    '604-555-0142', 'SOF-9931-2204', 'EMP-08812', 'debit',     87.42,  TIMESTAMP '2026-05-10 17:22:11'),
# MAGIC   (1002, 'Save-On-Foods', 'BC-VANCOUVER-001', 'BC', 'mark.wei@example.com',    '604-555-0188', 'SOF-1102-8856', 'EMP-04217', 'visa',      142.15, TIMESTAMP '2026-05-10 18:01:44'),
# MAGIC   (1003, 'Urban Fare',    'BC-DOWNTOWN-003',  'BC', 'priya.menon@example.com', '604-555-0103', 'UF-7700-5512',  'EMP-01199', 'amex',      54.20,  TIMESTAMP '2026-05-10 11:14:02'),
# MAGIC   (1004, 'PriceSmart',    'BC-LANGLEY-007',   'BC', 'cathy.singh@example.com', '604-555-0177', 'PS-2233-0188',  'EMP-09012', 'cash',      211.95, TIMESTAMP '2026-05-10 15:48:31'),
# MAGIC   (1005, 'Quality Foods', 'BC-COMOX-002',     'BC', 'tom.barber@example.com',  '250-555-0144', 'QF-4471-3309',  'EMP-03114', 'debit',     76.10,  TIMESTAMP '2026-05-10 13:55:09'),
# MAGIC   (1006, 'Save-On-Foods', 'AB-CALGARY-022',   'AB', 'linda.zhao@example.com',  '403-555-0156', 'SOF-5511-8821', 'EMP-07731', 'visa',      38.75,  TIMESTAMP '2026-05-10 09:32:17'),
# MAGIC   (1007, 'Urban Fare',    'BC-YALETOWN-001',  'BC', 'derek.olu@example.com',   '604-555-0199', 'UF-3308-9941',  'EMP-02201', 'apple_pay', 92.30,  TIMESTAMP '2026-05-10 19:11:50'),
# MAGIC   (1008, 'Buy-Low Foods', 'BC-KAMLOOPS-005',  'BC', 'eve.ng@example.com',      '250-555-0125', 'BL-1188-6677',  'EMP-05509', 'debit',     168.40, TIMESTAMP '2026-05-10 14:27:33');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transactions LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — What auto-classification would apply
# MAGIC
# MAGIC In a real classification-enabled catalog, the agent would tag the sensitive columns within 24h. To keep the demo flowing, we'll apply the same tags the agent would set — `class.pii_type` on each PII column.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulating what Data Classification would auto-apply.
# MAGIC -- Note: UC reserves `.`, `=`, `/` in tag keys, so we use underscored keys.
# MAGIC ALTER TABLE transactions ALTER COLUMN customer_email  SET TAGS ('class_pii' = 'true', 'class_pii_type' = 'email');
# MAGIC ALTER TABLE transactions ALTER COLUMN customer_phone  SET TAGS ('class_pii' = 'true', 'class_pii_type' = 'phone');
# MAGIC ALTER TABLE transactions ALTER COLUMN loyalty_card_id SET TAGS ('class_pii' = 'true', 'class_pii_type' = 'loyalty_id');
# MAGIC ALTER TABLE transactions ALTER COLUMN cashier_emp_id  SET TAGS ('class_pii' = 'true', 'class_pii_type' = 'employee_id');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The agent's output, surfaced via information_schema
# MAGIC SELECT column_name, tag_name, tag_value
# MAGIC FROM   system.information_schema.column_tags
# MAGIC WHERE  catalog_name = :catalog
# MAGIC   AND  schema_name  = :schema
# MAGIC   AND  table_name   = 'transactions'
# MAGIC ORDER BY column_name, tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Layer on the custom governed tags
# MAGIC
# MAGIC Auto-classification handles *what kind of data this is*. The domain team layers on *who owns it* and *which slice of the business it belongs to*. These tags are what the row filter will read.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: this workspace has a tag policy on `sensitivity` enforcing the
# MAGIC -- vocabulary [pii, internal, public] — exactly the kind of guardrail
# MAGIC -- you want once tags drive access policy.
# MAGIC ALTER TABLE transactions SET TAGS (
# MAGIC   'data_owner'    = 'supply_chain',
# MAGIC   'sensitivity'   = 'internal',
# MAGIC   'business_unit' = 'Operations'
# MAGIC );
# MAGIC
# MAGIC -- Mark `banner` as the row-filter key so the policy author knows where to hook
# MAGIC ALTER TABLE transactions ALTER COLUMN banner SET TAGS ('row_filter_key' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combined view: every tag on the table, system + custom, table-level + column-level
# MAGIC SELECT 'TABLE'                       AS scope, tag_name, tag_value
# MAGIC FROM   system.information_schema.table_tags
# MAGIC WHERE  catalog_name = :catalog AND schema_name = :schema AND table_name = 'transactions'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT CONCAT('COL: ', column_name)  AS scope, tag_name, tag_value
# MAGIC FROM   system.information_schema.column_tags
# MAGIC WHERE  catalog_name = :catalog AND schema_name = :schema AND table_name = 'transactions'
# MAGIC
# MAGIC ORDER BY scope, tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC We just produced the **resource attributes** the ABAC engine needs:
# MAGIC
# MAGIC - **PII columns** are tagged automatically — `class_pii_type: email/phone/loyalty_id/employee_id`
# MAGIC - **Domain context** is tagged manually — `banner`, `data_owner`, `sensitivity`, `business_unit`
# MAGIC - **Policy hooks** are marked explicitly — `row_filter_key` on the column the next-section filter will read
# MAGIC
# MAGIC Next section we'll write the **row filter** and **column mask** UDFs that consume these tags. The policy logic stays simple because the tags do the heavy lifting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus — setting up your own tag policies
# MAGIC
# MAGIC ### What is a tag policy?
# MAGIC
# MAGIC A tag policy is a metastore-level governance artifact that locks down which **values** are allowed for a given tag **key**. Without one, anyone with tag-write permission can apply any value. With one, UC rejects out-of-vocabulary values at the `ALTER TABLE` boundary — *before* the bad tag ever reaches a row filter.
# MAGIC
# MAGIC ### Why this matters for ABAC
# MAGIC
# MAGIC Once a tag drives access (e.g. a row filter reads `sensitivity` to decide what to mask), **any drift in tag values is silent drift in access policy**. One analyst tagging `Confidential!` instead of `confidential` opens a gap nobody notices until the audit. Tag policies are the upstream guardrail that prevents that drift from ever landing.
# MAGIC
# MAGIC ### Where to manage them
# MAGIC
# MAGIC As of current Databricks releases, tag policies are managed primarily through:
# MAGIC
# MAGIC - **UI** — Catalog Explorer → *Governance* → *Tag Policies*. Easiest path; this is where the `sensitivity` and `business_unit` policies you hit live were created.
# MAGIC - **REST API** — `/api/2.1/unity-catalog/tag-policies` for programmatic setup and IaC (Terraform via the Databricks provider).
# MAGIC
# MAGIC A stable SQL DDL (`CREATE TAG POLICY`) for this is on the roadmap but isn't reliably available as of writing — check current docs before scripting. The practical path today is: pilot vocabulary in the UI, then once stable, lift the same definitions into Terraform.
# MAGIC
# MAGIC ### A pragmatic rollout for PFG
# MAGIC
# MAGIC Start narrow. Three or four governed keys is enough to anchor the program — every key you add to a policy is a key your ABAC policies can trust.
# MAGIC
# MAGIC | Tag key | Suggested vocabulary | Why this one first |
# MAGIC |---|---|---|
# MAGIC | `sensitivity` | `pii`, `internal`, `public` | Drives column masks and row filters directly |
# MAGIC | `data_owner` | curated list of team identifiers | Routes every table to an accountable owner |
# MAGIC | `business_unit` | `Finance`, `Operations`, `Pharmacy`, `Loyalty`, `Supply Chain` | Maps tables to their analytical home; aids cost/usage attribution |
# MAGIC | `banner` | `Save-On-Foods`, `Urban Fare`, `PriceSmart`, `Quality Foods`, `Buy-Low` | The dominant row-filter dimension for retail data |
# MAGIC
# MAGIC Iterate from there. Roll out tags first (ungoverned), watch what values actually land in the wild, *then* lock the vocabulary based on real usage rather than top-down guesses.
# MAGIC
# MAGIC ### Who owns the policy?
# MAGIC
# MAGIC Tag-policy creation requires metastore-admin privileges. In practice that's a small group — typically the central data governance team. The *values* inside each policy should be agreed cross-functionally (a banner list is meaningless without retail input, an owner list is meaningless without finance/HR input). Treat the policy DDL like any other infra change: code-reviewed, version-controlled, applied via CI.

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 3 — Building & Applying Policies
# MAGIC
# MAGIC Tags give us the **resource attributes**; the section above set those up. Now we'll write the actual ABAC enforcement. Two policy types do the work — both are SQL UDFs you attach to UC objects.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Two policy types
# MAGIC
# MAGIC | | What it does | Return type | Example |
# MAGIC |---|---|---|---|
# MAGIC | **Row Filter** | Decides whether a row is visible to the caller | `BOOLEAN` | "Return TRUE only when `banner = caller's home banner`" |
# MAGIC | **Column Mask** | Transforms a column's value before returning | Same type as the column | "Return last 4 of SSN unless caller is in `hr_admins`" |
# MAGIC
# MAGIC Both can be attached at **table** level (specific column/table) *or* **catalog** level (any column matching a tag predicate). The catalog-level pattern is where ABAC pays off — tag a column once, the policy applies everywhere automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy anatomy
# MAGIC
# MAGIC Every policy has four moving parts:
# MAGIC
# MAGIC 1. **Scope** — which UC object the policy attaches to (a single table, a catalog, or all columns with a given tag in a catalog)
# MAGIC 2. **Match condition** — usually a tag predicate, e.g. *"this column has tag `pii = ssn`"*
# MAGIC 3. **Caller condition** — who's exempt vs subject to the policy (group membership, attribute checks via `is_account_group_member()`, `current_user()`)
# MAGIC 4. **Transformation** — the SQL UDF that returns the boolean (row filter) or transformed value (column mask)
# MAGIC
# MAGIC The UDF itself is *just SQL* — `CASE WHEN ... THEN ... ELSE ... END`. If you can write a `CASE`, you can write an ABAC policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live demo — column mask via tag, attached at catalog scope
# MAGIC
# MAGIC Following the official ABAC tutorial pattern:
# MAGIC
# MAGIC 1. Define a governed tag `pii` with allowed values `ssn`, `address`
# MAGIC 2. Create a small HR-shaped `employees` table with sensitive columns
# MAGIC 3. Tag those columns with `pii = ssn` / `pii = address`
# MAGIC 4. Write mask UDFs that hide the value unless the caller is in `hr_admins`
# MAGIC 5. Attach the masks at **catalog** scope — anything tagged `pii = ssn` in the catalog gets masked, table by table or column by column, automatically
# MAGIC 6. Query and observe the mask firing
# MAGIC
# MAGIC **The key moment** is step 5 → step 6: we'll create a *second* table in the same catalog, tag a column, and watch the mask apply with zero per-table wiring.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Who am I, and what groups am I in?
# MAGIC -- The mask UDFs we're about to write will use this to decide whether to redact.
# MAGIC SELECT
# MAGIC   current_user()                            AS me,
# MAGIC   is_account_group_member('hr_admins')      AS am_i_hr_admin;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — Govern the `pii` tag vocabulary
# MAGIC
# MAGIC Same pattern we covered in the Bonus section of Section 2 — lock the allowed values before anyone applies the tag in anger.

# COMMAND ----------

# MAGIC %md
# MAGIC **Note on tag-policy creation:**
# MAGIC
# MAGIC In current Databricks releases, tag policies are managed via the **Catalog Explorer UI** (*Governance → Tag Policies*) or the **REST API** — there isn't yet a stable SQL DDL surface for `CREATE TAG POLICY`. We're skipping the create step here for that reason; the rest of the demo works regardless (an ungoverned tag key just accepts any value). We will be setting up a catalog-level policy later.
# MAGIC
# MAGIC If you want to set up the `pii` vocabulary in this workspace before the audience arrives, do it once in the UI — same pattern you saw on the pre-existing `sensitivity` / `business_unit` policies in Section 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — A small HR-shaped table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS employees;
# MAGIC CREATE OR REPLACE TABLE employees (
# MAGIC   employee_id   STRING,
# MAGIC   full_name     STRING,
# MAGIC   banner        STRING,
# MAGIC   store_id      STRING,
# MAGIC   ssn           STRING   COMMENT 'Social insurance/security number — sensitive',
# MAGIC   home_address  STRING   COMMENT 'Residential address — sensitive',
# MAGIC   hire_date     DATE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC   ('EMP-08812', 'Jane Kim',     'Save-On-Foods', 'BC-SURREY-014',    '111-22-3344', '1212 Maple St, Surrey BC',       DATE '2019-03-15'),
# MAGIC   ('EMP-04217', 'Mark Wei',     'Save-On-Foods', 'BC-VANCOUVER-001', '222-33-4455', '88 Oak Ave, Vancouver BC',       DATE '2020-07-22'),
# MAGIC   ('EMP-01199', 'Priya Menon',  'Urban Fare',    'BC-DOWNTOWN-003',  '333-44-5566', '405 Granville St, Vancouver BC', DATE '2021-11-08'),
# MAGIC   ('EMP-09012', 'Cathy Singh',  'PriceSmart',    'BC-LANGLEY-007',   '444-55-6677', '900 Glover Rd, Langley BC',      DATE '2018-05-30'),
# MAGIC   ('EMP-03114', 'Tom Barber',   'Quality Foods', 'BC-COMOX-002',     '555-66-7788', '77 Comox Rd, Comox BC',          DATE '2022-01-12');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity check — raw data, no masks yet
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Tag the sensitive columns

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE employees ALTER COLUMN ssn          SET TAGS ('pii' = 'ssn');
# MAGIC ALTER TABLE employees ALTER COLUMN home_address SET TAGS ('pii' = 'address');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — Write the mask UDFs
# MAGIC
# MAGIC Pure SQL. The CASE is doing all the work — `hr_admins` see the raw value, everyone else sees the redacted form.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SSN: hr_admins see raw; everyone else sees `XXX-XX-<last 4>`
# MAGIC CREATE OR REPLACE FUNCTION mask_ssn(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('hr_admins') THEN val
# MAGIC   WHEN val IS NULL                          THEN NULL
# MAGIC   ELSE CONCAT('XXX-XX-', RIGHT(val, 4))
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Address: hr_admins see raw; everyone else sees `[REDACTED — <province>]`
# MAGIC CREATE OR REPLACE FUNCTION mask_address(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('hr_admins') THEN val
# MAGIC   WHEN val IS NULL                          THEN NULL
# MAGIC   ELSE CONCAT('[REDACTED — ', RIGHT(val, 2), ']')
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — Attach the masks
# MAGIC
# MAGIC Two ways to attach a column mask in Unity Catalog today:
# MAGIC
# MAGIC | Scope | How it's attached | What it gives you |
# MAGIC |---|---|---|
# MAGIC | **Per table / column** (what we'll demo) | `ALTER TABLE ... ALTER COLUMN ... SET MASK function_name` — stable SQL DDL | The mask runs whenever this specific column is queried |
# MAGIC | **Catalog-scope, tag-driven** (the ABAC end state) | Currently **UI / REST API** — Catalog Explorer → *Governance* → *Policies* | Any column anywhere in the catalog that carries the matching tag picks up the mask automatically — no per-table wiring |
# MAGIC
# MAGIC We're going to use the per-table form for the live demo because the SQL is rock solid. The audience can imagine the catalog-scope form trivially — *"same UDF, attached once, applies everywhere a column is tagged `pii = ssn`."*

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 480" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 22px; font-weight: 700; fill: #1B3139; }
    .panel-title { font-size: 16px; font-weight: 700; fill: #1B3139; letter-spacing: 1.5px; }
    .panel-sub { font-size: 12px; fill: #425563; font-style: italic; }
    .box-label { font-size: 13px; fill: #1B3139; font-weight: 600; }
    .box-label-white { font-size: 13px; fill: #FFFFFF; font-weight: 700; }
    .small-mono { font-size: 11px; fill: #FFFFFF; font-family: 'Courier New', Courier, monospace; }
    .arrow-label { font-size: 10px; fill: #FF3621; font-weight: 700; font-family: 'Courier New', Courier, monospace; }
    .caption { font-size: 13px; fill: #1B3139; font-weight: 700; }
    .caption-sub { font-size: 11px; fill: #425563; font-style: italic; }
    .box-soft { fill: #F9F7F4; stroke: #1B3139; stroke-width: 1.5; }
    .box-dark { fill: #1B3139; stroke: #1B3139; stroke-width: 1.5; }
    .box-brick { fill: #FF3621; stroke: #FF3621; stroke-width: 1.5; }
    .box-future { fill: #F9F7F4; stroke: #425563; stroke-width: 1.5; stroke-dasharray: 5 3; }
    .arrow-gray { stroke: #425563; stroke-width: 1.5; fill: none; }
    .arrow-brick { stroke: #FF3621; stroke-width: 1.5; fill: none; }
    .divider { stroke: #425563; stroke-width: 0.5; stroke-dasharray: 4 4; }
  </style>
  <defs>
    <marker id="ah-gray" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
      <polygon points="0 0, 8 4, 0 8" fill="#425563"/>
    </marker>
    <marker id="ah-brick" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
      <polygon points="0 0, 8 4, 0 8" fill="#FF3621"/>
    </marker>
  </defs>

  <text x="500" y="32" class="title" text-anchor="middle">Per-table attachment vs catalog-scope policy</text>

  <line class="divider" x1="500" y1="55" x2="500" y2="450"/>

  <text x="245" y="75" class="panel-title" text-anchor="middle">PER-TABLE (TODAY)</text>
  <text x="245" y="95" class="panel-sub" text-anchor="middle">stable SQL DDL — works, doesn't scale</text>

  <rect class="box-dark" x="175" y="115" width="140" height="40" rx="6"/>
  <text x="245" y="140" class="box-label-white" text-anchor="middle">UDF: mask_ssn</text>

  <rect class="box-soft" x="80" y="220" width="160" height="36" rx="5"/>
  <text x="160" y="243" class="box-label" text-anchor="middle">transactions</text>

  <rect class="box-soft" x="80" y="270" width="160" height="36" rx="5"/>
  <text x="160" y="293" class="box-label" text-anchor="middle">employees</text>

  <rect class="box-soft" x="80" y="320" width="160" height="36" rx="5"/>
  <text x="160" y="343" class="box-label" text-anchor="middle">payroll_runs</text>

  <rect class="box-soft" x="80" y="370" width="160" height="36" rx="5"/>
  <text x="160" y="393" class="box-label" text-anchor="middle">new_customer_data</text>

  <path class="arrow-gray" d="M 230 155 C 215 185, 205 210, 195 218" marker-end="url(#ah-gray)"/>
  <path class="arrow-gray" d="M 245 155 C 235 215, 220 255, 205 268" marker-end="url(#ah-gray)"/>
  <path class="arrow-gray" d="M 260 155 C 255 245, 240 305, 220 318" marker-end="url(#ah-gray)"/>
  <path class="arrow-gray" d="M 275 155 C 290 280, 265 355, 235 368" marker-end="url(#ah-gray)"/>

  <text x="295" y="190" class="arrow-label">SET MASK</text>
  <text x="305" y="238" class="arrow-label">SET MASK</text>
  <text x="320" y="288" class="arrow-label">SET MASK</text>
  <text x="335" y="340" class="arrow-label">SET MASK</text>

  <text x="245" y="440" class="caption" text-anchor="middle">N tables = N attachments</text>
  <text x="245" y="458" class="caption-sub" text-anchor="middle">every new table is a SQL change</text>

  <text x="755" y="75" class="panel-title" text-anchor="middle">CATALOG-SCOPE (ABAC)</text>
  <text x="755" y="95" class="panel-sub" text-anchor="middle">attached once via Catalog Explorer / REST</text>

  <rect class="box-brick" x="600" y="115" width="310" height="50" rx="6"/>
  <text x="755" y="135" class="box-label-white" text-anchor="middle">POLICY: mask_ssn_policy</text>
  <text x="755" y="153" class="small-mono" text-anchor="middle">FOR TABLES MATCH COLUMNS has_tag_value('pii','ssn')</text>

  <rect class="box-soft" x="540" y="220" width="160" height="36" rx="5"/>
  <text x="620" y="243" class="box-label" text-anchor="middle">transactions</text>

  <rect class="box-soft" x="540" y="270" width="160" height="36" rx="5"/>
  <text x="620" y="293" class="box-label" text-anchor="middle">employees</text>

  <rect class="box-soft" x="540" y="320" width="160" height="36" rx="5"/>
  <text x="620" y="343" class="box-label" text-anchor="middle">payroll_runs</text>

  <rect class="box-soft" x="540" y="370" width="160" height="36" rx="5"/>
  <text x="620" y="393" class="box-label" text-anchor="middle">new_customer_data</text>

  <rect class="box-future" x="730" y="320" width="180" height="36" rx="5"/>
  <text x="820" y="343" class="box-label" text-anchor="middle">…every future table</text>

  <path class="arrow-brick" d="M 690 165 C 670 185, 650 210, 640 218" marker-end="url(#ah-brick)"/>
  <path class="arrow-brick" d="M 720 165 C 710 215, 690 255, 670 268" marker-end="url(#ah-brick)"/>
  <path class="arrow-brick" d="M 755 165 C 750 245, 730 305, 700 318" marker-end="url(#ah-brick)"/>
  <path class="arrow-brick" d="M 780 165 C 785 280, 770 355, 740 368" marker-end="url(#ah-brick)"/>
  <path class="arrow-brick" d="M 810 165 C 820 250, 820 300, 820 318" marker-end="url(#ah-brick)"/>

  <text x="755" y="440" class="caption" text-anchor="middle">1 policy = every tagged column</text>
  <text x="755" y="458" class="caption-sub" text-anchor="middle">current and future tables, no per-table SQL</text>
</svg>
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attach per column on this table.  In production with catalog-scope
# MAGIC -- attachment, these two ALTERs would not be needed — the tag alone would
# MAGIC -- pull the mask in.
# MAGIC ALTER TABLE employees ALTER COLUMN ssn          SET MASK mask_ssn;
# MAGIC ALTER TABLE employees ALTER COLUMN home_address SET MASK mask_address;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 — Watch the mask fire

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If you're NOT in `hr_admins`, the SSN and address columns come back redacted.
# MAGIC -- If you ARE in `hr_admins`, you see raw values.
# MAGIC SELECT employee_id, full_name, banner, ssn, home_address
# MAGIC FROM   employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 — Reusing the UDF on a brand-new table
# MAGIC
# MAGIC The mask UDF is fully reusable — same `mask_ssn` function, attached to a *different* table's column. Two steps today (table-level mode); one step in catalog-scope mode (just the tag).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Brand-new payroll table — never touched our masks before
# MAGIC DROP TABLE IF EXISTS payroll_runs;
# MAGIC CREATE OR REPLACE TABLE payroll_runs (
# MAGIC   run_id       BIGINT,
# MAGIC   employee_id  STRING,
# MAGIC   employee_ssn STRING,
# MAGIC   gross_amount DECIMAL(10,2),
# MAGIC   run_date     DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO payroll_runs VALUES
# MAGIC   (1, 'EMP-08812', '111-22-3344', 1842.50, DATE '2026-05-09'),
# MAGIC   (2, 'EMP-04217', '222-33-4455', 2150.00, DATE '2026-05-09'),
# MAGIC   (3, 'EMP-01199', '333-44-5566', 1740.75, DATE '2026-05-09');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag the sensitive column.  In catalog-scope mode (UI/API), this single
# MAGIC -- line would be enough — the tag would pull `mask_ssn` in automatically.
# MAGIC ALTER TABLE payroll_runs ALTER COLUMN employee_ssn SET TAGS ('pii' = 'ssn');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Today's table-level DDL: we also have to attach.  The mask UDF, though,
# MAGIC -- is exactly the same one we wrote earlier — write once, attach anywhere.
# MAGIC ALTER TABLE payroll_runs ALTER COLUMN employee_ssn SET MASK mask_ssn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now mask_ssn fires on payroll_runs — same redaction logic, different table.
# MAGIC SELECT * FROM payroll_runs;

# COMMAND ----------

# MAGIC %md
# MAGIC ### The catalog-scope future state
# MAGIC
# MAGIC In a workspace where the column-mask policy is attached at **catalog** scope (via the Catalog Explorer UI or REST API), Step 7 collapses to just the *tag* — the `ALTER TABLE ... SET MASK` disappears entirely, because the catalog-level policy says *"any column with tag `pii = ssn`, anywhere in this catalog, gets `mask_ssn`."*
# MAGIC
# MAGIC That's the operational payoff: **The UDF is written once, attaches the policy once, and every future PFG table inherits the mask the moment a column is tagged.** No per-table SQL, no governance backlog when new tables land.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row filters work the same way
# MAGIC
# MAGIC We won't demo this live, but the shape is identical — only the UDF return type changes (`BOOLEAN` instead of the column's type):
# MAGIC
# MAGIC ```sql
# MAGIC -- Sketch: each user only sees rows for their home banner
# MAGIC CREATE OR REPLACE FUNCTION banner_row_filter(row_banner STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   is_account_group_member('all_banner_admins')
# MAGIC   OR row_banner = current_user_attribute('home_banner');
# MAGIC
# MAGIC ALTER TABLE transactions
# MAGIC   SET ROW FILTER banner_row_filter ON (banner);
# MAGIC ```
# MAGIC
# MAGIC Same anatomy: scope, match, caller condition, UDF. The audience can write a row filter the moment they've seen a column mask.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC In a short period of time, we went from raw tags to enforced ABAC:
# MAGIC
# MAGIC - **Two policy types** — row filter and column mask — both are just SQL UDFs
# MAGIC - **Anatomy** — scope + match condition + caller condition + transformation
# MAGIC - **Catalog-scoped attachment** — the killer feature; one policy declaration covers every current and future table in the catalog that carries the matching tag
# MAGIC - **Auto-apply demo** — `payroll_runs` got masked without us ever telling UC about it specifically; the tag did the work
# MAGIC
# MAGIC The combination of Section 2 (tagging hygiene) + Section 3 (policies that consume tags) is what makes ABAC operationally feasible at PFG scale. Each new table inherits governance from the tags it carries — no manual per-table policy authoring, no role explosion.

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 4 — The Magic: Auto-Enforcement
# MAGIC
# MAGIC **~25 min · mostly live demo**
# MAGIC
# MAGIC Everything we've built so far — the tag vocabulary, the mask UDFs, the catalog-scope attachment — has been *setup*. This section is the payoff.
# MAGIC
# MAGIC We're going to pretend a brand-new table just landed from PFG's Exadata migration. There is no policy for it. Nobody has wired up a mask. Just a new table, two tags, and a query — and we'll watch what happens.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What "auto-enforcement" actually means
# MAGIC
# MAGIC In Section 3 the masks were attached **table by table** via `ALTER TABLE ... SET MASK`. That works, but it doesn't scale — every new table is a per-table SQL change, and every per-table SQL change is a governance ticket.
# MAGIC
# MAGIC The catalog-scope policy (attached once via *Catalog Explorer → Governance → Policies*) flips the model:
# MAGIC
# MAGIC > *"Any column in this catalog tagged `pii = ssn` is masked by `mask_ssn`. Any column tagged `pii = address` is masked by `mask_address`."*
# MAGIC
# MAGIC One declaration. Every current table. Every future table. **The tag is the policy hook.**

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 980 340" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 22px; font-weight: 700; fill: #1B3139; }
    .step-label { font-size: 13px; font-weight: 700; fill: #1B3139; }
    .step-label-white { font-size: 13px; font-weight: 700; fill: #FFFFFF; }
    .step-sub { font-size: 11px; fill: #425563; font-style: italic; }
    .step-sub-white { font-size: 11px; fill: #FFFFFF; font-style: italic; opacity: 0.92; }
    .mono { font-size: 10px; fill: #1B3139; font-family: 'Courier New', Courier, monospace; }
    .mono-white { font-size: 10px; fill: #FFFFFF; font-family: 'Courier New', Courier, monospace; }
    .box-soft { fill: #F9F7F4; stroke: #1B3139; stroke-width: 1.5; }
    .box-dark { fill: #1B3139; stroke: #1B3139; stroke-width: 1.5; }
    .box-brick { fill: #FF3621; stroke: #FF3621; stroke-width: 2; }
    .arrow { stroke: #425563; stroke-width: 2; fill: none; }
    .caption-bottom { font-size: 14px; font-weight: 700; fill: #2EB67D; letter-spacing: 1.5px; }
    .caption-sub { font-size: 12px; fill: #425563; font-style: italic; }
  </style>
  <defs>
    <marker id="ah-step" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
      <polygon points="0 0, 10 5, 0 10" fill="#425563"/>
    </marker>
  </defs>

  <text x="490" y="32" class="title" text-anchor="middle">Auto-enforcement: the tag is the policy hook</text>

  <rect class="box-soft" x="40" y="100" width="140" height="90" rx="6"/>
  <text x="110" y="125" class="step-label" text-anchor="middle">1. Table lands</text>
  <text x="110" y="143" class="step-sub" text-anchor="middle">from migration</text>
  <text x="110" y="170" class="mono" text-anchor="middle">CREATE TABLE …</text>

  <line class="arrow" x1="185" y1="145" x2="225" y2="145" marker-end="url(#ah-step)"/>

  <rect class="box-soft" x="235" y="100" width="140" height="90" rx="6"/>
  <text x="305" y="125" class="step-label" text-anchor="middle">2. Tag applied</text>
  <text x="305" y="143" class="step-sub" text-anchor="middle">by domain team</text>
  <text x="305" y="170" class="mono" text-anchor="middle">SET TAGS('pii','ssn')</text>

  <line class="arrow" x1="380" y1="145" x2="420" y2="145" marker-end="url(#ah-step)"/>

  <rect class="box-brick" x="430" y="92" width="140" height="106" rx="6"/>
  <text x="500" y="118" class="step-label-white" text-anchor="middle">3. Policy detects</text>
  <text x="500" y="138" class="step-sub-white" text-anchor="middle">automatically</text>
  <text x="500" y="165" class="mono-white" text-anchor="middle">has_tag_value</text>
  <text x="500" y="180" class="mono-white" text-anchor="middle">matches</text>

  <line class="arrow" x1="575" y1="145" x2="615" y2="145" marker-end="url(#ah-step)"/>

  <rect class="box-soft" x="625" y="100" width="140" height="90" rx="6"/>
  <text x="695" y="125" class="step-label" text-anchor="middle">4. Mask attaches</text>
  <text x="695" y="143" class="step-sub" text-anchor="middle">no SET MASK DDL</text>
  <text x="695" y="170" class="mono" text-anchor="middle">mask_ssn(col)</text>

  <line class="arrow" x1="770" y1="145" x2="810" y2="145" marker-end="url(#ah-step)"/>

  <rect class="box-dark" x="820" y="100" width="140" height="90" rx="6"/>
  <text x="890" y="125" class="step-label-white" text-anchor="middle">5. Query masked</text>
  <text x="890" y="143" class="step-sub-white" text-anchor="middle">next SELECT</text>
  <text x="890" y="170" class="mono-white" text-anchor="middle">XXX-XX-1234</text>

  <text x="490" y="265" class="caption-bottom" text-anchor="middle">ZERO ADDITIONAL GOVERNANCE WORK</text>
  <text x="490" y="290" class="caption-sub" text-anchor="middle">Admin team writes the UDF once; every future tagged column inherits the mask</text>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### One-time setup — catalog-scope policies
# MAGIC
# MAGIC For the next cells to show pure auto-enforcement (tag → mask, no per-table `SET MASK` DDL), two catalog-scope policies need to be attached **once** on `:catalog`. The DDL below uses the `has_tag_value()` predicate so the policy matches only columns where `pii` has a specific value — `mask_ssn` fires for `pii = ssn`, `mask_address` fires for `pii = address`.
# MAGIC
# MAGIC Run this cell once per workspace. After it's in place, every future table in the catalog inherits both masks the moment the right tag lands on a column.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One-time setup. Two notes before re-running this anywhere new:
# MAGIC --   1. The catalog name (`ademianczuk_uc_1_catalog`) is HARDCODED in
# MAGIC --      `ON CATALOG` and in the fully-qualified function names. UC does
# MAGIC --      not support `IDENTIFIER(:catalog)` for policy DDL, so the
# MAGIC --      `:catalog` / `:schema` widgets above do NOT propagate here.
# MAGIC --      Moving to another workspace? Edit the 4 occurrences below.
# MAGIC --   2. Function names MUST be fully qualified — the policy resolver
# MAGIC --      looks them up under `<catalog>.default` otherwise and fails.
# MAGIC CREATE OR REPLACE POLICY mask_ssn_policy
# MAGIC ON CATALOG ademianczuk_uc_1_catalog
# MAGIC COMMENT 'Apply mask_ssn to columns tagged pii = ssn'
# MAGIC COLUMN MASK ademianczuk_uc_1_catalog.pfg_abac_demo.mask_ssn
# MAGIC TO `account users`
# MAGIC FOR TABLES MATCH COLUMNS has_tag_value('pii', 'ssn') AS ssn_col ON COLUMN ssn_col;
# MAGIC
# MAGIC CREATE OR REPLACE POLICY mask_address_policy
# MAGIC ON CATALOG ademianczuk_uc_1_catalog
# MAGIC COMMENT 'Apply mask_address to columns tagged pii = address'
# MAGIC COLUMN MASK ademianczuk_uc_1_catalog.pfg_abac_demo.mask_address
# MAGIC TO `account users`
# MAGIC FOR TABLES MATCH COLUMNS has_tag_value('pii', 'address') AS addr_col ON COLUMN addr_col;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm both policies are attached
# MAGIC SHOW POLICIES ON CATALOG ademianczuk_uc_1_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — a brand new "Exadata-migrated" table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pretend this table just landed from PFG's Exadata migration.
# MAGIC --
# MAGIC -- Note the explicit DROP: in UC, `CREATE OR REPLACE TABLE` preserves the
# MAGIC -- table's object identity, which means column tags survive the replace.
# MAGIC -- That would silently mask the "before tagging" SELECT below on a second
# MAGIC -- run.  DROP forces a true clean slate.
# MAGIC DROP TABLE IF EXISTS new_customer_data;
# MAGIC CREATE TABLE new_customer_data (
# MAGIC   customer_id     STRING,
# MAGIC   full_name       STRING,
# MAGIC   banner          STRING,
# MAGIC   ssn             STRING   COMMENT 'Customer SSN — landed from Exadata',
# MAGIC   home_address    STRING   COMMENT 'Customer home address — landed from Exadata',
# MAGIC   loyalty_tier    STRING,
# MAGIC   joined_date     DATE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO new_customer_data VALUES
# MAGIC   ('CUST-22001', 'Aisha Patel',     'Save-On-Foods', '777-88-9911', '34 Birch St, Burnaby BC',        'gold',     DATE '2023-04-12'),
# MAGIC   ('CUST-22002', 'Daniel Cho',      'Urban Fare',    '666-77-8800', '120 Davie St, Vancouver BC',     'platinum', DATE '2022-09-30'),
# MAGIC   ('CUST-22003', 'Hannah Williams', 'PriceSmart',    '555-44-3322', '88 Fraser Hwy, Langley BC',      'silver',   DATE '2024-01-05'),
# MAGIC   ('CUST-22004', 'Raj Singh',       'Quality Foods', '444-33-2211', '12 Comox Rd, Comox BC',          'gold',     DATE '2021-11-18'),
# MAGIC   ('CUST-22005', 'Megan O''Connor', 'Buy-Low Foods', '333-22-1100', '901 Kamloops Lane, Kamloops BC', 'silver',   DATE '2025-02-22');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Right now the table is wide open.  No tags, no masks.  Anyone with SELECT
# MAGIC -- on this table sees raw SSN and address — exactly the kind of leakage
# MAGIC SELECT * FROM new_customer_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### The one-step fix: tag the columns

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE new_customer_data ALTER COLUMN ssn          SET TAGS ('pii' = 'ssn');
# MAGIC ALTER TABLE new_customer_data ALTER COLUMN home_address SET TAGS ('pii' = 'address');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same SELECT.  No CREATE FUNCTION.  No SET MASK.  No new policy authoring.
# MAGIC -- The catalog-scope policy attached in Catalog Explorer saw the new tags
# MAGIC -- and applied mask_ssn / mask_address automatically.
# MAGIC SELECT * FROM new_customer_data;

# COMMAND ----------

# MAGIC %md
# MAGIC > **This is what happens every time a new table lands and gets tagged. Zero additional governance work.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this generalizes — the migration math
# MAGIC
# MAGIC The pattern just demonstrated is a single tag → instant governance. Multiply that by Chris Allen's Exadata migration backlog and the operational shape becomes clear.
# MAGIC
# MAGIC | Per new table, RBAC world | Per new table, ABAC world |
# MAGIC |---|---|
# MAGIC | Identify sensitive columns manually | Auto-classification surfaces PII candidates within 24h |
# MAGIC | Mint roles for each access pattern | Subject attributes already populated from IdP groups |
# MAGIC | Author per-table grants / views | One catalog-scope policy, written once |
# MAGIC | Author per-column masks | Tag the column — mask attaches automatically |
# MAGIC | Re-review whenever the table evolves | Tag change is the only thing that needs to change |
# MAGIC
# MAGIC The work doesn't disappear — it **collapses upstream into tagging**, which has clearer owners and a much smaller surface area than per-table policy authoring.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What scales for free
# MAGIC
# MAGIC With Section 2 (auto-tagging + custom tags) + Section 3 (catalog-scope policies) in place, four things scale linearly with effort that used to scale combinatorially:
# MAGIC
# MAGIC - **New tables** — pickup is automatic; the tag is the policy hook
# MAGIC - **New columns** on existing tables — same path; tag → masked
# MAGIC - **New consumers** — adding a user to `hr_admins` (or any caller group) toggles their view of every masked column at once
# MAGIC - **New sensitivity classes** — adding a new `pii` value (e.g. `pii = credit_card`) is one new UDF + one new policy attachment, and every column anywhere that adopts the tag inherits it
# MAGIC
# MAGIC The thing that *doesn't* scale for free is **tag hygiene** — but that's an upstream problem the data classification agent + tag policies (Section 2 bonus) are designed to solve.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The PFG connection — governance that scales with the migration
# MAGIC
# MAGIC Chris Allen has a backlog of Exadata workloads queued up for migration. In a per-table-policy world, every one of those migrations is also a governance project — author the masks, attach them, re-test, document, hand off. The governance work becomes the bottleneck on the migration itself.
# MAGIC
# MAGIC In the world we just demonstrated:
# MAGIC
# MAGIC - The mask UDFs (`mask_ssn`, `mask_address`, future ones) are written **once**
# MAGIC - The catalog-scope attachment is set up **once** in Catalog Explorer
# MAGIC - Auto-classification handles the *what kind of data is this* tagging without human effort
# MAGIC - Domain teams own the *banner / owner / sensitivity* tagging for the tables they land
# MAGIC
# MAGIC Every Exadata table that lands with the right tags is **immediately governed**. The governance program stops being a tax on the migration and starts being a force multiplier on it.
# MAGIC
# MAGIC > **Your governance scales with your migration, not against it.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where it fails (honest)
# MAGIC
# MAGIC ABAC at this scale has three failure modes worth naming upfront — better to plan for them than be surprised:
# MAGIC
# MAGIC - **Untagged data** — if a sensitive column never gets tagged, it never gets masked. Mitigation: the classification agent catches well-known PII patterns; tag policies + the `data_owner` field route uncovered cases to humans who can fix them.
# MAGIC - **Tag drift** — someone applies `pii = SSN` (uppercase) and the mask doesn't fire because the policy matched the lowercase value. Mitigation: tag policies lock the vocabulary at the metastore level so the bad value is rejected at `ALTER TABLE` time.
# MAGIC - **Caller-condition complexity** — once policies blend multiple subject attributes (`is_account_group_member` AND `current_user_attribute`), the *why was this row returned?* question gets harder to answer at audit time. Mitigation: keep UDFs short; lean on the UC audit logs that record policy evaluation.
# MAGIC
# MAGIC None of these are unique to ABAC — they're tradeoffs you accept in exchange for the migration math working.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap — what we showed in 25 minutes
# MAGIC
# MAGIC - **A brand-new table** landed (`new_customer_data` — pretend Exadata)
# MAGIC - **No policy authoring** happened
# MAGIC - **Two `ALTER COLUMN SET TAGS`** statements brought it under governance
# MAGIC - The catalog-scope policy saw the tags and applied the masks automatically
# MAGIC - The same playbook scales to every table in Chris Allen's migration backlog

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fallback — if catalog-scope isn't set up yet
# MAGIC
# MAGIC If the Catalog Explorer policy attachment isn't in place in this workspace, the demo above won't auto-mask. Drop in the per-table attachments below to still make the generalization point — *the UDFs are reused; only the wiring differs*:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE new_customer_data ALTER COLUMN ssn          SET MASK mask_ssn;
# MAGIC ALTER TABLE new_customer_data ALTER COLUMN home_address SET MASK mask_address;
# MAGIC SELECT * FROM new_customer_data;
# MAGIC ```
# MAGIC
# MAGIC In production with catalog-scope attached, those two `SET MASK` lines disappear entirely — the tags are enough.

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 5 — Power BI Security
# MAGIC
# MAGIC **~20 min · slides + conversation**
# MAGIC
# MAGIC Everything we just demonstrated — row filters, column masks, catalog-scope policies — runs **server-side at Databricks**. That has a direct payoff for the BI tools sitting on top of UC, Power BI included. This section walks through what that means for Negin's PBI Centre of Excellence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture — what actually happens when PBI hits UC
# MAGIC
# MAGIC Power BI in DirectQuery mode pushes the user's query down to a Databricks SQL Warehouse. The warehouse asks Unity Catalog to evaluate the policies for the calling user. UC applies the row filter and the column mask **before the rows leave the warehouse**. Only the filtered, masked result is what travels back to PBI.
# MAGIC
# MAGIC The thing to internalize: **Power BI never sees the raw data.** If the mask says redact, the redacted bytes are what PBI receives. There is no path where the unredacted value briefly exists on the BI side, no client-side step that could be misconfigured to leak it.

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 980 440" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 22px; font-weight: 700; fill: #1B3139; }
    .stage-label { font-size: 14px; font-weight: 700; fill: #1B3139; }
    .stage-sub { font-size: 12px; fill: #425563; }
    .stage-label-white { font-size: 14px; font-weight: 700; fill: #FFFFFF; }
    .stage-sub-white { font-size: 12px; fill: #FFFFFF; opacity: 0.88; }
    .flow-label { font-size: 12px; fill: #425563; font-style: italic; }
    .flow-label-brick { font-size: 13px; fill: #FF3621; font-weight: 700; letter-spacing: 1px; }
    .boundary { font-size: 12px; fill: #FF3621; font-weight: 700; letter-spacing: 1.5px; }
    .box-soft { fill: #F9F7F4; stroke: #1B3139; stroke-width: 1.5; }
    .box-dark { fill: #1B3139; stroke: #1B3139; stroke-width: 1.5; }
    .box-brick { fill: #FF3621; stroke: #FF3621; stroke-width: 1.5; }
    .arrow-down { stroke: #425563; stroke-width: 2; fill: none; }
    .arrow-up { stroke: #2EB67D; stroke-width: 2; fill: none; }
    .boundary-line { stroke: #FF3621; stroke-width: 1.5; stroke-dasharray: 6 4; fill: none; }
  </style>
  <defs>
    <marker id="ah-d" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
      <polygon points="0 0, 10 5, 0 10" fill="#425563"/>
    </marker>
    <marker id="ah-u" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
      <polygon points="0 0, 10 5, 0 10" fill="#2EB67D"/>
    </marker>
  </defs>

  <text x="490" y="32" class="title" text-anchor="middle">Power BI → SQL Warehouse → Unity Catalog</text>

  <rect class="box-soft" x="260" y="60" width="220" height="60" rx="6"/>
  <text x="370" y="84" class="stage-label" text-anchor="middle">Power BI (DirectQuery)</text>
  <text x="370" y="104" class="stage-sub" text-anchor="middle">Entra-authenticated user</text>

  <line class="arrow-down" x1="370" y1="120" x2="370" y2="155" marker-end="url(#ah-d)"/>
  <text x="385" y="142" class="flow-label">SQL pushdown</text>

  <rect class="box-soft" x="260" y="160" width="220" height="60" rx="6"/>
  <text x="370" y="184" class="stage-label" text-anchor="middle">Databricks SQL Warehouse</text>
  <text x="370" y="204" class="stage-sub" text-anchor="middle">Asks UC to evaluate policies</text>

  <line class="arrow-down" x1="370" y1="220" x2="370" y2="255" marker-end="url(#ah-d)"/>

  <rect class="box-brick" x="260" y="260" width="220" height="60" rx="6"/>
  <text x="370" y="284" class="stage-label-white" text-anchor="middle">Unity Catalog</text>
  <text x="370" y="304" class="stage-sub-white" text-anchor="middle">Row filter + column mask applied</text>

  <line class="arrow-down" x1="370" y1="320" x2="370" y2="355" marker-end="url(#ah-d)"/>

  <rect class="box-dark" x="260" y="360" width="220" height="60" rx="6"/>
  <text x="370" y="384" class="stage-label-white" text-anchor="middle">Delta tables (ADLS / S3)</text>
  <text x="370" y="404" class="stage-sub-white" text-anchor="middle">Governed access only</text>

  <path class="arrow-up" d="M 530 390 C 620 390, 620 90, 530 90" marker-end="url(#ah-u)"/>
  <text x="700" y="200" class="flow-label-brick" text-anchor="middle">FILTERED + MASKED</text>
  <text x="700" y="220" class="flow-label-brick" text-anchor="middle">RESULT ONLY</text>

  <line class="boundary-line" x1="60" y1="142" x2="250" y2="142"/>
  <text x="155" y="132" class="boundary" text-anchor="middle">SECURITY BOUNDARY</text>
  <text x="155" y="158" class="flow-label" text-anchor="middle">raw data lives below this line</text>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identity flow — same person, same policies, every client
# MAGIC
# MAGIC The user's identity travels with the query end-to-end:
# MAGIC
# MAGIC - **Entra ID** authenticates the person opening the Power BI report
# MAGIC - **Databricks SSO** maps that Entra principal to a Databricks identity (the SCIM-synced user, with all their group memberships)
# MAGIC - **Unity Catalog** evaluates the row filter and column mask against that identity at query time
# MAGIC
# MAGIC The practical consequence: `is_account_group_member('hr_admins')` returns the same answer whether the query came from a notebook, a SQL Editor tab, or a Power BI dashboard. One identity, one policy decision, regardless of the front door.

# COMMAND ----------

displayHTML("""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 980 380" font-family="Helvetica, Arial, sans-serif">
  <style>
    .title { font-size: 22px; font-weight: 700; fill: #1B3139; }
    .center-label { font-size: 16px; font-weight: 700; fill: #FFFFFF; }
    .center-sub { font-size: 12px; fill: #FFFFFF; opacity: 0.88; }
    .client-label { font-size: 14px; font-weight: 700; fill: #1B3139; }
    .client-sub { font-size: 12px; fill: #425563; }
    .caption { font-size: 13px; fill: #425563; font-style: italic; }
    .box-soft { fill: #F9F7F4; stroke: #1B3139; stroke-width: 1.5; }
    .box-brick { fill: #FF3621; stroke: #FF3621; stroke-width: 1.5; }
    .arrow { stroke: #425563; stroke-width: 1.5; fill: none; }
  </style>
  <defs>
    <marker id="ah2" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
      <polygon points="0 0, 8 4, 0 8" fill="#425563"/>
    </marker>
  </defs>

  <text x="490" y="32" class="title" text-anchor="middle">Set once in UC — every client inherits</text>

  <rect class="box-brick" x="360" y="150" width="260" height="80" rx="6"/>
  <text x="490" y="180" class="center-label" text-anchor="middle">Unity Catalog</text>
  <text x="490" y="200" class="center-sub" text-anchor="middle">Row filters + column masks</text>
  <text x="490" y="216" class="center-sub" text-anchor="middle">defined once, evaluated per query</text>

  <rect class="box-soft" x="50" y="80" width="180" height="60" rx="6"/>
  <text x="140" y="105" class="client-label" text-anchor="middle">Databricks Notebook</text>
  <text x="140" y="124" class="client-sub" text-anchor="middle">SQL via cluster / warehouse</text>
  <line class="arrow" x1="230" y1="130" x2="360" y2="180" marker-end="url(#ah2)"/>

  <rect class="box-soft" x="50" y="240" width="180" height="60" rx="6"/>
  <text x="140" y="265" class="client-label" text-anchor="middle">SQL Editor</text>
  <text x="140" y="284" class="client-sub" text-anchor="middle">Direct SQL</text>
  <line class="arrow" x1="230" y1="260" x2="360" y2="210" marker-end="url(#ah2)"/>

  <rect class="box-soft" x="750" y="80" width="180" height="60" rx="6"/>
  <text x="840" y="105" class="client-label" text-anchor="middle">Power BI</text>
  <text x="840" y="124" class="client-sub" text-anchor="middle">DirectQuery (JDBC)</text>
  <line class="arrow" x1="750" y1="130" x2="620" y2="180" marker-end="url(#ah2)"/>

  <rect class="box-soft" x="750" y="240" width="180" height="60" rx="6"/>
  <text x="840" y="265" class="client-label" text-anchor="middle">Tableau / any JDBC</text>
  <text x="840" y="284" class="client-sub" text-anchor="middle">Same policy surface</text>
  <line class="arrow" x1="750" y1="260" x2="620" y2="210" marker-end="url(#ah2)"/>

  <text x="490" y="358" class="caption" text-anchor="middle">One identity (Entra → Databricks) → one policy decision, regardless of the client</text>
</svg>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set once, persist everywhere
# MAGIC
# MAGIC This is the operational shape Negin's team should care about:
# MAGIC
# MAGIC | Client | How it talks to UC | What it sees |
# MAGIC |---|---|---|
# MAGIC | Databricks notebook | SQL via cluster / warehouse | Same masks + filters |
# MAGIC | SQL Editor | Direct SQL on the warehouse | Same masks + filters |
# MAGIC | Power BI (DirectQuery) | JDBC over Entra-authenticated session | Same masks + filters |
# MAGIC | Tableau / any JDBC or ODBC client | Same JDBC surface | Same masks + filters |
# MAGIC
# MAGIC Define the policy **once** in UC. Every current and future client inherits it. The BI tool is just another caller — its security story is the data layer's security story.

# COMMAND ----------

# MAGIC %md
# MAGIC ## New in Runtime 18.1 — RLS-aware result caching
# MAGIC
# MAGIC Historically, queries against tables with row filters or column masks **bypassed the result cache** — caching a per-user-filtered result for the wrong user would be a security incident, so the safe default was to skip caching entirely. The price was performance: every PBI page render re-evaluated the policies from scratch, even for the same user clicking the same visual twice.
# MAGIC
# MAGIC Runtime 18.1 ships **policy-aware result caching**: the cache key now incorporates the evaluated policy and the calling principal. Same query, same principal, same policy state → cache hit. Different principal → cache miss, no leakage. The security model didn't change; the performance ceiling moved.
# MAGIC
# MAGIC For a dashboard refreshing across 200 store managers, this is the difference between *acceptable* and *snappy*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this means for Negin's PBI CoE
# MAGIC
# MAGIC Two shifts worth surfacing explicitly:
# MAGIC
# MAGIC - **No more parallel RLS implementation in PBI.** RLS roles defined in Power BI semantic models, DAX security filter expressions, per-workspace security tables — none of that needs to exist when the row filter already lives in UC. The semantic model gets to be about *semantics*, not security.
# MAGIC - **One audit story.** *"Why did this user see that row?"* has one answer, not two — and that answer lives in UC's audit logs, not split between PBI and the data layer.
# MAGIC
# MAGIC > **Negin's team writes zero PBI RLS for any table whose row filter is already enforced in UC.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion — questions to surface in the room
# MAGIC
# MAGIC Three to draw the conversation out:
# MAGIC
# MAGIC 1. **How are you currently handling RLS in Power BI? Are you maintaining separate security logic per semantic model?**
# MAGIC 2. **When a new dataset gets published to PBI, what's the process today to get it secured? Who owns it, how long does it take?**
# MAGIC 3. **What would it feel like if all of that was handled once, at the data layer, before PBI ever touches it?**
# MAGIC
# MAGIC The goal isn't to convince — it's to make the duplication visible. If Negin's team is already maintaining shadow RLS in PBI semantic models on top of the access controls that already live in the warehouse, the math is going to tell itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ The one thing not to do
# MAGIC
# MAGIC There's a tempting shortcut that breaks the whole model: giving Power BI **direct storage-level access** to ADLS or Blob — service principal credentials, account keys, SAS tokens, anything that lets the BI tool read the underlying files without going through the SQL Warehouse. **Don't.**
# MAGIC
# MAGIC The moment PBI reads files directly from storage:
# MAGIC
# MAGIC - UC is bypassed — no row filter, no column mask, no caller-condition check
# MAGIC - The masked values become the raw values, and PBI has no way to know the difference
# MAGIC - The audit trail goes dark — the security perimeter you just spent the day building no longer covers this access path
# MAGIC
# MAGIC The only governed path is **through the SQL Warehouse**, which lives behind UC. Databricks and Microsoft engineering are actively working on richer integration paths — direct semantic model integration, Fabric/OneLake patterns — all of which preserve UC governance by design. If a use case appears to require direct-storage access, flag it and wait for the governed path. **Working around UC is never the right answer for a regulated dataset.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - **PBI inherits UC's security** via DirectQuery — server-side enforcement, raw data never crosses the boundary
# MAGIC - **One identity** travels Entra → Databricks → UC; one policy decision applies regardless of client
# MAGIC - **Set once, persist everywhere** — notebook, SQL Editor, PBI, any JDBC/ODBC client see the same masks and filters
# MAGIC - **Runtime 18.1 unlocks performance** with policy-aware result caching — no security tradeoff
# MAGIC - **One thing not to do**: never grant PBI storage-level access that bypasses UC
# MAGIC
# MAGIC > **The PBI security conversation collapses from "what do we build in Power BI" to "what's already in UC." That's the whole pitch.**

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion — where the value compounds
# MAGIC
# MAGIC We covered a lot of ground. Let me consolidate the arc into the shape PFG can act on Monday morning.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we built, in one breath
# MAGIC
# MAGIC - **Section 1** — RBAC explodes combinatorially; ABAC scales because policy lives at *query time*, not at *assignment time*
# MAGIC - **Section 2** — Auto-classification plus custom governed tags give UC the resource attributes the policies need
# MAGIC - **Section 3** — Row filters and column masks are just SQL UDFs — `CASE WHEN` is the whole language
# MAGIC - **Section 4** — Catalog-scope policies turn the tag itself into the policy hook: tag a column, it's governed
# MAGIC - **Section 5** — Every client — notebook, SQL Editor, Power BI, any JDBC/ODBC tool — inherits the same enforcement, server-side

# COMMAND ----------

# MAGIC %md
# MAGIC ## Three teams, one model
# MAGIC
# MAGIC The model we walked through serves three PFG teams at once. Each owns a different piece, but they're all operating on the same artifact:
# MAGIC
# MAGIC | Team | What they own | What they stop maintaining |
# MAGIC |---|---|---|
# MAGIC | **Data governance team** | Tag policies, mask/filter UDFs, catalog-scope attachments | Per-table grants, per-banner views, role-explosion paperwork |
# MAGIC | **Negin's PBI CoE** | Semantic models, dashboards, distribution | Shadow RLS in PBI, parallel security tables, dual audit story |
# MAGIC | **Chris Allen's migration team** | Landing Exadata tables into UC with the right tags | Governance backlog blocking each table landing |
# MAGIC
# MAGIC Three workstreams, one source of truth. That's the shape worth installing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The PFG bet
# MAGIC
# MAGIC Chris Allen's Exadata workloads are landing in UC table by table. In a per-table-policy world, every one of those landings is its own governance project — author the masks, mint the roles, write the views, document the access patterns, hand off. **Governance becomes the schedule constraint on the migration itself.**
# MAGIC
# MAGIC In the world we just demonstrated:
# MAGIC
# MAGIC - The mask and filter UDFs are written **once**
# MAGIC - The catalog-scope policies are attached **once**
# MAGIC - Auto-classification surfaces PII without human effort
# MAGIC - Domain teams own the business tags for the tables they land
# MAGIC - Every downstream client — notebooks, PBI, JDBC — inherits the result automatically
# MAGIC
# MAGIC Each Exadata table that lands with the right tags is governed the moment it lands. The migration runs on its own clock.
# MAGIC
# MAGIC > **Your governance program becomes a multiplier on the migration, not a tax on it.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Three things to do Monday
# MAGIC
# MAGIC Three concrete moves to convert today's demo into next week's progress:
# MAGIC
# MAGIC 1. **Stand up the tag vocabulary.** Pick three to five governed keys — `sensitivity`, `data_owner`, `business_unit`, `banner` is a strong starting set — and create the tag policies in Catalog Explorer. Even an ungoverned tag key is a wedge you can lock down later.
# MAGIC 2. **Write the first two UDFs at catalog scope.** Pick the two most common masking patterns at PFG today (start with what auto-classification flags most often) and stand them up. Use this notebook's `mask_ssn` and `mask_address` as templates. Validate end-to-end with one tagged column on one table.
# MAGIC 3. **Pick one real Exadata table to land governed.** Choose a table from Chris Allen's queue — not a demo table — land it in UC with the right tags, and watch the catalog-scope policies fire. One end-to-end proof is worth more than any design doc.
# MAGIC
# MAGIC None of these require new tooling, new vendors, or org changes. They're a focused week.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The closing thought
# MAGIC
# MAGIC Most security models fail at scale because the people maintaining them can't keep up with the org's complexity. ABAC works at PFG's scale because **it doesn't ask them to.**
# MAGIC
# MAGIC The policy is small. The attributes do the work. The audit story is one story. Every new table, every new banner, every new analyst inherits the model the moment they show up in the systems that already know about them — Entra, UC, the IdP, the classification agent.
# MAGIC
# MAGIC > **Set the model once. Let the platform scale it.**
