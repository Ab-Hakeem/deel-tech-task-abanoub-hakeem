## Description & motivation
<!---
Describe your changes, and why you're making them. Is this linked to an open
issue, a Trello card, or another pull request? Link it here.
-->
Note: here is where I would put a ref for the jira ticket i.e: 
`Link to JIRA ticket: https://deel.atlassian.net/browse/AH-8223`

This project was designed with the philosophy that Data Engineering is the backbone of financial trust. By implementing a modular, prefix-aligned architecture, I have provided a solution that is not only audit-ready but also strategically positioned for the next phase of Deel's growth. The included Audit Marts and Polymorphic Trend models ensure that both Finance and Executives have a clear, reliable window into Globepay's performance.

This PR introduces a robust, modular data model designed to transform raw "Globepay" data into high-confidence, analytics-ready assets. The architecture follows dbt best practices, prioritising scalability and data integrity to support the brand's rapid expansion.

The model is structured across three distinct layers: Staging, Data Intermediate and Data Mart layers in which we will touch on further down. Firstly, we have to establish a purpose for this task. The objective of this project is to integrate disparate data sources from Globepay (Acceptance and Chargeback reports) into a unified Single Source of Truth. By standardising settlement amounts in USD and resolving data gaps, this pipeline empowers the Finance and Operations teams to monitor acceptance rates, identify regional friction, and perform proactive data auditing.

## Part 1
Key objectives were:
1. Preliminary data exploration
2. Summary of your model architecture
3. Lineage graphs
4. Tips around macros, data validation, and documentation


1. Data Exploration & Investigation
Before modelling, a thorough exploration of the source CSVs revealed several critical requirements:

- Currency Normalisation: Exchange rates were provided as semi-structured JSON strings within the acceptance report. A dynamic extraction strategy was required to calculate USD values.

- Entity Resolution: Both reports used a shared identifier, which we standardised to `PAYMENT_ID` for consistent joining.

- Data Gaps: Initial exploration showed that there was potential for not all accepted payments existed in the chargeback report, necessitating a robust "Anti-Join" audit strategy.

Whilst conducting some preliminary data exploration, it was found that:
- The `external_ref` field (which we renamed as `payment_id`) was unique for both datasets. We conducted a development test using `QUALIFY COUNT(*) OVER (PARTITION BY payment_id) > 1`. This, as expected returned no duplicates for either of the staging models for `acceptance_report` and `chargeback_report ` We added a permanent level of security over this through the use of dbt tests on the PK (payment_id) of `unique` and `not_null` tests.

- We also found that a minimal amount of the records in  `globepay_acceptance_report` had minus figures in the amount. This could be a data integrity issue and would have needed to be revisited.

- `rates` field: whilst the rates field had all the attributes of a JSON, it was actually a `string` datatype masquerading as a JSON. We parsed this string into a JSON in our modelling so we could calculated the USD conversion amounts against the exchange rates. 

There were also 2 more finds regarding the status fields in both the `acceptance_report` and `chargeback_report` in which both `status` in each of the datasets always returned as `true`. 

2.  Summary of your model architecture
The project is built on a Modular Layered Architecture (Staging, Intermediate, and Data Mart). This structure ensures that data flows through a rigorous transformation pipeline where logic is isolated, tested, and reusable.


The Staging Layer (stg_) 
- The foundation of the project consists of 1-to-1 mappings of the raw source data (Acceptance and Chargeback reports).
- Purpose: To clean, type-cast, and standardise raw fields into a consistent format.
- Ingestion: We utilised dbt Seeds for the ingestion of the provided CSV datasets and the Country Mapping file.
- Best Practice: No complex business logic is applied here; the goal is simply to create a clean data product from which all downstream models can draw.
- We wanted to keep the staging layer as close to the raw datasets as possible. However, I did rename various fields to fit a naming convention that is human readable to analysts and other stakeholders of all technical levels.
We utilised dbt Seeds to ingest the raw CSV data into Snowflake, ensuring data types were strictly enforced via dbt_project.yml.

stg_globepay__acceptance: The base atom for all payment attempts.

stg_globepay__chargebacks: The base atom for dispute data.

globepay_country_mapping (Seed): Created to denormalise ISO codes into human-readable country names (e.g., "AE" → "United Arab Emirates").

Best Practice Applied: All staging models are 1-to-1 with source data, featuring standardised naming conventions and explicit type casting.
 

The Intermediate Layer (int_) — The Molecular Layer
- This is the layer with all of the heavily lifting in the project, where disparate staging models are combined into logic-data products.
- Standardisation: We utilised dynamic JSON extraction to parse exchange rates and calculate a standardised USD Settlement Amount for every transaction.
- Transformation: This layer handles the complex joins between payments and chargebacks and integrates the Country Mapping seed.
- Auditing: We implemented an Anti-Join strategy (NOT EXISTS) to identify transactions present in the Acceptance report but missing from the Chargeback report, creating a specialised audit module.

The Data Mart Layer (dm_)
- The final layer consists of highly optimised, "display-only" models tailored for end-user consumption. We also created further data products in the data mart layer to answer the specific questions that were requested by Deel/analysts. 
- Grain Preservation: Marts are materialised as tables to maximise BI performance and are organised by business entity (e.g., Payments, Trends, Audits).
- Human-Readable: All IDs and codes are enriched with human-readable labels (e.g., Full Country Names) and pre-calculated metrics (e.g., Acceptance Rates).
- Separation of Concerns: By keeping the Marts as simple "displayers" of the Intermediate modules, we ensure that changes to business logic only need to be made once in the intermediate layer.

Naming Convention: As we spoke previously about Data Marts having the suffix of `_model`, we have also prefixed each of the models depending on what data layer they are stored. The purpose of this is for visibility reasoning and ease of debugging. For instance, we run a daily job and one of the models fail, depending on the prefix, it eases the debugging process as you will immediately know where to find this model. If I were creating a production envoriment where multiple teams will commit to, I would have team dependent model naming conventions so that the associated team would know that this is their ownership to fix i.e. data platform = dp_, customer service = cs_

For our example in this task, we have gone with the data layer approach:
```
Staging: stg_
Data Intermediate: int_
Data Mart: dm_
```

3. Lineage 

<img width="1332" height="438" alt="Screenshot 2026-02-13 at 17 44 50" src="https://github.com/user-attachments/assets/771f6e9e-878f-4d74-ae42-61dda9543386" />

--------------------------------------------------------------------------------------------------------------------
 


## Part 2: Business Insights & Query Walkthrough
The goal of this layer was to turn standardised, audit-ready data into clear answers for three critical business questions. By separating the logic (Intermediate) from the presentation (Marts), we created a robust reporting framework.

1. Acceptance Rate Over Time
The Challenge: Stakeholders need to see acceptance rates over time to identify performance trends or sudden technical failures.

Logic (Intermediate): We pre-calculated the rate using sum(is_accepted) / count(*) across multiple time grains (daily, weekly, and monthly) to ensure consistent definitions.

Presentation (Mart): `dm_globepay__agg_acceptance_trends` provides a filtered view for BI tools.

Final Answer Query:
```
-- View daily acceptance rates for the current month
SELECT 
    EVENT_DATE, 
    ACCEPTANCE_RATE,
    TOTAL_ATTEMPTS
FROM {{ ref('dm_globepay__agg_acceptance_trends') }}
WHERE TIME_GRAIN = 'day'
ORDER BY EVENT_DATE DESC;
```

I created as such because I wanted to give the end user the ability to aggregate up to a weekly, monthly, quarterly or yearly grain. The data product as it stands stacks all of these time_grain periods. 

If I had more time, I would aggregate further by creating sums of those time grains. 

2. Regional Friction: Declined Volume > $25M
The Challenge: Finance needs to know which countries are losing the most "opportunity" in USD to prioritise provider optimisation.

Logic (Intermediate): We filtered specifically for DECLINED payments and summed the standardised AMOUNT_USD calculated in the transformation molecules.

Presentation (Mart): `dm_globepay__declined_payments_over_25m_country_list` isolates only the critical outliers.

```
-- Identify high-priority regions with over $25M in declined volume
SELECT 
    COUNTRY_NAME, 
    TOTAL_DECLINED_USD
FROM {{ ref('dm_globepay__declined_payments_over_25m_country_list') }}
ORDER BY TOTAL_DECLINED_USD DESC;
```

3. Data Integrity: Missing Chargeback Data
The Challenge: Operations needs to find records present in the Acceptance file that were never reported in the Chargeback file.

Logic (Intermediate): We implemented a NOT EXISTS Anti-Join to detect payments that have no matching record in the chargeback source, accounting for potential ID mismatches via TRIM(UPPER(...)).

Presentation (Mart): `dm_globepay__audit_missing_chargebacks` acts as a specialised "Work Queue."
```
-- Generate a 'Hit List' of transactions missing from the chargeback report
SELECT 
    PAYMENT_ID, 
    CREATED_AT, 
    COUNTRY_NAME, 
    AUDIT_NOTE
FROM {{ ref('dm_globepay__audit_missing_chargebacks') }}
ORDER BY CREATED_AT DESC;
```

I did not find any missing chargeback data for transactions. There were an equal amount of records between the acceptance and chargebacks datasets, and the payment_id (external_ref) was always populated. 


## If I had more time
1. Advanced Macro & Package Implementation
- DRY Currency Conversion: I would refactor the dynamic JSON extraction and USD conversion logic into a reusable dbt Macro. This would allow Deel to onboard new payment providers (e.g., Worldpay or Stripe) using the exact same financial logic, ensuring consistency across the global ledger.
- dbt-utils & dbt-expectations: I would integrate the dbt-utils package for more complex surrogate key generation and dbt-expectations to implement financial-grade testing (e.g., verifying that exchange rates fall within expected historical variances).

2. Enhanced Data Governance & Quality
- Slim CI Implementation: I would set up a CI/CD pipeline using "Slim CI." This ensures that when a developer makes a change, dbt only runs and tests the modified models and their downstream dependencies, significantly reducing Snowflake credit consumption and deployment time.
- Source Freshness Alerts: I would implement freshness blocks in the src_globepay.yml. Since financial reports are time-sensitive, we need automated alerts if the Globepay Acceptance or Chargeback files stop arriving on schedule.

3. Performance & Scalability
- Incremental Materialisation: As transaction volumes grow into the millions or billions, I would transition the dm_globepay__fact_payments table from a table materialisation to incremental. This would allow dbt to only process new transactions rather than rebuilding the entire history every run.
- dbt Snapshots: I would implement Type-2 Slowly Changing Dimensions (SCD Type 2) via dbt Snapshots on the payment statuses. This would allow the business to track when a payment moved from 'PENDING' to 'ACCEPTED' or 'DECLINED', providing a full audit trail of the transaction lifecycle.

4. Observability & Metadata
- Exposure Tracking: I would define exposures in dbt to link our Marts directly to the specific Looker dashboards or downstream internal tools they feed. This creates a full "End-to-End" lineage, allowing us to perform impact analysis before making any breaking changes to the upstream SQL.

5. Model specific considerations
- acceptance rates over time: If I had more time, I would aggregate further by creating sums of those time grains. 
- converting a string to a JSON: In hindsight, I would use a `TRY_PARSE_JSON`. This is because the benefits of the following 2 commands are greater than what we have used.
```
SELECT TRY_PARSE_JSON('{"keyx"x:CORRUPT"xxx1"}'); -- this would return a null result
SELECT PARSE_JSON('{"keyx"x:CORRUPT"xxx1"}') -- this would break our query, just as what we have done
```
- I wouldn't usually build data products to answer specific questions, just queries that could answer these questions but for the purpose of this tech task, I thought it would be a good use case to build data products in the data marts layer to answer those questions.



## Pre Deployment Steps
- [ ]: Run every model in from staging, data intermediate, data mart
- [ ]: Assertion tests on PK via `unique`, `not_null` tests
- [ ]: Whilst developing, run qualify commands on PK 
-->

## Post Deployment Steps
- [ ]: N/A for this task
