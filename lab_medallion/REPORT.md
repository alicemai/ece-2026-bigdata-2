# Lab DBT: Bronze to Silver transformation with dbt and DuckDB

## 1. Introduction

In this lab I built a small data pipeline with dbt and DuckDB. The raw CSV files (`users.csv` and `orders.csv`) were already in the bronze layer of my S3 bucket (`user-a-coste-ece`). I declared them as dbt sources, built a silver layer with cleaned and typed models, tested the data, and exported the result to Parquet.

The pipeline is:

```text
S3 bronze (CSV) -> dbt + DuckDB -> silver models (views) -> Parquet on S3
```

## 2. Setup

I added `dbt-duckdb` to my uv project and created the project `lab_medallion` with `dbt init`. The `profiles.yml` file points to a local DuckDB file, and the S3 access goes through the `s3_onyxia_connection` secret from the DuckDB lab. `dbt debug` returned "All checks passed".

I put `target/`, `logs/`, `dbt_packages/` and `*.duckdb` in the `.gitignore`, because they are generated files and not source code.

## 3. Bronze layer

The two CSV files are declared as sources in `models/bronze/sources.yml`, using the `external_location` option of dbt-duckdb so that DuckDB reads them directly on S3. dbt does not modify them.

**Why leave the bronze files untouched?**
If a bug is found in a transformation months later, the original data is still there. I can fix the SQL and rebuild the silver layer without losing anything, and I keep a full history of what was received.

**Which format for hourly ingestion over several years?**
I would use Parquet instead of CSV. It is columnar, compressed and typed, so it is much faster to scan. I would partition it by date (for example `orders/year=2026/month=10/day=14/`), so that a query only reads the partitions it needs and each hourly load just adds new files.

## 4. Silver layer

I created one model per source table:

- `stg_users`: columns renamed (`uuid` became `user_id`, `mail` became `email`), cast to proper types, text cleaned (`trim`, `lower`, `upper`), and the two-line address split into `street`, `city`, `state` and `zip_code`. Military addresses like `DPO AP 09617` have no city, so the city is `NULL` for them.
- `stg_orders`: typed columns, and a deduplication with `row_number()` that keeps one row per order, so the model still works if the bronze layer is replayed.

Both models are views. `dbt run` created them in the `main_silver` schema.

**What happens when a view is queried, and when is it better than a table?**
A view stores only the SQL query, so the transformation runs again every time someone reads it. The result is always up to date and takes no storage. A view is a good choice for small data or cheap queries. For heavy transformations that are read often, a table is better.

**How many bytes does a UUID take?**
A native `UUID` uses 16 bytes. As text it has 36 characters (32 hexadecimal digits and 4 dashes), so about 36 bytes as a `VARCHAR`, more than twice as much.

## 5. Data tests

I declared generic tests in `silver.yml`: `unique` and `not_null` on the keys, `accepted_values` on `sex`, `quantity` and `product`, and `relationships` between `stg_orders.user_id` and `stg_users.user_id`. I also wrote singular tests in `tests/`: `assert_orders_have_users`, `assert_quantity_positive`, `assert_orders_after_user_birth` and `assert_zip_matches_state`.

All the generic tests passed. I also used `--store-failures` to look at how failing tests are saved in the `main_dbt_test__audit` schema. Because all of my tests passed at that point, the failure tables were empty.

### 5.1 Orders placed before the user's birth

The test `assert_orders_after_user_birth` failed with **288 rows**. The cause is that the orders are dated 2020, while Faker generates birthdates relative to the current date, so some users are "born" after their first order. The problem is in the source data, so the bronze layer cannot be corrected.

Possible strategies are to reject the invalid records, replace the invalid birthdates with `NULL`, ask the producer to fix the generator, or accept the problem for now. I first tried the last one with `severity: warn`, which gave `WARN=1` instead of an error. Later I implemented the second one in exercise 3.

**Which strategy for which dashboard?**

- **Age of the customers:** I would replace the invalid birthdates with `NULL` and exclude them, because wrong ages would distort the results.
- **Sales per region:** I would accept the issue with a warning, because the birthdate has no effect on the sales per region.

### 5.2 ZIP codes

The generated addresses combine a state with a random ZIP code (for example `KY 01352`). To detect this, a test must check that each ZIP code falls within the valid range for its state. This requires reference data linking states to ZIP ranges, which should come from an official source like USPS. My test `assert_zip_matches_state` flagged **49 users** as warnings.

## 6. Full pipeline and lineage

I deleted the DuckDB file and ran `dbt build`, which runs the models and then their tests in dependency order. With `dbt ls` I listed the models and the graph behind `stg_orders`.

dbt knows the dependencies through two functions:

- `source('bronze', 'orders')` refers to an external raw dataset;
- `ref('stg_users')` refers to another dbt resource.

The lineage is `bronze.users -> stg_users` and `bronze.orders -> stg_orders`. I generated the documentation with `dbt docs generate --static` and checked the lineage graph in the browser.

## 7. Parquet export

I exported `stg_users` and `stg_orders` to `s3://user-a-coste-ece/silver/dbt/users.parquet` and `orders.parquet` with `COPY ... TO ... (FORMAT parquet)`. I queried the files directly with `read_parquet`, and the orders file returned **2829 rows**, the same as the view.

A view is a logical materialization: only the query is stored, and the transformation runs at every read. A Parquet file is a physical materialization: the transformation runs once during the export, and later queries read the stored result.

## 8. Exercises

1. **`username_normalized`:** I added `lower(trim(username))` to `stg_users`, documented the column in `silver.yml` and added a `not_null` test.
2. **Order validation:** the positive quantity is checked by the singular test `assert_quantity_positive`. The expected products and the existence of each `user_id` are checked by generic tests (`accepted_values` and `relationships`).
3. **Invalid birthdates:** in `stg_users`, I compute the date of each user's first order. If the birthdate is later than that date, it is replaced by `NULL`, and a boolean column `birthdate_is_valid` keeps track of it. **5 users** had an invalid birthdate. After this change, `assert_orders_after_user_birth` passes.
4. **ZIP codes:** I created the seed `seeds/states.csv` with a ZIP range per state. It is used by a `relationships` test on `state` and by `assert_zip_matches_state`.
   - *Where should the reference data come from?* From an official source like USPS.
   - *Should it be a seed?* Yes if it is small and stable, because it is versioned in git. A large or frequently updated list would be better loaded as a table by the ingestion process.
   - *Should the validation happen in silver?* Yes, as a test on the cleaned data. Whether to reject the rows is a separate decision.
   - *What happens when the reference data changes?* I update the CSV, run `dbt seed` again and rerun the tests. The git history keeps track of the change.
5. **Materialization experiment:** I switched the silver models to `table`, ran `dbt build`, compared, and restored `view`.

   | | View | Table |
   |---|---|---|
   | Where is the data stored? | nowhere, only the query | in the DuckDB file |
   | When does the transformation run? | at every query | once, at build time |
   | If the source CSV changes | seen at the next query | stale until the next build |
   | Which uses more storage? | none | more |
   | Avoids re-reading S3 at each query | no | yes |

## 9. Problems I ran into

- **Expired S3 credentials:** the first `aws s3 ls` failed with `InvalidAccessKeyId`, because the temporary Onyxia credentials had expired. I restarted the service to get new ones.
- **Lost files after the restart:** the new service did not keep my files, so I cloned the repository again from GitHub, ran `uv sync`, and recreated the DuckDB S3 secret.
- **GitHub login:** pushing failed with an authentication error in the new service, which I fixed with a personal access token.
- **Mixed-up prompts:** I sometimes pasted YAML or SQL into the terminal instead of a file, and typed a shell command inside the DuckDB shell. I learned to check the prompt (`$` means shell, `duckdb >` means SQL).

## 10. Conclusion

I now have a working bronze-to-silver pipeline: the raw files stay unchanged, dbt builds typed and cleaned models, and the tests show real data quality problems (invalid birthdates and ZIP codes). The warnings show that dbt can document a data issue without blocking the pipeline. The Parquet export shows how the same silver data can be stored physically.