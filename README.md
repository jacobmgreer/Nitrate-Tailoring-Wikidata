# Wikidata SPARQL queries media-related data

# Nitrate-Tailoring-Wikidata

**Role in who-db:** Nightly Wikidata catalog builder. Executes a curated library of SPARQL queries against the Wikidata Query Service and packages the results as Parquet files in a GitHub Release — defining which QIDs are relevant to the `who-db` ecosystem.

## GitHub Release naming

Releases follow a `YYYY-MM-DD` tag pattern. All query outputs for a given night are uploaded to the same release.

### Media-Related Alternate IDs

**Note:** other wikidata properties 
https://www.wikidata.org/wiki/Wikidata:WikiProject_Movies/Properties/identifiers
https://www.wikidata.org/wiki/Template:Movie_person_properties
https://www.wikidata.org/wiki/Template:Movie_properties
https://www.wikidata.org/wiki/Template:Movie_industry_properties
https://www.wikidata.org/wiki/Template:Television_properties
https://en.wikipedia.org/wiki/Category:WikiProject_Film_templates

https://en.wikipedia.org/wiki/Category:WikiProject_Pornography
https://en.wikipedia.org/wiki/Template:Adult_entertainment_awards


# Nitrate Tailoring Wikidata

This repository defines a **highly reproducible SPARQL query pipeline** for querying, processing, and managing domain-specific Wikidata records. It uses **SPARQL** for querying entities, **R-based scripting** for transformations, and **DuckDB** for lightweight, embedded analytics. Outputs are versioned and released via **GitHub Actions** workflows.

---

## Intent and Invariants

### **What This Code Does**
- Executes domain-specific SPARQL queries against the Wikidata public endpoint.
- Extracts relevant `results.bindings` and processes them with type-safe transformations into DuckDB.
- Produces compact, queryable outputs in **Apache Parquet** format.
  
---

## Non-Obvious Patterns

### **Why the Pipeline Avoids In-Memory Joins**
R provides strong data manipulation tools, but this repository delegates joins and filtering to **DuckDB** for the following reasons:
1. **Columnar Efficiency**: Large SPARQL datasets can be processed efficiently using DuckDB's columnar engine.
2. **SQL as Documentation**: The SQL macros in DuckDB clarify query logic better than multiline R code.
3. **Scalability**: DuckDB handles large workloads in constant memory, which is critical for automation tasks.

---

### **Why SPARQL Results Are Saved as JSON Before Processing**
1. **Deterministic Pipelining**:
   - The `json/temp_<prefix>.json` files provide a clean checkpoint that ensures reproducibility if SPARQL queries fail mid-run.
2. **Flexible Error Handling**:
   - Parsing failures (e.g., format changes in `results.bindings`) are isolated to the SPARQL step.

---

## Execution Model

1. **SPARQL Queries**:
   - Queries in `SPARQL/*` are stored as `.sparql` flat files for better organization.
   - Each file represents an independent pipeline.
   - Outputs are parsed into JSON objects, saved in `json/`.

2. **DuckDB Transformation**:
   - JSON outputs are processed directly into DuckDB temporary tables.
   - Data is cleaned, validated, and exported as Parquet files in `release/`.

3. **GitHub Actions Workflow**:
   - Defined in [`sparql_matrix_release3.yml`](.github/workflows/sparql_matrix_release3.yml):
     - Runs nightly, weekly, or ad hoc.
     - Automatically attaches outputs to GitHub Releases.
