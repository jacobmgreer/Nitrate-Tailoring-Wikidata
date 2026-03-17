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
