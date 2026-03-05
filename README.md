# Wikidata SPARQL queries media-related data

# Nitrate-Tailoring-Wikidata

**Role in who-db:** Nightly Wikidata catalog builder. Executes a curated library of SPARQL queries against the Wikidata Query Service and packages the results as Parquet files in a GitHub Release — defining which QIDs are relevant to the `who-db` ecosystem.

## What this repo does

`Nitrate-Tailoring-Wikidata` maintains a directory of `.sparql` files organized by domain. Each night, GitHub Actions executes all queries and uploads the resulting Parquet files to a versioned GitHub Release. These releases are the **catalog manifest** for `db-wikidata`: every QID that appears in any release file is a QID that `db-wikidata` should maintain a local property cache for.

This repo **does not store Wikidata property data** — it only stores the catalog of relevant QIDs + their last-updated timestamps.

## GitHub Release naming

Releases follow a `YYYY-MM-DD` tag pattern. All query outputs for a given night are uploaded to the same release.

## Tech stack

- **Language:** R (`httr`, `jsonlite`, `arrow`, `dplyr`)
- **Orchestration:** GitHub Actions (nightly cron)
- **Query protocol:** SPARQL 1.1 over HTTPS, JSON response format
- **Storage:** Apache Parquet, published as GitHub Release assets

<!-- 
| Dataset | Property | Note | 
| --- | --- | --- |
|  |  |  |
-->

## SPARQL queries

### Credit-Related Properties

**Note:** some credits might be related to other unexpected forms of performance/entertainment (may include live/stage).

| Type | Property | Note | 
| --- | --- | --- |
| after a work by | P1877 | might not be film |
| cast member | P161 | |
| cinematographer | P344 | 'director of photography' |
| composer | P86 |  |
| costume designer | P2515 |  |
| executive producer | P1431 |  |
| film editor | P1040 |  |
| producer | P162 |  |
| production designer | P2554 |  |
| recorded-participant | P11108  |  |
| screenwriter | P58 |  |
| sound designer | P5028 |  |
| voice actor | 725 |  |
| translator | P655 | monitored specifically from film/tv content |

### Media-Related Alternate IDs

**Note:** other wikidata properties https://www.wikidata.org/wiki/Wikidata:WikiProject_Movies/Properties/identifiers

| Dataset | Property | Note | 
| --- | --- | --- |
| Czech-Slovak film database (čsfd) film ID | P2529 | |
| EIDR content ID | P2704 | |
| CITWF title ID | P9146 | also known as FCOC |
| Film Affinity film ID | P480 | |
| IMDb | P345 | broken out by 'tt-' prefix |
| Letterboxd film ID | P6127 | |
| Moviebuff ID | P12320  |  |
| Moviemeter film ID | P1970 |  |
| Netflix ID | P1874 |  |
| German Online-Filmdatenban OFDb film id | P3138 |  |
| PORT film ID | P905 |  |
| Swedish Film Database ID | P2334 |  |
| TCM Movie Database film ID | P2631 |  |
| The Numbers movie ID | P3808 |  |
| TMDB movie ID | P4947 |  |
| TMDB TV series ID | P4983 |  |

### Music-Related Alternate IDs

| Dataset | Property | Note | 
| --- | --- | --- |
| Discogs master ID | P1954 |  |
| Spotify album ID | P2205 |  |

### Media-Related Companies

| Dataset | Property | Note | 
| --- | --- | --- |
| Discogs label ID | P1955 |  |
| EIDR party ID | P12142 |  |
| IMDb company ID | P345 | broken out by 'co-' prefix |
| TMDB company ID | P11806 |  |

### Media-Related Profession Data

| Dataset | Property | Note | 
| --- | --- | --- |
| IMDb person ID | P345 | broken out by 'nm-' prefix |
| TCM Movie Database person ID | P3056 |  |
| TMDB person ID | P4985 |  

### Music-Related Profession Data

| Dataset | Property | Note | 
| --- | --- | --- |
| Apple Music artist ID (U.S. version) | P2850 |  |
| ACE Repertory publisher ID | P10550 |  |
| Bandcamp profile ID | P3283 |  |
| DAHR artist ID | P4457 |  |
| Discogs artist ID | P1953 |  |
| Discogs label ID | P1955 |  |
| Last.fm ID | P3192 |  |
| Mixcloud ID | P9509 |  |
| MusicBrainz artist ID | P434 |  |
| SoundCloud ID | P3040 |  |
| Spotify artist ID | P1902 |  |

### Film Awards and Media Events

| Dataset | Property | Note | 
| --- | --- | --- |
| FilmFreeway ID | P6762 |  |
| IMDb event ID | P345 | broken out by 'ev-' prefix |
| IMDb event instance ID | P345 | broken out by 'ev-' and a year '/YYYY' prefix |

### Monitoring Other Alt-Identifiers

| Dataset | Property | Note | 
| --- | --- | --- |
| Facebook page ID | P4003 |  |
| Facebook username | P2013 |  |
| Instagram username | P2003 |  |
| Internet Archive ID | P724 |  |
| Myspace ID | P3265 |  |
| OpenCorporates ID | P1320 |  |
| TikTok username | P7085 |  |
| X (Twitter) username | P2002 |  |
| Vimeo ID | P4015 |  |
| VK username | P3185 |  |
| YouTube channel ID | P2397 |  |
| YouTube handle | P11245 |  |
