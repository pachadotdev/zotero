library(RSQLite)
library(purrr)

devtools::load_all()

# con <- dbConnect(RSQLite::SQLite(), "~/Zotero/zotero.sqlite", synchronous = NULL)

# dbListTables(con)

# parse exported data ----
drdf <- import("inst/extdata/test.rdf")
dbib <- import("inst/extdata/test.bib")
dris <- import("inst/extdata/test.ris")

colnames(drdf)
colnames(dbib)
colnames(dris)

dsql <- collection("test")

# colnames(dsql)

map(dsql$key, function(x) colnames(key(x)))

key(dsql$key[1])
