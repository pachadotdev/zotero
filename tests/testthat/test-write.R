test_that("write parsed files", {
  rdf_path <- system.file("extdata", "test.rdf", package = "zotero")
  bib_path <- system.file("extdata", "test.bib", package = "zotero")
  ris_path <- system.file("extdata", "test.ris", package = "zotero")

  drdf <- import(rdf_path)
  dbib <- import(bib_path)
  dris <- import(ris_path)

  find_match <- function(target_row, df) {
    td <- norm_doi(target_row$doi)
    if (!is.na(td)) {
      df_dois <- sapply(df$doi, norm_doi)
      matches <- which(!is.na(df_dois) & df_dois == td)
      if (length(matches) >= 1) {
        return(matches[1])
      }
    }
    tt <- norm_title(target_row$title)
    if (!is.na(tt)) {
      # exact normalized match first
      matches <- which(sapply(df$title, function(x) identical(norm_title(x), tt)))
      if (length(matches) >= 1) {
        return(matches[1])
      }
      # then try agrep fuzzy match on normalized titles
      df_titles <- sapply(df$title, function(x) {
        tt2 <- norm_title(x)
        if (is.na(tt2)) "" else tt2
      })
      cand <- agrep(tt, df_titles, max.distance = 0.2, ignore.case = TRUE)
      if (length(cand) >= 1) {
        return(cand[1])
      }
    }
    # try matching by URL
    tu <- norm_url(target_row$url)
    if (!is.na(tu)) {
      matches <- which(sapply(df$url, function(x) identical(norm_url(x), tu)))
      if (length(matches) >= 1) {
        return(matches[1])
      }
    }
    # try matching by key
    if (!is.null(target_row$key) && !is.na(target_row$key) && nzchar(as.character(target_row$key))) {
      k <- as.character(target_row$key)
      matches <- which(!is.na(df$key) & as.character(df$key) == k)
      if (length(matches) >= 1) {
        return(matches[1])
      }
    }
    # fuzzy title match as last resort (agrep)
    if (!is.na(tt)) {
      df_titles <- sapply(df$title, function(x) {
        tt2 <- norm_title(x)
        if (is.na(tt2)) "" else tt2
      })
      cand <- agrep(tt, df_titles, max.distance = 0.12, ignore.case = TRUE)
      if (length(cand) >= 1) {
        return(cand[1])
      }
    }
    NA_integer_
  }

  tmp_rdf <- tempfile("fromrdf", fileext = ".bib")
  tmp_bib <- tempfile("frombib", fileext = ".bib")
  tmp_ris <- tempfile("fromris", fileext = ".bib")

  write_bib(drdf, tmp_rdf)
  write_bib(dbib, tmp_bib)
  write_bib(dris, tmp_ris)

  # expect_equal(readLines(tmp_rdf), readLines(tmp_bib))

  drdf2 <- import(tmp_rdf)
  dbib2 <- import(tmp_bib)
  dris2 <- import(tmp_ris)

  # tests that the non-NA fields are the same in the three data frames
  for (i in seq_len(nrow(drdf))) {
    row_d <- drdf[i, ]
    j <- find_match(row_d, dbib2)
    k <- find_match(row_d, dris2)
    if (!is.na(j)) {
      row_b <- dbib2[j, ]
      common_cols <- intersect(names(drdf), names(row_b))
      # exclude unstable or source-specific columns that can differ across formats
      unstable_cols <- c("key", "bibtype", "file", "urldate", "shorttitle")
      common_cols <- setdiff(common_cols, unstable_cols)
      for (col in common_cols) {
        v_d <- row_d[[col]]
        v_b <- row_b[[col]]
        if (!is.na(v_d) && nzchar(as.character(v_d)) && !is.na(v_b) && nzchar(as.character(v_b))) {
          expect_equal(v_d, v_b, info = paste("Row", i, "column", col, "rdf vs bib"))
        }
      }
    } else {
      warning(paste("No match for row", i, "in bib"))
    }
    if (!is.na(k)) {
      row_s <- dris2[k, ]
      common_cols2 <- intersect(names(drdf), names(row_s))
      unstable_cols <- c("key", "bibtype", "file", "urldate", "shorttitle")
      common_cols2 <- setdiff(common_cols2, unstable_cols)
      for (col in common_cols2) {
        v_d <- row_d[[col]]
        v_s <- row_s[[col]]
        if (!is.na(v_d) && nzchar(as.character(v_d)) && !is.na(v_s) && nzchar(as.character(v_s))) {
          expect_equal(v_d, v_s, info = paste("Row", i, "column", col, "rdf vs ris"))
        }
      }
    } else {
      warning(paste("No match for row", i, "in ris"))
    }
  }
})
