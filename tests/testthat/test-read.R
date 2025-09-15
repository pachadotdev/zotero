test_that("read from exported files", {
  # load_all()

  rdf_path <- system.file("extdata", "test.rdf", package = "zotero")
  bib_path <- system.file("extdata", "test.bib", package = "zotero")
  ris_path <- system.file("extdata", "test.ris", package = "zotero")

  drdf <- import(rdf_path)
  dbib <- import(bib_path)
  dris <- import(ris_path)

  # drdf$title
  # dbib$title

  # ensure the three imports have the same number of rows
  expect_equal(nrow(drdf), nrow(dbib))
  expect_equal(nrow(drdf), nrow(dris))

  # use normalization helpers exported from the package: norm_title(), norm_doi(), norm_journal(), norm_url(), extract_last_names()

  find_match <- function(target_row, df) {
    td <- norm_doi(target_row$doi)
    if (!is.na(td)) {
      df_dois <- sapply(df$doi, norm_doi)
      matches <- which(!is.na(df_dois) & df_dois == td)
      if (length(matches) >= 1) return(matches[1])
    }
    tt <- norm_title(target_row$title)
    if (!is.na(tt)) {
      # exact normalized match first
      matches <- which(sapply(df$title, function(x) identical(norm_title(x), tt)))
      if (length(matches) >= 1) return(matches[1])
      # then try agrep fuzzy match on normalized titles
      df_titles <- sapply(df$title, function(x) { tt2 <- norm_title(x); if (is.na(tt2)) "" else tt2 })
      cand <- agrep(tt, df_titles, max.distance = 0.2, ignore.case = TRUE)
      if (length(cand) >= 1) return(cand[1])
    }
    # try matching by URL
    tu <- norm_url(target_row$url)
    if (!is.na(tu)) {
      matches <- which(sapply(df$url, function(x) identical(norm_url(x), tu)))
      if (length(matches) >= 1) return(matches[1])
    }
    # try matching by key
    if (!is.null(target_row$key) && !is.na(target_row$key) && nzchar(as.character(target_row$key))) {
      k <- as.character(target_row$key)
      matches <- which(!is.na(df$key) & as.character(df$key) == k)
      if (length(matches) >= 1) return(matches[1])
    }
    # fuzzy title match as last resort (agrep)
    if (!is.na(tt)) {
      df_titles <- sapply(df$title, function(x) { tt2 <- norm_title(x); if (is.na(tt2)) "" else tt2 })
      cand <- agrep(tt, df_titles, max.distance = 0.12, ignore.case = TRUE)
      if (length(cand) >= 1) return(cand[1])
    }
    NA_integer_
  }

  # Row-wise comparisons ----------------------------------------------------
  for (i in seq_len(nrow(drdf))) {
    row_rdf <- drdf[i, , drop=FALSE]
    j_bib <- find_match(row_rdf, dbib)
    j_ris <- find_match(row_rdf, dris)
    expect_false(is.na(j_bib), info = paste("no match in bib for row", i))
    expect_false(is.na(j_ris), info = paste("no match in ris for row", i))

    # compare title if present in imports; imports are normalized by the reader
    t_r <- as.character(row_rdf$title)
    t_b <- as.character(dbib$title[j_bib])
    t_ris <- as.character(dris$title[j_ris])
    # simplified title comparison: identical or substring containment or agrep match
    if (!is.na(t_b) && nzchar(t_b)) {
      ok <- identical(t_r, t_b) || grepl(t_r, t_b, fixed = TRUE) || grepl(t_b, t_r, fixed = TRUE) || length(agrep(t_r, t_b, max.distance = 0.2, ignore.case = TRUE)) > 0
      expect_true(ok, info = paste("title mismatch row", i, "rdf vs bib"))
    }
    if (!is.na(t_ris) && nzchar(t_ris)) {
      ok2 <- identical(t_r, t_ris) || grepl(t_r, t_ris, fixed = TRUE) || grepl(t_ris, t_r, fixed = TRUE) || length(agrep(t_r, t_ris, max.distance = 0.2, ignore.case = TRUE)) > 0
      expect_true(ok2, info = paste("title mismatch row", i, "rdf vs ris"))
    }

    # compare doi if present in any
    d0 <- norm_doi(row_rdf$doi)
    d1 <- norm_doi(dbib$doi[j_bib])
    d2 <- norm_doi(dris$doi[j_ris])
    if (!is.na(d0) || !is.na(d1) || !is.na(d2)) {
      vals <- unique(na.omit(c(d0, d1, d2)))
      expect_true(length(vals) == 1, info = paste("doi mismatch row", i))
    }

    # compare journal when available
    j0 <- norm_journal(row_rdf$journal)
    j1 <- norm_journal(dbib$journal[j_bib])
    j2 <- norm_journal(dris$journal[j_ris])
    valsj <- unique(na.omit(c(j0, j1, j2)))
    if (length(valsj) > 0) {
      expect_true(length(valsj) == 1, info = paste("journal mismatch row", i))
    }

    # compare year if present
    y0 <- as.character(row_rdf$year); y1 <- as.character(dbib$year[j_bib]); y2 <- as.character(dris$year[j_ris])
    yvals <- unique(na.omit(c(y0,y1,y2)))
    if (length(yvals) > 0) expect_true(length(yvals) == 1, info = paste("year mismatch row", i))

    # compare authors by last-name sets
  # authors are normalized by the reader to 'Family, Given' separated by '; '
  a0 <- extract_last_names(as.character(row_rdf$author))
  a1 <- extract_last_names(as.character(dbib$author[j_bib]))
  a2 <- extract_last_names(as.character(dris$author[j_ris]))
    aset <- unique(tolower(c(a0,a1,a2)))
    if (length(aset) > 0 && length(setdiff(aset, "")) > 0) {
      expect_true(setequal(tolower(a0), tolower(a1)), info = paste("author mismatch rdf vs bib row", i))
      expect_true(setequal(tolower(a0), tolower(a2)), info = paste("author mismatch rdf vs ris row", i))
    }
  }
})
