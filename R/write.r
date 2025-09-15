##' Simple writer: write BibTeX from a data.frame of Zotero-like entries
##'
#' @param entry A data.frame where each row is a Zotero-like record (columns
#'   such as title, creators/authors, year, journal, publisher, doi, url,
#'   note/abstract, bibtype, key). Only a data.frame is supported by this
#'   simplified helper.
#' @param fout Output filename (e.g. "file.bib").
#' @return Invisibly TRUE on success.
#' @importFrom stats na.omit
#' @export
write_bib <- function(entry, fout) {
  if (!is.data.frame(entry)) stop("'entry' must be a data.frame")
  esc <- function(x) {
    x <- as.character(x)
    # minimal cleanup: collapse newlines and trim
    x <- gsub("[\r\n]+", " ", x)
    trimws(x)
  }

  xml_escape <- function(x) {
    x <- as.character(x)
    x <- gsub("&", "&amp;", x, fixed = TRUE)
    x <- gsub("<", "&lt;", x, fixed = TRUE)
    x <- gsub(">", "&gt;", x, fixed = TRUE)
    x <- gsub('"', "&quot;", x, fixed = TRUE)
    x <- gsub("'", "&apos;", x, fixed = TRUE)
    x
  }

  # helper to split author strings into vector of names
  split_authors <- function(a) {
    if (is.null(a) || is.na(a) || !nzchar(as.character(a))) {
      return(character())
    }
    s <- as.character(a)
    # prefer semicolon separation, fallback to ' and '
    parts <- if (grepl(";", s)) unlist(strsplit(s, ";")) else unlist(strsplit(s, " and ", fixed = TRUE))
    parts <- trimws(parts)
    parts[parts != ""]
  }

  write_ris <- function(df, fout) {
    con <- file(fout, "w", encoding = "UTF-8")
    on.exit(close(con))
    for (i in seq_len(nrow(df))) {
      row <- df[i, , drop = FALSE]
      # TY - JOUR as generic journal article
      writeLines("TY  - JOUR", con)
      # authors
      if ("author" %in% names(row) && !is.na(row$author)) {
        auths <- split_authors(row$author)
        for (a in auths) writeLines(paste0("AU  - ", esc(a)), con)
      }
      if ("title" %in% names(row) && !is.na(row$title)) writeLines(paste0("TI  - ", esc(row$title)), con)
      if ("journal" %in% names(row) && !is.na(row$journal)) writeLines(paste0("JO  - ", esc(row$journal)), con)
      if ("year" %in% names(row) && !is.na(row$year)) writeLines(paste0("PY  - ", esc(row$year)), con)
      if ("doi" %in% names(row) && !is.na(row$doi)) writeLines(paste0("DO  - ", esc(row$doi)), con)
      if ("url" %in% names(row) && !is.na(row$url)) writeLines(paste0("UR  - ", esc(row$url)), con)
      if ("abstract" %in% names(row) && !is.na(row$abstract)) writeLines(paste0("AB  - ", esc(row$abstract)), con)
      writeLines("ER  - ", con)
      writeLines("", con)
    }
    invisible(TRUE)
  }

  write_rdf <- function(df, fout) {
    con <- file(fout, "w", encoding = "UTF-8")
    on.exit(close(con))
    cat('<?xml version="1.0" encoding="UTF-8"?>\n', file = con)
    cat('<rdf:RDF\n xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n xmlns:z="http://www.zotero.org/namespaces/export#"\n xmlns:dcterms="http://purl.org/dc/terms/"\n xmlns:bib="http://purl.org/net/biblio#"\n xmlns:foaf="http://xmlns.com/foaf/0.1/"\n xmlns:link="http://purl.org/rss/1.0/modules/link/"\n xmlns:dc="http://purl.org/dc/elements/1.1/">\n', file = con)
    for (i in seq_len(nrow(df))) {
      row <- df[i, , drop = FALSE]
      about <- if (!is.na(row$url) && nzchar(as.character(row$url))) xml_escape(row$url) else paste0("urn:entry:", i)
      cat(sprintf('    <bib:Article rdf:about="%s">\n', about), file = con)
      # itemType
      if ("bibtype" %in% names(row) && !is.na(row$bibtype)) cat(sprintf("        <z:itemType>%s</z:itemType>\n", xml_escape(as.character(row$bibtype))), file = con)
      # authors
      auths <- character()
      if ("author" %in% names(row) && !is.na(row$author)) auths <- split_authors(row$author)
      if (length(auths) > 0) {
        cat("        <bib:authors>\n            <rdf:Seq>\n", file = con)
        for (a in auths) {
          # parse name into given and family if possible
          a2 <- a
          family <- ""
          given <- ""
          if (grepl(",", a2)) {
            parts <- strsplit(a2, ",", fixed = TRUE)[[1]]
            family <- trimws(parts[1])
            given <- trimws(paste(parts[-1], collapse = ","))
          } else {
            toks <- unlist(strsplit(a2, "\\s+"))
            if (length(toks) == 1) {
              family <- toks[1]
            } else {
              family <- toks[length(toks)]
              given <- paste(toks[-length(toks)], collapse = " ")
            }
          }
          cat("                <rdf:li>\n                    <foaf:Person>\n", file = con)
          if (nzchar(family)) cat(sprintf("                        <foaf:surname>%s</foaf:surname>\n", xml_escape(family)), file = con)
          if (nzchar(given)) cat(sprintf("                        <foaf:givenName>%s</foaf:givenName>\n", xml_escape(given)), file = con)
          cat("                    </foaf:Person>\n                </rdf:li>\n", file = con)
        }
        cat("            </rdf:Seq>\n        </bib:authors>\n", file = con)
      }
      if ("title" %in% names(row) && !is.na(row$title)) cat(sprintf("        <dc:title>%s</dc:title>\n", xml_escape(as.character(row$title))), file = con)
      if ("abstract" %in% names(row) && !is.na(row$abstract)) cat(sprintf("        <dcterms:abstract>%s</dcterms:abstract>\n", xml_escape(as.character(row$abstract))), file = con)
      if ("year" %in% names(row) && !is.na(row$year)) cat(sprintf("        <dc:date>%s</dc:date>\n", xml_escape(as.character(row$year))), file = con)
      if ("url" %in% names(row) && !is.na(row$url)) {
        cat("        <dc:identifier>\n            <dcterms:URI>\n                <rdf:value>", file = con)
        cat(xml_escape(as.character(row$url)), file = con)
        cat("</rdf:value>\n            </dcterms:URI>\n        </dc:identifier>\n", file = con)
      }
      if ("pages" %in% names(row) && !is.na(row$pages)) cat(sprintf("        <bib:pages>%s</bib:pages>\n", xml_escape(as.character(row$pages))), file = con)
      cat("    </bib:Article>\n", file = con)
    }
    cat("</rdf:RDF>\n", file = con)
    invisible(TRUE)
  }

  # find a column name in entry ignoring case
  find_col <- function(df, candidates) {
    cols <- names(df)
    lc <- tolower(cols)
    for (c in candidates) {
      m <- which(lc == tolower(c))
      if (length(m)) {
        return(cols[m[1]])
      }
    }
    NULL
  }

  key_col <- find_col(entry, c("key", "citationkey", "id"))
  type_col <- find_col(entry, c("bibtype", "itemtype", "type"))
  auth_col <- find_col(entry, c("creators", "authors", "author", "creator"))
  title_col <- find_col(entry, c("title", "name"))
  year_col <- find_col(entry, c("year", "date"))
  journal_col <- find_col(entry, c("journal", "journaltitle", "booktitle", "publication"))
  publisher_col <- find_col(entry, c("publisher"))
  doi_col <- find_col(entry, c("doi"))
  url_col <- find_col(entry, c("url", "link"))
  abstract_col <- find_col(entry, c("abstract", "note", "notes"))

  # dispatch by extension
  # Always write BibTeX (.bib content) regardless of file extension.
  # Map common Zotero item types to valid BibTeX entry types.
  map_bibtype_to_bibtex <- function(t) {
    if (is.null(t) || is.na(t) || !nzchar(as.character(t))) {
      return("misc")
    }
    t <- tolower(as.character(t))
    t <- gsub("[^a-z0-9]", "", t)
    if (t %in% c("journalarticle", "journal", "article", "articlejournal")) {
      return("article")
    }
    if (t %in% c("book")) {
      return("book")
    }
    if (t %in% c("incollection", "bookchapter", "chapter")) {
      return("incollection")
    }
    if (t %in% c("conference", "conferencepaper", "inproceedings")) {
      return("inproceedings")
    }
    if (t %in% c("phdthesis", "phd", "thesis", "dissertation")) {
      return("phdthesis")
    }
    if (t %in% c("mastersthesis", "masters", "mastersthesis")) {
      return("mastersthesis")
    }
    if (t %in% c("techreport", "report")) {
      return("techreport")
    }
    if (t %in% c("manual", "software", "computer")) {
      return("manual")
    }
    if (t %in% c("misc", "other")) {
      return("misc")
    }
    # fallback
    "misc"
  }

  # create a deterministic key: family_shorttitle_year
  canonical_key <- function(row, i) {
    # prefer DOI if available (cleaned), else author_shorttitle_year
    get <- function(col) {
      if (is.null(col) || !col %in% names(row)) {
        return(NA_character_)
      }
      v <- as.character(row[[col]])
      if (is.na(v) || !nzchar(v)) {
        return(NA_character_)
      }
      v
    }
    doi <- get(doi_col)
    # attempt to extract DOI from url or abstract if not present
    if (is.na(doi)) {
      try_extract <- function(txt) {
        if (is.na(txt) || !nzchar(txt)) {
          return(NA_character_)
        }
        m <- regexpr("10\\.\\d{4,9}/[^\\s\\)\\}\\,]+", txt, perl = TRUE)
        if (m[1] == -1) {
          return(NA_character_)
        }
        regmatches(txt, m)
      }
      if (!is.na(get(url_col))) doi <- try_extract(get(url_col))
      if (is.na(doi) && !is.na(get(abstract_col))) doi <- try_extract(get(abstract_col))
    }

    # if the input had an original key with a trailing year, use it when year missing
    orig_key <- NA_character_
    if (!is.null(key_col) && key_col %in% names(row)) {
      v <- as.character(row[[key_col]])
      if (!is.na(v) && nzchar(v)) orig_key <- v
    }

    # author family name
    auth <- get(auth_col)
    fam <- NA_character_
    if (!is.na(auth)) {
      parts <- if (grepl(";", auth)) unlist(strsplit(auth, ";")) else unlist(strsplit(auth, " and ", fixed = TRUE))
      if (length(parts) > 0) {
        a <- trimws(parts[1])
        if (grepl(",", a)) {
          fam <- trimws(strsplit(a, ",", fixed = TRUE)[[1]][1])
          fam <- tolower(iconv(fam, to = "ASCII//TRANSLIT"))
          fam <- gsub("[^a-z0-9]+", "", fam)
        } else {
          toks <- unlist(strsplit(a, "\\s+"))
          fam <- tolower(iconv(toks[length(toks)], to = "ASCII//TRANSLIT"))
          fam <- gsub("[^a-z0-9]+", "", fam)
        }
      }
    }
    title <- get(title_col)
    shortt <- NA_character_
    if (!is.na(title)) {
      t0 <- tolower(iconv(title, to = "ASCII//TRANSLIT"))
      t0 <- gsub("[^a-z0-9 ]+", "", t0)
      toks <- unlist(strsplit(t0, "\\s+"))
      if (length(toks) > 0) shortt <- toks[1]
      if (!is.na(shortt)) shortt <- gsub("[^a-z0-9]+", "", shortt)
    }
    year <- get(year_col)
    if (!is.na(year)) {
      yy <- regmatches(year, regexpr("[0-9]{4}", year))
      if (length(yy)) year <- yy else year <- NA_character_
    } else {
      # try to extract year from original key suffix like _2020
      if (!is.na(orig_key)) {
        m <- regmatches(orig_key, regexpr("(_|-)([0-9]{4})$", orig_key, perl = TRUE))
        if (length(m) && nzchar(m)) {
          yy <- sub(".*(_|-)([0-9]{4})$", "\\2", m)
          if (nzchar(yy)) year <- yy
        }
      }
      if (is.na(year)) year <- NA_character_
    }

    parts <- c(fam, shortt, year)
    parts <- parts[!is.na(parts) & nzchar(parts)]
    if (length(parts) == 0) {
      # fallback: try DOI slug
      if (!is.na(doi)) {
        slug <- sub(".*/", "", doi)
        slug <- gsub("[^A-Za-z0-9._-]+", "", slug)
        slug <- gsub("\\.", "_", slug)
        slug <- tolower(slug)
        toks <- unlist(strsplit(slug, "[_-]"))
        first_alpha <- which(grepl("[a-z]", toks))
        if (length(first_alpha)) {
          toks2 <- toks[first_alpha[1]:length(toks)]
          slug2 <- paste(toks2, collapse = "_")
          if (nzchar(slug2)) {
            return(slug2)
          }
        }
        k <- gsub("[^A-Za-z0-9]+", "", doi)
        return(tolower(k))
      }
      return(paste0("entry", i))
    }
    k <- paste(parts, collapse = "_")
    # clean
    k <- gsub("[^A-Za-z0-9_]+", "", k)
    tolower(k)
  }

  con <- file(fout, "w", encoding = "UTF-8")
  on.exit(close(con))

  for (i in seq_len(nrow(entry))) {
    row <- entry[i, , drop = FALSE]
    # always use deterministic canonical key for stable output across importers
    key <- canonical_key(row, i)
    raw_typ <- if (!is.null(type_col) && nzchar(as.character(row[[type_col]]))) as.character(row[[type_col]]) else NA_character_
    typ <- map_bibtype_to_bibtex(raw_typ)
    # heuristic: if we have journal-like fields but ended up with misc, prefer article
    if (identical(typ, "misc")) {
      has_journal <- !is.null(journal_col) && journal_col %in% names(row) && !is.na(row[[journal_col]]) && nzchar(as.character(row[[journal_col]]))
      has_volume_pages <- ("volume" %in% names(row) && !is.na(row[["volume"]]) && nzchar(as.character(row[["volume"]]))) || ("pages" %in% names(row) && !is.na(row[["pages"]]) && nzchar(as.character(row[["pages"]])))
      if (has_journal || has_volume_pages) typ <- "article"
    }
    writeLines(sprintf("@%s{%s,", typ, key), con)

    # authors are written in the canonical field order below (avoid duplicates)

    write_field <- function(col, name) {
      if (!is.null(col) && !is.na(row[[col]]) && nzchar(as.character(row[[col]]))) {
        writeLines(paste0("  ", name, " = {", esc(row[[col]]), "},"), con)
      }
    }

    # canonical field order
    # canonical fields with synonym candidates; pick the first present non-empty column per row
    canonical_fields <- list(
      author = c(auth_col, "author", "authors", "creator", "creators"),
      title = c(title_col, "title", "name"),
      journal = c(journal_col, "journal", "journaltitle", "publication", "booktitle"),
      year = c(year_col, "year", "date"),
      volume = c("volume"),
      number = c("number"),
      pages = c("pages"),
      doi = c(doi_col, "doi"),
      url = c(url_col, "url", "link"),
      abstract = c(abstract_col, "abstract", "note", "notes"),
      publisher = c(publisher_col, "publisher"),
      issn = c("issn"),
      note = c("note")
    )
    for (fname in names(canonical_fields)) {
      candidates <- canonical_fields[[fname]]
      for (col in candidates) {
        if (!is.null(col) && col %in% names(row) && !is.na(row[[col]]) && nzchar(as.character(row[[col]]))) {
          write_field(col, fname)
          break
        }
      }
    }

    # remaining columns (stable sorted order)
    used <- unique(na.omit(unlist(c(list(key_col, type_col, auth_col, title_col, year_col, journal_col, publisher_col, doi_col, url_col, abstract_col), canonical_fields))))
    # also treat common author synonyms as used to avoid duplicate author-like fields
    author_synonyms <- c("author", "authors", "creator", "creators")
    used_syn_cols <- names(entry)[tolower(names(entry)) %in% author_synonyms]
    used <- unique(c(used, used_syn_cols))
    other_cols <- setdiff(names(entry), used)
    if (length(other_cols)) {
      other_cols <- sort(other_cols)
      for (cname in other_cols) {
        val <- row[[cname]]
        if (!is.na(val) && nzchar(as.character(val))) {
          writeLines(paste0("  ", cname, " = {", esc(val), "},"), con)
        }
      }
    }

    writeLines("}", con)
    writeLines("", con)
  }

  invisible(TRUE)
}
