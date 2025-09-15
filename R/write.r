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
  con <- file(fout, 'w', encoding = 'UTF-8')
  on.exit(close(con))

  esc <- function(x) {
    x <- as.character(x)
    # minimal cleanup: collapse newlines and trim
    x <- gsub('[\r\n]+', ' ', x)
    trimws(x)
  }

  # find a column name in entry ignoring case
  find_col <- function(df, candidates) {
    cols <- names(df)
    lc <- tolower(cols)
    for (c in candidates) {
      m <- which(lc == tolower(c))
      if (length(m)) return(cols[m[1]])
    }
    NULL
  }

  key_col <- find_col(entry, c('key', 'citationkey', 'id'))
  type_col <- find_col(entry, c('bibtype', 'itemtype', 'type'))
  auth_col <- find_col(entry, c('creators', 'authors', 'author', 'creator'))
  title_col <- find_col(entry, c('title', 'name'))
  year_col <- find_col(entry, c('year', 'date'))
  journal_col <- find_col(entry, c('journal', 'journaltitle', 'booktitle', 'publication'))
  publisher_col <- find_col(entry, c('publisher'))
  doi_col <- find_col(entry, c('doi'))
  url_col <- find_col(entry, c('url', 'link'))
  abstract_col <- find_col(entry, c('abstract', 'note', 'notes'))

  for (i in seq_len(nrow(entry))) {
    row <- entry[i, , drop = FALSE]
    key <- if (!is.null(key_col) && nzchar(as.character(row[[key_col]]))) as.character(row[[key_col]]) else paste0('entry', i)
    typ <- if (!is.null(type_col) && nzchar(as.character(row[[type_col]]))) tolower(as.character(row[[type_col]])) else 'misc'

    writeLines(sprintf('@%s{%s,', typ, key), con)

    # authors
    if (!is.null(auth_col) && !is.na(row[[auth_col]]) && nzchar(as.character(row[[auth_col]]))) {
      a <- as.character(row[[auth_col]])
      parts <- if (grepl(';', a)) trimws(unlist(strsplit(a, ';'))) else trimws(unlist(strsplit(a, ' and ')))
      parts <- vapply(parts, function(name) {
        if (grepl(',', name)) return(trimws(name))
        ws <- unlist(strsplit(name, '\\s+'))
        if (length(ws) == 1) return(ws)
        fam <- ws[length(ws)]; giv <- paste(ws[-length(ws)], collapse = ' ')
        paste(fam, ', ', giv, sep = '')
      }, character(1))
      auth_str <- paste(parts[nzchar(parts)], collapse = ' and ')
      if (nzchar(auth_str)) writeLines(paste0('  author = {', esc(auth_str), '},'), con)
    }

    write_field <- function(col, name) {
      if (!is.null(col) && !is.na(row[[col]]) && nzchar(as.character(row[[col]]))) {
        writeLines(paste0('  ', name, ' = {', esc(row[[col]]), '},'), con)
      }
    }

    write_field(title_col, 'title')
    write_field(journal_col, 'journal')
    write_field(year_col, 'year')
    write_field(publisher_col, 'publisher')
    write_field(doi_col, 'doi')
    write_field(url_col, 'url')
    write_field(abstract_col, 'abstract')

    used <- na.omit(c(key_col, type_col, auth_col, title_col, year_col, journal_col, publisher_col, doi_col, url_col, abstract_col))
    other_cols <- setdiff(names(entry), used)
    if (length(other_cols)) {
      for (cname in other_cols) {
        val <- row[[cname]]
        if (!is.na(val) && nzchar(as.character(val))) {
          writeLines(paste0('  ', cname, ' = {', esc(val), '},'), con)
        }
      }
    }

    writeLines('}', con)
    writeLines('', con)
  }

  invisible(TRUE)
}
