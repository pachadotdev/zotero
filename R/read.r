# Helpers ----

## These functions accept either an existing DBI connection (`con`) or a
## `zotero_db` path; when `con` is NULL a safe snapshot connection is created
## via `.open_zotero_snapshot()` and cleaned up automatically.

#' @importFrom DBI dbExecute
#' @importFrom RSQLite SQLite
.open_zotero_snapshot <- function(zotero_db = "~/Zotero/zotero.sqlite",
                                  snapshot = tempfile(fileext = ".sqlite"),
                                  timeout_ms = 5000,
                                  use_backup = TRUE) {
  zotero_db <- path.expand(zotero_db)
  stopifnot(file.exists(zotero_db))

  sqlite3_bin <- Sys.which("sqlite3")
  did_backup <- FALSE

  if (use_backup && nzchar(sqlite3_bin)) {
    # Use sqlite3's .backup (uses SQLite's safe backup API) -- best option while DB is live
    cmd <- sprintf('%s "%s" ".backup %s"', sqlite3_bin, zotero_db, snapshot)
    exit_code <- system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
    if (exit_code == 0 && file.exists(snapshot)) {
      did_backup <- TRUE
    } else {
      warning("sqlite3 .backup failed -- falling back to file copy. Exit code: ", exit_code)
    }
  }

  if (!did_backup) {
    # Fallback: copy main DB and, if present, the WAL/SHM files
    file.copy(zotero_db, snapshot, overwrite = TRUE)
    for (sfx in c("-wal", "-shm")) {
      src <- paste0(zotero_db, sfx)
      dest <- paste0(snapshot, sfx)
      if (file.exists(src)) file.copy(src, dest, overwrite = TRUE)
    }

    if (!file.exists(snapshot)) stop("Could not create snapshot copy of Zotero DB")
  }

  con <- DBI::dbConnect(RSQLite::SQLite(), dbname = snapshot)
  # Set the connection to query-only and a reasonable busy timeout
  tryCatch(
    {
      DBI::dbExecute(con, sprintf("PRAGMA busy_timeout = %d;", as.integer(timeout_ms)))
      DBI::dbExecute(con, "PRAGMA query_only = TRUE;")
    },
    error = function(e) {
      # If PRAGMA query_only is unsupported by the SQLite build, we continue -- snapshot is still read-only for our use.
      warning("Could not set PRAGMA(s) on snapshot: ", conditionMessage(e))
    }
  )

  cleanup <- function() {
    try(DBI::dbDisconnect(con), silent = TRUE)
    try(unlink(snapshot), silent = TRUE)
    try(unlink(paste0(snapshot, "-wal")), silent = TRUE)
    try(unlink(paste0(snapshot, "-shm")), silent = TRUE)
    invisible(TRUE)
  }

  list(con = con, snapshot_path = snapshot, cleanup = cleanup)
}

._build_creator_expr <- function(con) {
  # Inspect creators table to pick name columns
  cols <- DBI::dbGetQuery(con, "PRAGMA table_info(creators)")$name
  cols_lc <- tolower(cols)

  # Prefer first/last name style
  if (any(cols_lc %in% c("firstname", "first_name", "givenname", "given_name")) &&
    any(cols_lc %in% c("lastname", "last_name", "familyname", "family_name"))) {
    first <- cols[which(cols_lc %in% c("firstname", "first_name", "givenname", "given_name"))[1]]
    last <- cols[which(cols_lc %in% c("lastname", "last_name", "familyname", "family_name"))[1]]
    expr <- sprintf("trim(%s || ' ' || %s)", DBI::dbQuoteIdentifier(con, first), DBI::dbQuoteIdentifier(con, last))
    return(expr)
  }

  # Fallback to single name-like column
  if (any(cols_lc %in% c("name", "creatorname", "creator"))) {
    nm <- cols[which(cols_lc %in% c("name", "creatorname", "creator"))[1]]
    return(DBI::dbQuoteIdentifier(con, nm))
  }

  # Last resort: concatenate all text columns
  text_cols <- cols[cols_lc %in% cols_lc] # keep order
  if (length(text_cols) > 0) {
    quoted <- vapply(text_cols, function(x) DBI::dbQuoteIdentifier(con, x), character(1))
    expr <- paste(quoted, collapse = " || ' ' || ")
    return(sprintf("trim(%s)", expr))
  }

  # If nothing found, return an empty string
  ""
}

._get_title_fieldid <- function(con) {
  # fields table maps fieldID -> fieldName; find the title field
  if (!DBI::dbExistsTable(con, "fields")) {
    return(NA_integer_)
  }
  f <- DBI::dbGetQuery(con, "SELECT fieldID, fieldName FROM fields")
  if (nrow(f) == 0) {
    return(NA_integer_)
  }
  ix <- which(tolower(f$fieldName) == "title")
  if (length(ix) == 0) {
    ix <- grep("title", tolower(f$fieldName), fixed = TRUE)
  }
  if (length(ix) == 0) {
    return(NA_integer_)
  }
  f$fieldID[ix[1]]
}

.zotero_query_items <- function(con, where_clause = "1=1", params = list()) {
  title_fid <- ._get_title_fieldid(con)

  sql <- sprintf(
    "SELECT it.itemID,
            idv_title.value AS title,
            group_concat(DISTINCT cr.creatorID) AS creator_ids,
       group_concat(DISTINCT col.collectionName) AS collections,
            group_concat(DISTINCT t.name) AS keywords,
            it.itemTypeID,
            it.dateAdded,
            it.dateModified,
            it.key,
            group_concat(DISTINCT inote.note) AS notes,
            (SELECT group_concat(f.fieldName || '=' || idv2.value, '||')
               FROM itemData idt2
               JOIN itemDataValues idv2 ON idt2.valueID = idv2.valueID
               JOIN fields f ON idt2.fieldID = f.fieldID
              WHERE idt2.itemID = it.itemID) AS metadata
     FROM items it
     LEFT JOIN itemData idt_title ON it.itemID = idt_title.itemID AND idt_title.fieldID = %s
     LEFT JOIN itemDataValues idv_title ON idt_title.valueID = idv_title.valueID
     LEFT JOIN itemCreators ic ON it.itemID = ic.itemID
     LEFT JOIN creators cr ON ic.creatorID = cr.creatorID
     LEFT JOIN collectionItems colit ON it.itemID = colit.itemID
     LEFT JOIN collections col ON colit.collectionID = col.collectionID
     LEFT JOIN itemTags itag ON it.itemID = itag.itemID
     LEFT JOIN tags t ON itag.tagID = t.tagID
  LEFT JOIN itemNotes inote ON it.itemID = inote.itemID
    WHERE %s
    GROUP BY it.itemID
    ORDER BY idv_title.value",
    ifelse(is.na(title_fid), "NULL", as.character(title_fid)),
    where_clause
  )

  res <- DBI::dbGetQuery(con, sql, params = params)
  if (nrow(res) == 0) {
    return(res)
  }

  # Resolve creator_ids into human-readable names
  .resolve_creator_ids <- function(con, id_csv) {
    if (is.na(id_csv) || id_csv == "") {
      return(NA_character_)
    }
    ids <- unique(trimws(unlist(strsplit(id_csv, ","))))
    ids_num <- suppressWarnings(as.integer(ids))
    if (all(is.na(ids_num))) {
      return(NA_character_)
    }
    ids_num <- ids_num[!is.na(ids_num)]

    q <- sprintf("SELECT * FROM creators WHERE creatorID IN (%s)", paste(ids_num, collapse = ","))
    crs <- DBI::dbGetQuery(con, q)
    if (nrow(crs) == 0) {
      return(NA_character_)
    }

    build_name <- function(id) {
      row <- crs[crs$creatorID == id, , drop = FALSE]
      if (nrow(row) == 0) {
        return(NA_character_)
      }
      rn <- tolower(names(row))
      given_col <- names(row)[which(rn %in% c("givenname", "given_name", "firstname", "first_name", "given"))[1]]
      family_col <- names(row)[which(rn %in% c("surname", "familyname", "family_name", "lastname", "last_name", "last"))[1]]
      name_col <- names(row)[which(rn %in% c("name", "creatorname", "creator"))[1]]

      if (!is.na(given_col) && given_col %in% names(row) && nzchar(as.character(row[[given_col]]))) {
        if (!is.na(family_col) && family_col %in% names(row) && nzchar(as.character(row[[family_col]]))) {
          paste(trimws(as.character(row[[given_col]])), trimws(as.character(row[[family_col]])))
        } else {
          trimws(as.character(row[[given_col]]))
        }
      } else if (!is.na(name_col) && name_col %in% names(row) && nzchar(as.character(row[[name_col]]))) {
        trimws(as.character(row[[name_col]]))
      } else {
        # fallback: concatenate any character columns
        txtcols <- row[, sapply(row, is.character), drop = FALSE]
        vals <- unlist(txtcols[1, , drop = FALSE])
        vals <- vals[!is.na(vals) & nzchar(vals)]
        if (length(vals) == 0) NA_character_ else paste(vals, collapse = " ")
      }
    }

    # preserve original id order
    names_vec <- vapply(ids_num, build_name, character(1))
    names_vec <- names_vec[!is.na(names_vec) & nzchar(names_vec)]
    if (length(names_vec) == 0) {
      return(NA_character_)
    }
    paste(names_vec, collapse = ", ")
  }

  # populate creators column
  res$creators <- vapply(res$creator_ids, function(x) .resolve_creator_ids(con, x), character(1))

  # parse metadata column (format: fieldName=value||fieldName2=value2)
  parse_meta <- function(mstr) {
    if (is.na(mstr) || mstr == "") {
      return(list())
    }
    parts <- strsplit(mstr, "\\|\\|", perl = TRUE)[[1]]
    kv <- list()
    for (p in parts) {
      eq <- regexpr("=", p, fixed = TRUE)
      if (eq[1] > 0) {
        k <- tolower(trimws(substr(p, 1, eq[1] - 1)))
        v <- trimws(substr(p, eq[1] + 1, nchar(p)))
        kv[[k]] <- v
      }
    }
    kv
  }

  # ensure metadata target columns exist
  meta_cols <- c("volume", "number", "pages", "issn", "shorttitle", "language", "url", "doi", "abstract", "publisher", "file", "note", "urldate")
  for (mc in meta_cols) if (!mc %in% names(res)) res[[mc]] <- NA_character_

  for (i in seq_len(nrow(res))) {
    kv <- parse_meta(res$metadata[i])
    if (length(kv) == 0) next
    for (k in names(kv)) {
      k2 <- tolower(k)
      kcol <- switch(k2,
        volume = "volume",
        vol = "volume",
        number = "number",
        pages = "pages",
        page = "pages",
        issn = "issn",
        shorttitle = "shorttitle",
        language = "language",
        lang = "language",
        url = "url",
        uri = "url",
        link = "url",
        doi = "doi",
        abstract = "abstract",
        abstractnote = "abstract",
        abstract_note = "abstract",
        publisher = "publisher",
        file = "file",
        files = "file",
        note = "note",
        notes = "note",
        urldate = "urldate",
        url_date = "urldate",
        NULL
      )
      if (!is.null(kcol) && kcol %in% names(res)) res[i, kcol] <- kv[[k]]
    }
  }

  res$creator_ids <- NULL

  res
}


# Try reading BibTeX files with a simple, robust parser that handles nested braces and quoted fields
#' @importFrom stats setNames
.parse_bib_to_df <- function(path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  if (length(lines) == 0) {
    return(data.frame())
  }

  # join lines but preserve newlines for safer parsing
  txt <- paste(lines, collapse = "\n")
  # find entry starts
  starts <- gregexpr("\\n?\\s*@", paste0("\n", txt), perl = TRUE)[[1]]
  if (length(starts) == 0 || starts[1] == -1) {
    return(data.frame())
  }

  entries <- list()
  pos <- 1L
  n <- nchar(txt)

  while (pos <= n) {
    # find next @
    at <- regexpr("\\@", substring(txt, pos), perl = TRUE)[1]
    if (at == -1) break
    at_abs <- pos + at - 1L
    # read entry type and key
    header_re <- regexec("\\@\\s*([A-Za-z0-9_:-]+)\\s*\\{\\s*([^,\\s]+)\\s*,", substring(txt, at_abs), perl = TRUE)
    hm <- regmatches(substring(txt, at_abs), header_re)[[1]]
    if (length(hm) == 0) break
    etype <- tolower(hm[2])
    ekey <- hm[3]
    # locate the opening brace position (relative to at_abs)
    open_rel <- regexpr("\\{", substring(txt, at_abs))[1]
    if (open_rel == -1) break
    start_body <- at_abs + open_rel

    # scan forward to find the matching closing brace for the entry body
    depth <- 0L
    j <- start_body
    body <- NULL
    while (j <= n) {
      ch <- substring(txt, j, j)
      if (ch == "{") {
        depth <- depth + 1L
      } else if (ch == "}") {
        depth <- depth - 1L
        if (depth == 0L) {
          body <- substring(txt, start_body + 1L, j - 1L)
          pos <- j + 1L
          break
        }
      }
      j <- j + 1L
    }
    if (is.null(body)) break

    # parse fields from body: field = {value} OR field = "value" OR field = barevalue
    fields <- list(bibtype = etype, key = ekey)
    i <- 1L
    L <- nchar(body)
    while (i <= L) {
      # skip whitespace and commas
      mws <- regexpr("^[\\s,]+", substring(body, i), perl = TRUE)
      if (mws[1] == 1) i <- i + attr(mws, "match.length")
      if (i > L) break

      # field name
      fn_re <- regexec("^([A-Za-z0-9_:-]+)\\s*=\\s*", substring(body, i), perl = TRUE)
      fm <- regmatches(substring(body, i), fn_re)[[1]]
      if (length(fm) == 0) break
      fname <- tolower(fm[2])
      i <- i + nchar(fm[1])
      if (i > L) break

      ch <- substring(body, i, i)
      val <- ""
      if (ch == "{") {
        # balanced braces
        depth2 <- 0L
        j2 <- i
        while (j2 <= L) {
          cch <- substring(body, j2, j2)
          if (cch == "{") {
            depth2 <- depth2 + 1L
          } else if (cch == "}") {
            depth2 <- depth2 - 1L
            if (depth2 == 0L) {
              val <- substring(body, i + 1L, j2 - 1L)
              i <- j2 + 1L
              break
            }
          }
          j2 <- j2 + 1L
        }
        if (val == "") {
          val <- substring(body, i + 1L)
          i <- L + 1L
        }
      } else if (ch == '"') {
        # quoted string
        j2 <- i + 1L
        while (j2 <= L) {
          cch <- substring(body, j2, j2)
          if (cch == '"') {
            val <- substring(body, i + 1L, j2 - 1L)
            i <- j2 + 1L
            break
          }
          if (cch == "\\") j2 <- j2 + 2L else j2 <- j2 + 1L
        }
        if (val == "") {
          val <- substring(body, i + 1L)
          i <- L + 1L
        }
      } else {
        # bare until comma or EOL
        rest <- substring(body, i)
        m2 <- regexpr("[,\\n]", rest, perl = TRUE)
        if (m2[1] == -1) {
          val <- trimws(rest)
          i <- L + 1L
        } else {
          val <- substring(body, i, i + m2[1] - 2L)
          i <- i + m2[1]
        }
      }

      fields[[fname]] <- trimws(val)
    }

    entries[[length(entries) + 1L]] <- fields
  }

  if (length(entries) == 0) {
    return(data.frame())
  }
  all_keys <- unique(unlist(lapply(entries, names)))
  df <- do.call(rbind, lapply(entries, function(x) {
    row <- setNames(rep(NA_character_, length(all_keys)), all_keys)
    row[names(x)] <- vapply(x, as.character, character(1))
    as.data.frame(as.list(row), stringsAsFactors = FALSE)
  }))
  names(df) <- tolower(names(df))
  # Normalize columns that were created with dotted prefixes (e.g. title.title) by coalescing
  base_names <- tolower(gsub(".*\\.", "", names(df)))
  uniq_base <- unique(base_names)
  collapsed <- setNames(vector("list", length(uniq_base)), uniq_base)
  for (b in uniq_base) {
    idx <- which(base_names == b)
    if (length(idx) == 1) {
      collapsed[[b]] <- as.character(df[[idx]])
    } else {
      mat <- as.data.frame(df[, idx, drop = FALSE], stringsAsFactors = FALSE)
      # coalesce per-row: pick first non-empty, non-NA value
      collapsed[[b]] <- apply(mat, 1, function(r) {
        r <- as.character(r)
        r <- r[!is.na(r) & nzchar(trimws(r))]
        if (length(r) == 0) NA_character_ else r[1]
      })
    }
  }
  df <- as.data.frame(collapsed, stringsAsFactors = FALSE)

  # If title column is missing or entirely NA, do a focused, brace-aware pass to extract title fields
  if (!"title" %in% names(df) || all(is.na(df$title))) {
    txt <- paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
    # split into entry chunks by finding leading @ markers
    starts <- gregexpr("\n?\\s*@", paste0("\n", txt), perl = TRUE)[[1]]
    chunks <- list()
    if (!(length(starts) == 1 && starts[1] == -1)) {
      starts_abs <- starts
      starts_abs <- starts_abs + 0L
      # compute end positions
      ends_abs <- c(starts_abs[-1] - 1, nchar(txt) + 1)
      for (k in seq_along(starts_abs)) {
        st <- starts_abs[k]
        en <- ends_abs[k]
        chunk <- substring(txt, st, en)
        chunks[[k]] <- chunk
      }
    }

    extract_title_from_chunk <- function(chunk) {
      m <- regexec("title\\s*=\\s*", chunk, perl = TRUE, ignore.case = TRUE)
      mm <- regmatches(chunk, m)[[1]]
      if (length(mm) == 0) {
        return(NA_character_)
      }
      # position after the match
      pos <- attr(m[[1]], "match.length")
      if (is.na(pos)) {
        return(NA_character_)
      }
      start_pos <- as.integer(m[[1]]) + pos
      if (is.na(start_pos) || start_pos > nchar(chunk)) {
        return(NA_character_)
      }
      # skip whitespace
      s <- substring(chunk, start_pos)
      s_pos <- regexpr("\\S", s, perl = TRUE)[1]
      if (s_pos == -1) {
        return(NA_character_)
      }
      s_abs <- start_pos + s_pos - 1L
      ch <- substring(chunk, s_abs, s_abs)
      if (ch == "{") {
        depth <- 0L
        j <- s_abs
        while (j <= nchar(chunk)) {
          cch <- substring(chunk, j, j)
          if (cch == "{") {
            depth <- depth + 1L
          } else if (cch == "}") {
            depth <- depth - 1L
            if (depth == 0L) {
              return(trimws(substring(chunk, s_abs + 1L, j - 1L)))
            }
          }
          j <- j + 1L
        }
        return(NA_character_)
      } else if (ch == '"') {
        j <- s_abs + 1L
        while (j <= nchar(chunk)) {
          cch <- substring(chunk, j, j)
          if (cch == '"') {
            return(trimws(substring(chunk, s_abs + 1L, j - 1L)))
          }
          if (cch == "\\") j <- j + 2L else j <- j + 1L
        }
        return(NA_character_)
      } else {
        # bare until comma or newline
        rest <- substring(chunk, s_abs)
        m2 <- regexpr("[,\\n]", rest, perl = TRUE)
        if (m2[1] == -1) {
          return(trimws(rest))
        }
        return(trimws(substring(chunk, s_abs, s_abs + m2[1] - 2L)))
      }
    }

    titles <- vapply(chunks, extract_title_from_chunk, character(1))
    # assign to df rows in order if lengths match; otherwise fill available titles
    titles <- titles[!is.na(titles) & nzchar(titles)]
    if (length(titles) > 0) {
      if (!"title" %in% names(df)) df$title <- NA_character_
      # fill sequentially up to min rows
      nfill <- min(nrow(df), length(titles))
      df$title[seq_len(nfill)] <- titles[seq_len(nfill)]
    }
  }

  # If author column is missing or entirely NA, do a focused pass to extract author fields
  if (!"author" %in% names(df) || all(is.na(df$author))) {
    txt <- paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
    starts <- gregexpr("\n?\\s*@", paste0("\n", txt), perl = TRUE)[[1]]
    chunks <- list()
    if (!(length(starts) == 1 && starts[1] == -1)) {
      starts_abs <- starts
      ends_abs <- c(starts_abs[-1] - 1, nchar(txt) + 1)
      for (k in seq_along(starts_abs)) {
        st <- starts_abs[k]
        en <- ends_abs[k]
        chunks[[k]] <- substring(txt, st, en)
      }
    }

    extract_author_from_chunk <- function(chunk) {
      m <- regexec("author\\s*=\\s*", chunk, perl = TRUE, ignore.case = TRUE)
      mm <- regmatches(chunk, m)[[1]]
      if (length(mm) == 0) {
        return(NA_character_)
      }
      pos <- attr(m[[1]], "match.length")
      if (is.na(pos)) {
        return(NA_character_)
      }
      start_pos <- as.integer(m[[1]]) + pos
      if (is.na(start_pos) || start_pos > nchar(chunk)) {
        return(NA_character_)
      }
      s <- substring(chunk, start_pos)
      s_pos <- regexpr("\\S", s, perl = TRUE)[1]
      if (s_pos == -1) {
        return(NA_character_)
      }
      s_abs <- start_pos + s_pos - 1L
      ch <- substring(chunk, s_abs, s_abs)
      if (ch == "{") {
        depth <- 0L
        j <- s_abs
        while (j <= nchar(chunk)) {
          cch <- substring(chunk, j, j)
          if (cch == "{") {
            depth <- depth + 1L
          } else if (cch == "}") {
            depth <- depth - 1L
            if (depth == 0L) {
              return(trimws(substring(chunk, s_abs + 1L, j - 1L)))
            }
          }
          j <- j + 1L
        }
        return(NA_character_)
      } else if (ch == '"') {
        j <- s_abs + 1L
        while (j <= nchar(chunk)) {
          cch <- substring(chunk, j, j)
          if (cch == '"') {
            return(trimws(substring(chunk, s_abs + 1L, j - 1L)))
          }
          if (cch == "\\") j <- j + 2L else j <- j + 1L
        }
        return(NA_character_)
      } else {
        # bare until comma or newline -- but for authors we want the whole value (may contain ' and ')
        rest <- substring(chunk, s_abs)
        # find end at a comma followed by newline or the end of entry
        m2 <- regexpr("\\n", rest, perl = TRUE)
        if (m2[1] == -1) {
          return(trimws(rest))
        }
        return(trimws(substring(chunk, s_abs, s_abs + m2[1] - 2L)))
      }
    }

    authors_raw <- vapply(chunks, extract_author_from_chunk, character(1))
    authors_raw <- authors_raw[!is.na(authors_raw) & nzchar(authors_raw)]
    if (length(authors_raw) > 0) {
      if (!"author" %in% names(df)) df$author <- NA_character_
      nfill <- min(nrow(df), length(authors_raw))
      authors_norm <- vapply(authors_raw, function(s) {
        s <- gsub("\\s*\\band\\b\\s*", "; ", s, ignore.case = TRUE, perl = TRUE)
        s <- gsub("\\s+", " ", s)
        trimws(s)
      }, character(1))
      df$author[seq_len(nfill)] <- authors_norm[seq_len(nfill)]
    }
  }

  # If doi or url columns are missing or empty, try to extract them from the raw entry chunks
  if (!"doi" %in% names(df) || !"url" %in% names(df) || all(is.na(df$doi)) || all(is.na(df$url))) {
    txt2 <- paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
    starts2 <- gregexpr("\n?\\s*@", paste0("\n", txt2), perl = TRUE)[[1]]
    chunks2 <- list()
    if (!(length(starts2) == 1 && starts2[1] == -1)) {
      starts_abs2 <- starts2
      ends_abs2 <- c(starts_abs2[-1] - 1, nchar(txt2) + 1)
      for (k in seq_along(starts_abs2)) {
        st <- starts_abs2[k]
        en <- ends_abs2[k]
        chunks2[[k]] <- substring(txt2, st, en)
      }
    }

    extract_field_from_chunk <- function(chunk, field) {
      pat <- paste0(field, "\\s*=\\s*")
      m <- regexec(pat, chunk, perl = TRUE, ignore.case = TRUE)
      mm <- regmatches(chunk, m)[[1]]
      if (length(mm) == 0) {
        return(NA_character_)
      }
      pos <- attr(m[[1]], "match.length")
      if (is.na(pos)) {
        return(NA_character_)
      }
      start_pos <- as.integer(m[[1]]) + pos
      if (is.na(start_pos) || start_pos > nchar(chunk)) {
        return(NA_character_)
      }
      s <- substring(chunk, start_pos)
      s_pos <- regexpr("\\S", s, perl = TRUE)[1]
      if (s_pos == -1) {
        return(NA_character_)
      }
      s_abs <- start_pos + s_pos - 1L
      ch <- substring(chunk, s_abs, s_abs)
      if (ch == "{") {
        depth <- 0L
        j <- s_abs
        while (j <= nchar(chunk)) {
          cch <- substring(chunk, j, j)
          if (cch == "{") {
            depth <- depth + 1L
          } else if (cch == "}") {
            depth <- depth - 1L
            if (depth == 0L) {
              return(trimws(substring(chunk, s_abs + 1L, j - 1L)))
            }
          }
          j <- j + 1L
        }
        return(NA_character_)
      } else if (ch == '"') {
        j <- s_abs + 1L
        while (j <= nchar(chunk)) {
          cch <- substring(chunk, j, j)
          if (cch == '"') {
            return(trimws(substring(chunk, s_abs + 1L, j - 1L)))
          }
          if (cch == "\\") j <- j + 2L else j <- j + 1L
        }
        return(NA_character_)
      } else {
        rest <- substring(chunk, s_abs)
        m2 <- regexpr("[,\\n]", rest, perl = TRUE)
        if (m2[1] == -1) {
          return(trimws(rest))
        }
        return(trimws(substring(chunk, s_abs, s_abs + m2[1] - 2L)))
      }
    }

    # try to fill doi and url from chunks
    if (!"doi" %in% names(df)) df$doi <- NA_character_
    if (!"url" %in% names(df)) df$url <- NA_character_
    for (k in seq_len(min(nrow(df), length(chunks2)))) {
      if (is.na(df$doi[k]) || !nzchar(as.character(df$doi[k]))) {
        v <- extract_field_from_chunk(chunks2[[k]], "doi")
        if (!is.na(v) && nzchar(v)) df$doi[k] <- v
      }
      if (is.na(df$url[k]) || !nzchar(as.character(df$url[k]))) {
        v2 <- extract_field_from_chunk(chunks2[[k]], "url")
        if (!is.na(v2) && nzchar(v2)) df$url[k] <- v2
      }
    }
  }

  df
}

.parse_ris_to_df <- function(path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  records <- list()
  cur <- list()
  add_cur <- function() {
    if (length(cur) == 0) {
      return()
    }
    # collapse multiple AU into author
    if (!is.null(cur$AU)) cur$author <- paste(cur$AU, collapse = "; ")
    rec <- list(
      bibtype = if (!is.null(cur$TY)) cur$TY else NA_character_,
      key = if (!is.null(cur$ID)) cur$ID else NA_character_,
      title = if (!is.null(cur$TI)) cur$TI else if (!is.null(cur$T1)) cur$T1 else NA_character_,
      volume = if (!is.null(cur$VL)) cur$VL else NA_character_,
      issn = if (!is.null(cur$SN)) cur$SN else NA_character_,
      shorttitle = if (!is.null(cur$ST)) cur$ST else NA_character_,
      url = if (!is.null(cur$UR)) cur$UR else NA_character_,
      doi = if (!is.null(cur$DO)) cur$DO else NA_character_,
      abstract = if (!is.null(cur$AB)) cur$AB else NA_character_,
      language = if (!is.null(cur$LA)) cur$LA else NA_character_,
      number = if (!is.null(cur$IS)) cur$IS else NA_character_,
      urldate = if (!is.null(cur$Y2)) cur$Y2 else NA_character_,
      journal = if (!is.null(cur$JO)) cur$JO else if (!is.null(cur$JF)) cur$JF else NA_character_,
      author = if (!is.null(cur$author)) cur$author else NA_character_,
      year = if (!is.null(cur$PY)) cur$PY else if (!is.null(cur$Y1)) cur$Y1 else NA_character_,
      pages = if (!is.null(cur$SP)) {
        end <- if (!is.null(cur$EP)) paste0("-", cur$EP) else ""
        paste0(cur$SP, end)
      } else {
        NA_character_
      },
      file = if (!is.null(cur$L1)) cur$L1 else NA_character_,
      keywords = if (!is.null(cur$KW)) paste(cur$KW, collapse = "; ") else NA_character_,
      note = if (!is.null(cur$N1)) cur$N1 else NA_character_
    )
    records[[length(records) + 1]] <<- rec
    cur <<- list()
  }
  for (ln in lines) {
    if (grepl("^[[:space:]]*ER[[:space:]]*-", ln)) {
      add_cur()
      next
    }
    m <- regexec("^([A-Z0-9]{2})  -[[:space:]]*(.*)$", ln)
    mm <- regmatches(ln, m)[[1]]
    if (length(mm) >= 3) {
      tag <- mm[2]
      val <- mm[3]
      if (tag == "AU") cur$AU <- c(cur$AU, val) else cur[[tag]] <- c(cur[[tag]], val)
    }
  }
  # final
  add_cur()
  if (length(records) == 0) {
    return(data.frame())
  }
  df <- do.call(rbind, lapply(records, function(r) as.data.frame(r, stringsAsFactors = FALSE)))
  df
}

.read_zotero_rdf <- function(path) {
  doc <- xml2::read_xml(path)

  # First, collect all foaf:Person entries (authors/creators)
  persons <- xml2::xml_find_all(doc, "//foaf:Person")
  person_map <- list()
  for (person in persons) {
    about <- xml2::xml_attr(person, "about")
    if (!is.null(about)) {
      surname <- xml2::xml_text(xml2::xml_find_first(person, ".//foaf:surname"))
      given <- xml2::xml_text(xml2::xml_find_first(person, ".//foaf:givenName"))
      # Handle NA values properly
      if (is.na(surname) || length(surname) == 0) surname <- ""
      if (is.na(given) || length(given) == 0) given <- ""
      person_map[[about]] <- list(surname = surname, given = given)
    }
  }

  # Now find all bibliographic items (exclude attachments and collections)
  items <- xml2::xml_find_all(doc, "//bib:Document | //bib:Article | //bib:Book | //bib:Data")

  records <- lapply(items, function(node) {
    fields <- list()

    # Get the item type
    item_type <- xml2::xml_text(xml2::xml_find_first(node, ".//z:itemType"))
    if (!is.na(item_type)) fields$type <- item_type

    # Extract basic metadata: use direct child dc:title to avoid picking nested journal titles
    title_node <- xml2::xml_find_first(node, "dc:title")
    if (length(title_node) > 0) {
      title <- xml2::xml_text(title_node)
      if (!is.na(title) && nzchar(title)) fields$title <- title
    }

    date <- xml2::xml_text(xml2::xml_find_first(node, ".//dc:date"))
    if (!is.na(date)) fields$date <- date

    abstract <- xml2::xml_text(xml2::xml_find_first(node, ".//dcterms:abstract"))
    if (!is.na(abstract)) fields$abstract <- abstract

    publisher <- xml2::xml_text(xml2::xml_find_first(node, ".//dc:publisher"))
    if (!is.na(publisher)) fields$publisher <- publisher

    # Handle journal information: support both referenced and inline Journal nodes
    ispart_node <- xml2::xml_find_first(node, ".//dcterms:isPartOf")
    journal_title <- NULL
    if (length(ispart_node) > 0) {
      journal_ref <- xml2::xml_attr(ispart_node, "resource")
      if (!is.null(journal_ref) && nzchar(journal_ref)) {
        journal_node <- xml2::xml_find_first(doc, paste0("//bib:Journal[@rdf:about='", journal_ref, "']"))
        if (length(journal_node) > 0) {
          jt_node <- xml2::xml_find_first(journal_node, "dc:title")
          if (length(jt_node) > 0) journal_title <- xml2::xml_text(jt_node)
        }
      } else {
        # inline Journal element inside isPartOf
        inline_j <- xml2::xml_find_first(ispart_node, "bib:Journal")
        if (length(inline_j) > 0) {
          jt_node <- xml2::xml_find_first(inline_j, "dc:title")
          if (length(jt_node) > 0) journal_title <- xml2::xml_text(jt_node)
        }
      }
      if (!is.null(journal_title) && !is.na(journal_title) && nzchar(journal_title)) fields$journal <- journal_title
    }

    # Extract authors/creators from bib:authors (handle both inline and referenced)
    authors_node <- xml2::xml_find_first(node, ".//bib:authors")
    if (length(authors_node) > 0) {
      author_refs <- xml2::xml_find_all(authors_node, ".//rdf:li")
      author_names <- character(0)

      for (author_ref in author_refs) {
        # Check if it's a reference (resource attribute)
        ref <- xml2::xml_attr(author_ref, "resource")
        if (!is.null(ref) && !is.na(ref) && ref %in% names(person_map)) {
          person <- person_map[[ref]]
          # Check for NULL, NA and empty values properly
          given_ok <- !is.null(person$given) && !is.na(person$given) && nzchar(person$given)
          surname_ok <- !is.null(person$surname) && !is.na(person$surname) && nzchar(person$surname)

          if (given_ok && surname_ok) {
            author_names <- c(author_names, paste(person$given, person$surname))
          } else if (surname_ok) {
            author_names <- c(author_names, person$surname)
          } else if (given_ok) {
            author_names <- c(author_names, person$given)
          }
        } else {
          # Check if it's an inline foaf:Person
          person_node <- xml2::xml_find_first(author_ref, ".//foaf:Person")
          if (length(person_node) > 0) {
            given <- xml2::xml_text(xml2::xml_find_first(person_node, ".//foaf:givenName"))
            surname <- xml2::xml_text(xml2::xml_find_first(person_node, ".//foaf:surname"))

            # Check for NULL, NA and empty values properly
            given_ok <- !is.null(given) && !is.na(given) && nzchar(given)
            surname_ok <- !is.null(surname) && !is.na(surname) && nzchar(surname)

            if (given_ok && surname_ok) {
              author_names <- c(author_names, paste(given, surname))
            } else if (surname_ok) {
              author_names <- c(author_names, surname)
            } else if (given_ok) {
              author_names <- c(author_names, given)
            }
          }
        }
      }

      if (length(author_names) > 0) {
        fields$author <- author_names
      }
    }

    # Extract programmers for software items (check both inline and referenced)
    programmers_node <- xml2::xml_find_first(node, ".//z:programmers")
    if (length(programmers_node) > 0 && is.null(fields$author)) {
      prog_refs <- xml2::xml_find_all(programmers_node, ".//rdf:li")
      prog_names <- character(0)

      for (prog_ref in prog_refs) {
        # Check if it's a reference (resource attribute)
        ref <- xml2::xml_attr(prog_ref, "resource")
        if (!is.null(ref) && !is.na(ref) && ref %in% names(person_map)) {
          person <- person_map[[ref]]
          # Check for NULL, NA and empty values properly
          given_ok <- !is.null(person$given) && !is.na(person$given) && nzchar(person$given)
          surname_ok <- !is.null(person$surname) && !is.na(person$surname) && nzchar(person$surname)

          if (given_ok && surname_ok) {
            prog_names <- c(prog_names, paste(person$given, person$surname))
          } else if (surname_ok) {
            prog_names <- c(prog_names, person$surname)
          } else if (given_ok) {
            prog_names <- c(prog_names, person$given)
          }
        } else {
          # Check if it's an inline foaf:Person
          person_node <- xml2::xml_find_first(prog_ref, ".//foaf:Person")
          if (length(person_node) > 0) {
            given <- xml2::xml_text(xml2::xml_find_first(person_node, ".//foaf:givenName"))
            surname <- xml2::xml_text(xml2::xml_find_first(person_node, ".//foaf:surname"))

            # Check for NULL, NA and empty values properly
            given_ok <- !is.null(given) && !is.na(given) && nzchar(given)
            surname_ok <- !is.null(surname) && !is.na(surname) && nzchar(surname)

            if (given_ok && surname_ok) {
              prog_names <- c(prog_names, paste(given, surname))
            } else if (surname_ok) {
              prog_names <- c(prog_names, surname)
            } else if (given_ok) {
              prog_names <- c(prog_names, given)
            }
          }
        }
      }

      if (length(prog_names) > 0) {
        fields$author <- prog_names
      }
    }

    # Get URL/identifier
    about <- xml2::xml_attr(node, "about")
    if (!is.null(about)) {
      fields$about <- about
      if (grepl("^https?://", about)) {
        fields$url <- about
      }
    }

    # Extract DOI from identifiers
    identifiers <- xml2::xml_text(xml2::xml_find_all(node, ".//dc:identifier"))
    for (id in identifiers) {
      if (grepl("DOI", id, ignore.case = TRUE)) {
        fields$doi <- gsub(".*DOI\\s*", "", id, ignore.case = TRUE)
        break
      }
    }

    fields
  })

  # Filter out empty records
  records[lengths(records) > 0]
}

.to_bibentry_from_rdf <- function(records) {
  parse_authors <- function(a) {
    if (is.null(a) || length(a) == 0) {
      return(NULL)
    }
    if (is.list(a)) a <- unlist(a)
    a <- a[!is.na(a) & nzchar(a)]
    if (length(a) == 0) {
      return(NULL)
    }

    persons <- lapply(a, function(x) {
      x <- trimws(x)
      if (x == "") {
        return(NULL)
      }

      # Names are already in "Given Surname" format from RDF parsing
      parts <- strsplit(x, "\\s+")[[1]]
      if (length(parts) == 1) {
        utils::person(family = parts[1])
      } else {
        family <- parts[length(parts)]
        given <- paste(parts[-length(parts)], collapse = " ")
        utils::person(given = given, family = family)
      }
    })

    persons <- persons[!vapply(persons, is.null, logical(1))]
    if (length(persons) == 0) {
      return(NULL)
    }
    do.call(c, persons)
  }

  map_type <- function(t) {
    if (is.null(t)) {
      return("Misc")
    }
    t <- tolower(t)
    if (grepl("journal|article", t)) {
      return("Article")
    }
    if (grepl("book", t)) {
      return("Book")
    }
    if (grepl("webpage|document", t)) {
      return("Misc")
    }
    if (grepl("computer|software", t)) {
      return("Manual")
    }
    return("Misc")
  }

  entries <- lapply(seq_along(records), function(i) {
    r <- records[[i]]
    bibtype <- map_type(r$type)
    key <- if (!is.null(r$about)) {
      # Create a clean key from the URL/about field
      clean_key <- basename(r$about)
      clean_key <- gsub("[^A-Za-z0-9_-]", "_", clean_key)
      if (nzchar(clean_key)) clean_key else paste0("zot_rdf_", i)
    } else {
      paste0("zot_rdf_", i)
    }

    authors <- parse_authors(r$author)

    # Handle bibentry validation requirements
    # If it's a Book without author/editor, change to Misc
    if (bibtype == "Book" && is.null(authors)) {
      bibtype <- "Misc"
    }
    # If it's an Article without required fields, change to Misc
    if (bibtype == "Article" && (is.null(r$title) || is.null(authors) || is.null(r$journal))) {
      bibtype <- "Misc"
    }

    fields <- list(
      bibtype = bibtype,
      key = key,
      title = if (!is.null(r$title)) as.character(r$title) else NULL,
      author = authors,
      journal = if (!is.null(r$journal)) as.character(r$journal) else NULL,
      year = if (!is.null(r$date)) {
        # Extract year from date
        year_match <- regexpr("\\b\\d{4}\\b", r$date)
        if (year_match > 0) {
          regmatches(r$date, year_match)
        } else {
          substr(as.character(r$date), 1, 4)
        }
      } else {
        NULL
      },
      publisher = if (!is.null(r$publisher)) as.character(r$publisher) else NULL,
      doi = if (!is.null(r$doi)) as.character(r$doi) else NULL,
      note = if (!is.null(r$abstract)) as.character(r$abstract) else NULL,
      url = if (!is.null(r$url)) as.character(r$url) else NULL
    )

    fields <- fields[!vapply(fields, is.null, logical(1))]
    do.call(utils::bibentry, fields)
  })

  if (length(entries) == 0) {
    return(utils::bibentry())
  }
  do.call(c, entries)
}

.standardize_import_df <- function(df) {
  if (!is.data.frame(df)) {
    return(df)
  }
  # canonical columns expected by tests and other code
  canonical <- c("bibtype", "key", "title", "volume", "issn", "shorttitle", "url", "doi", "abstract", "language", "number", "urldate", "journal", "author", "year", "pages", "file", "keywords", "note")
  # lower-case column names
  names(df) <- tolower(names(df))
  missing <- setdiff(canonical, names(df))
  for (m in missing) df[[m]] <- NA_character_
  # keep only canonical columns in canonical order
  df <- df[, canonical, drop = FALSE]
  # Apply normalization so imported data is comparable across formats
  if ("title" %in% names(df)) df$title <- vapply(df$title, function(x) if (is.na(x)) NA_character_ else norm_title(x), character(1))
  if ("doi" %in% names(df)) df$doi <- vapply(df$doi, function(x) if (is.na(x)) NA_character_ else norm_doi(x), character(1))
  if ("journal" %in% names(df)) df$journal <- vapply(df$journal, function(x) if (is.na(x)) NA_character_ else norm_journal(x), character(1))
  if ("url" %in% names(df)) df$url <- vapply(df$url, function(x) if (is.na(x)) NA_character_ else norm_url(x), character(1))

  # normalize author strings to semicolon-separated 'Family, Given' entries
  normalize_author_field <- function(s) {
    if (is.na(s) || !nzchar(as.character(s))) {
      return(NA_character_)
    }
    s <- as.character(s)
    # split common separators
    parts <- unlist(strsplit(s, "\\s*(?:;|\\sand\\s|\\s&\\s|\\s+and\\s+)\\s*", perl = TRUE))
    parts <- parts[nzchar(trimws(parts))]
    norm_parts <- vapply(parts, function(p) {
      p <- trimws(p)
      # if already 'Family, Given' keep; else try to split tokens
      if (grepl(",", p)) {
        # family may be first token before comma
        fam <- trimws(strsplit(p, ",")[[1]][1])
        giv <- trimws(paste(strsplit(p, ",")[[1]][-1], collapse = ", "))
        if (nzchar(giv)) paste0(fam, ", ", giv) else fam
      } else {
        toks <- unlist(strsplit(p, "\\s+"))
        if (length(toks) == 1) {
          toks[1]
        } else {
          fam <- toks[length(toks)]
          giv <- paste(toks[-length(toks)], collapse = " ")
          paste0(fam, ", ", giv)
        }
      }
    }, character(1))
    paste(norm_parts, collapse = "; ")
  }

  if ("author" %in% names(df)) df$author <- vapply(df$author, function(x) if (is.na(x)) NA_character_ else normalize_author_field(x), character(1))

  df
}

# Normalization helpers ----

norm_title <- function(x) {
  x <- ifelse(is.na(x), NA_character_, as.character(x))
  x <- gsub("\\{", "", x)
  x <- gsub("\\}", "", x)
  # x <- tolower(x)
  x <- gsub("[[:space:]]+", " ", x)
  trimws(x)
}

norm_doi <- function(x) {
  x <- ifelse(is.na(x), NA_character_, as.character(x))
  # x <- tolower(x)
  x <- gsub("^\\s*doi\\s*[:]?:\\s*", "", x)
  x <- trimws(x)
  if (identical(x, "") || is.na(x)) NA_character_ else x
}

norm_journal <- function(x) {
  x <- ifelse(is.na(x), NA_character_, as.character(x))
  x <- gsub("\\\\&", "&", x)
  x <- gsub("&amp;", "&", x)
  x <- gsub("[[:space:]]+", " ", x)
  # trimws(tolower(x))
  trimws(x)
}

norm_url <- function(x) {
  x <- ifelse(is.na(x), NA_character_, as.character(x))
  x <- gsub("[[:space:]]+", "", x)
  # x <- tolower(x)
  trimws(x)
}

extract_last_names <- function(author_str) {
  if (is.na(author_str) || author_str == "") {
    return(character())
  }
  parts <- unlist(strsplit(as.character(author_str), "\\s*(?:;| and | & |, and )\\s*", perl = TRUE))
  sapply(parts, function(p) {
    if (grepl(",", p)) {
      part <- strsplit(p, ",")[[1]][1]
      trimws(part)
    } else {
      w <- unlist(strsplit(p, "\\s+"))
      trimws(w[length(w)])
    }
  }, USE.NAMES = FALSE)
}

# User-facing wrappers ----

#' @title Read RDF, BibTeX, or RIS files exported from Zotero
#' @description Detects the file format and returns a data frame with standardized columns.
#' @param path Path to the exported file (Zotero RDF, BibTeX, or RIS)
#' @return A data.frame with columns: title, author, year, journal, doi, url, abstract, and bibtype
#' @import xml2
#' @export
import <- function(path) {
  stopifnot(file.exists(path))
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("ris")) {
    return(.standardize_import_df(.parse_ris_to_df(path)))
  }
  if (ext %in% c("bib")) {
    return(.standardize_import_df(.parse_bib_to_df(path)))
  }
  if (ext %in% c("rdf", "xml")) {
    records <- .read_zotero_rdf(path)
    # create a richer data.frame mapping many common fields
    df <- do.call(rbind, lapply(records, function(r) {
      bibtype <- if (!is.null(r$type)) tolower(r$type) else NA_character_
      key <- if (!is.null(r$about)) {
        k <- basename(r$about)
        k <- gsub("[^A-Za-z0-9_-]", "_", k)
        if (nzchar(k)) k else NA_character_
      } else {
        NA_character_
      }
      title <- ifelse(is.null(r$title), NA_character_, r$title)
      author <- if (is.null(r$author)) NA_character_ else paste(r$author, collapse = "; ")
      year <- if (!is.null(r$date)) {
        m <- regexpr("\\b\\d{4}\\b", r$date)
        if (m > 0) regmatches(r$date, m) else substr(as.character(r$date), 1, 4)
      } else {
        NA_character_
      }
      journal <- ifelse(is.null(r$journal), NA_character_, r$journal)
      volume <- ifelse(is.null(r$volume), NA_character_, r$volume)
      number <- ifelse(is.null(r$number), NA_character_, r$number)
      pages <- ifelse(is.null(r$pages), NA_character_, r$pages)
      issn <- ifelse(is.null(r$issn), NA_character_, r$issn)
      shorttitle <- ifelse(is.null(r$shorttitle), NA_character_, r$shorttitle)
      language <- ifelse(is.null(r$language), NA_character_, r$language)
      keywords <- ifelse(is.null(r$keywords), NA_character_, r$keywords)
      doi <- ifelse(is.null(r$doi), NA_character_, r$doi)
      url <- ifelse(is.null(r$url), NA_character_, r$url)
      abstract <- ifelse(is.null(r$abstract), NA_character_, r$abstract)
      publisher <- ifelse(is.null(r$publisher), NA_character_, r$publisher)
      note <- ifelse(is.null(r$note), NA_character_, r$note)
      urldate <- NA_character_
      # normalize encoding
      fields <- list(
        bibtype = bibtype, key = key, title = title, volume = volume, issn = issn, shorttitle = shorttitle,
        url = url, doi = doi, abstract = abstract, language = language, number = number,
        urldate = urldate, journal = journal, author = author, year = year, pages = pages,
        file = NA_character_, keywords = keywords, note = note, publisher = publisher
      )
      fields <- lapply(fields, function(x) if (!is.null(x) && is.character(x)) enc2utf8(x) else x)
      as.data.frame(fields, stringsAsFactors = FALSE)
    }))
    return(.standardize_import_df(df))
  }
  stop("Unsupported file extension: ", ext)
}

#' @title Search Zotero DB collection by name
#' @param name Collection name (partial match)
#' @param con An existing DBI connection to a Zotero SQLite DB (optional)
#' @param zotero_db Path to the Zotero SQLite DB (default: '~/Zotero/zotero.sqlite')
#' @return A data.frame with columns: itemID, title, creators, collections, itemTypeID, dateAdded, dateModified, key
#' @export
collection <- function(name, con = NULL, zotero_db = "~/Zotero/zotero.sqlite") {
  stopifnot(!missing(name))
  need_cleanup <- FALSE
  if (is.null(con)) {
    info <- .open_zotero_snapshot(zotero_db)
    con <- info$con
    need_cleanup <- TRUE
  }
  on.exit(
    {
      if (need_cleanup) info$cleanup()
    },
    add = TRUE
  )

  .zotero_query_items(con, "col.collectionName LIKE ?", params = list(paste0("%", name, "%")))
}

#' @title Title keyword search in Zotero DB
#' @param keyword Title keyword (partial match)
#' @param con An existing DBI connection to a Zotero SQLite DB (optional)
#' @param zotero_db Path to the Zotero SQLite DB (default: '~/Zotero/zotero.sqlite')
#' @return A data.frame with columns: itemID, title, creators, collections, itemTypeID, dateAdded, dateModified, key
#' @export
title <- function(keyword, con = NULL, zotero_db = "~/Zotero/zotero.sqlite") {
  stopifnot(!missing(keyword))
  need_cleanup <- FALSE
  if (is.null(con)) {
    info <- .open_zotero_snapshot(zotero_db)
    con <- info$con
    need_cleanup <- TRUE
  }
  on.exit(
    {
      if (need_cleanup) info$cleanup()
    },
    add = TRUE
  )

  .zotero_query_items(con, "lower(idv_title.value) LIKE lower(?)", params = list(paste0("%", keyword, "%")))
}

#' @title Search Zotero DB collection by name
#' @param key Zotero item key (exact match)
#' @param con An existing DBI connection to a Zotero SQLite DB (optional)
#' @param zotero_db Path to the Zotero SQLite DB (default: '~/Zotero/zotero.sqlite')
#' @return A data.frame with columns: itemID, title, creators, collections, itemTypeID, dateAdded, dateModified, key
#' @export
key <- function(key, con = NULL, zotero_db = "~/Zotero/zotero.sqlite") {
  stopifnot(!missing(key))
  need_cleanup <- FALSE
  if (is.null(con)) {
    info <- .open_zotero_snapshot(zotero_db)
    con <- info$con
    need_cleanup <- TRUE
  }
  on.exit(
    {
      if (need_cleanup) info$cleanup()
    },
    add = TRUE
  )

  .zotero_query_items(con, "it.key = ?", params = list(as.character(key)))
}

#' @title Creator (author) keyword search in Zotero DB
#' @param keyword Creator (author) keyword (partial match)
#' @param con An existing DBI connection to a Zotero SQLite DB (optional)
#' @param zotero_db Path to the Zotero SQLite DB (default: '~/Zotero/zotero.sqlite')
#' @return A data.frame with columns: itemID, title, creators, collections, itemTypeID, dateAdded, dateModified, key
#' @export
creator <- function(keyword, con = NULL, zotero_db = "~/Zotero/zotero.sqlite") {
  stopifnot(!missing(keyword))
  need_cleanup <- FALSE
  if (is.null(con)) {
    info <- .open_zotero_snapshot(zotero_db)
    con <- info$con
    need_cleanup <- TRUE
  }
  on.exit(
    {
      if (need_cleanup) info$cleanup()
    },
    add = TRUE
  )

  .zotero_query_items(con, sprintf("EXISTS (SELECT 1 FROM itemCreators ic2 JOIN creators cr2 ON ic2.creatorID = cr2.creatorID WHERE ic2.itemID = it.itemID AND (lower(cr2.name) LIKE lower(?) OR lower(cr2.surname) LIKE lower(?) OR lower(cr2.familyname) LIKE lower(?) OR lower(cr2.givenname) LIKE lower(?) OR lower(cr2.firstname) LIKE lower(?)))"), params = list(rep(paste0("%", keyword, "%"), 5)))
}
