#' @title Read Zotero export (RDF, BibTeX, RIS)
#' @description Auto-detects the file format and returns a list with elements: data.frame (revtools style) and bib
#' (bibentry)
#' @param path Path to the exported file (RDF/XML, BibTeX, or RIS)
#' @return A list with two elements: df (data.frame) and bib (bibentry)
#' @import xml2
#' @export  
read_zotero <- function(path) {
  stopifnot(file.exists(path))
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c('ris', 'bib')) {
  df <- .safe_revtools_read(path)
  bib <- .revtools_to_bibentry(df)
    return(list(df = df, bib = bib))
  }
  if (ext %in% c('rdf', 'xml')) {
    records <- .read_zotero_rdf(path)
    bib <- .to_bibentry_from_rdf(records)
    # also create a minimal data.frame with title, author, year
    df <- do.call(rbind, lapply(records, function(r) {
      title <- ifelse(is.null(r$title), NA, r$title)
      author <- if (is.null(r$author)) {
        NA
      } else {
        paste(r$author, collapse = '; ')
      }
      year <- ifelse(is.null(r$date), ifelse(is.null(r$year), NA, r$year), substr(r$date, 1, 4))
      # force UTF-8 on fields that may contain non-ASCII text
      title <- if (!is.na(title) && is.character(title)) enc2utf8(title) else title
      author <- if (!is.na(author) && is.character(author)) enc2utf8(author) else author
      year <- if (!is.na(year) && is.character(year)) enc2utf8(year) else year
      data.frame(title = title, author = author, year = year, stringsAsFactors = FALSE)
    }))
    return(list(df = df, bib = bib))
  }
  stop('Unsupported file extension: ', ext)
}

# Try reading with revtools, preemptively clean encoding to avoid translation warnings
.safe_revtools_read <- function(path) {
  if (!requireNamespace('revtools', quietly = TRUE)) stop("Package 'revtools' required")
  
  # Read the file as raw text and preemptively clean encoding issues
  raw_text <- readLines(path, warn = FALSE, encoding = 'unknown')
  
  # Convert each line to UTF-8, then immediately transliterate to ASCII to avoid rbind encoding conflicts
  txt_ascii <- vapply(raw_text, function(line) {
    # First convert to UTF-8 if needed
    if (!validUTF8(line)) {
      # Try multiple common encodings
      for (enc in c('latin1', 'CP1252', 'UTF-8')) {
        result <- tryCatch({
          iconv(line, from = enc, to = 'UTF-8', sub = '')
        }, error = function(e) NULL)
        if (!is.null(result) && !is.na(result) && validUTF8(result)) {
          line <- result
          break
        }
      }
      # Fallback if still not valid UTF-8
      if (!validUTF8(line)) {
        line <- iconv(line, from = 'latin1', to = 'UTF-8', sub = '?')
      }
    }
    
    # Now transliterate to ASCII to avoid any encoding conflicts in revtools
    ascii_result <- tryCatch({
      iconv(line, to = "ASCII//TRANSLIT//IGNORE", sub = "")
    }, error = function(e) {
      # Fallback: just remove non-ASCII characters
      gsub("[^\x01-\x7F]", "", line)
    })
    
    if (is.na(ascii_result) || length(ascii_result) == 0) {
      return(line)
    }
    
    ascii_result
  }, FUN.VALUE = character(1), USE.NAMES = FALSE)
  
  # Write cleaned content to temp file
  tmp <- tempfile(fileext = paste0('.', tools::file_ext(path)))
  writeLines(txt_ascii, tmp, useBytes = TRUE)
  
  # Read with revtools (should be clean now)
  df <- revtools::read_bibliography(tmp)
  unlink(tmp)
  
  # Ensure all character columns are properly UTF-8 encoded
  if (is.data.frame(df) && nrow(df) > 0) {
    df[] <- lapply(df, function(col) {
      if (is.character(col)) enc2utf8(col) else col
    })
  }
  
  df
}

.revtools_to_bibentry <- function(df) {
  if (nrow(df) == 0) return(utils::bibentry())
  parse_authors <- function(a) {
    if (is.null(a)) return(NULL)
    if (is.list(a)) a <- unlist(a)
    if (length(a) == 0) return(NULL)
    if (length(a) > 1) authors <- a else authors <- unlist(strsplit(a, ' and |;|\\|'))
    authors <- trimws(authors)
    persons <- lapply(authors, function(x) {
      if (grepl(',', x)) {
        parts <- strsplit(x, ',')[[1]]
        utils::person(given = trimws(paste(parts[-1], collapse = ' ')), family = trimws(parts[1]))
      } else {
        parts <- strsplit(x, '\\s+')[[1]]
        utils::person(given = paste(parts[-length(parts)], collapse = ' '), family = parts[length(parts)])
      }
    })
    do.call(c, persons)
  }
  entries <- lapply(seq_len(nrow(df)), function(i) {
    row <- df[i, , drop = FALSE]
    key <- if (!is.null(row$key) && nzchar(row$key)) as.character(row$key) else paste0('zot_', i)
    authors <- parse_authors(row$author)
    fields <- list(bibtype = if (!is.null(row$bibtype)) as.character(row$bibtype) else 'Misc', key = key, title = if (!is.null(row$title)) as.character(row$title) else NULL, author = authors, journal = if (!is.null(row$journal)) as.character(row$journal) else NULL, year = if (!is.null(row$year)) as.character(row$year) else NULL, publisher = if (!is.null(row$publisher)) as.character(row$publisher) else NULL, url = if (!is.null(row$url)) as.character(row$url) else NULL)
    fields <- fields[!vapply(fields, is.null, logical(1))]
    do.call(utils::bibentry, fields)
  })
  do.call(c, entries)
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
    
    # Extract basic metadata
    title <- xml2::xml_text(xml2::xml_find_first(node, ".//dc:title"))
    if (!is.na(title)) fields$title <- title
    
    date <- xml2::xml_text(xml2::xml_find_first(node, ".//dc:date"))
    if (!is.na(date)) fields$date <- date
    
    abstract <- xml2::xml_text(xml2::xml_find_first(node, ".//dcterms:abstract"))
    if (!is.na(abstract)) fields$abstract <- abstract
    
    publisher <- xml2::xml_text(xml2::xml_find_first(node, ".//dc:publisher"))
    if (!is.na(publisher)) fields$publisher <- publisher
    
    # Handle journal information
    journal_ref <- xml2::xml_attr(xml2::xml_find_first(node, ".//dcterms:isPartOf"), "resource")
    if (!is.null(journal_ref)) {
      journal_node <- xml2::xml_find_first(doc, paste0("//bib:Journal[@rdf:about='", journal_ref, "']"))
      if (length(journal_node) > 0) {
        journal_title <- xml2::xml_text(xml2::xml_find_first(journal_node, ".//dc:title"))
        if (!is.na(journal_title)) fields$journal <- journal_title
      }
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
    if (is.null(a) || length(a) == 0) return(NULL)
    if (is.list(a)) a <- unlist(a)
    a <- a[!is.na(a) & nzchar(a)]
    if (length(a) == 0) return(NULL)
    
    persons <- lapply(a, function(x) {
      x <- trimws(x)
      if (x == "") return(NULL)
      
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
    if (length(persons) == 0) return(NULL)
    do.call(c, persons)
  }
  
  map_type <- function(t) {
    if (is.null(t)) return('Misc')
    t <- tolower(t)
    if (grepl('journal|article', t)) return('Article')
    if (grepl('book', t)) return('Book')
    if (grepl('webpage|document', t)) return('Misc')
    if (grepl('computer|software', t)) return('Manual')
    return('Misc')
  }
  
  entries <- lapply(seq_along(records), function(i) {
    r <- records[[i]]
    bibtype <- map_type(r$type)
    key <- if (!is.null(r$about)) {
      # Create a clean key from the URL/about field
      clean_key <- basename(r$about)
      clean_key <- gsub("[^A-Za-z0-9_-]", "_", clean_key)
      if (nzchar(clean_key)) clean_key else paste0('zot_rdf_', i)
    } else {
      paste0('zot_rdf_', i)
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
      } else NULL,
      publisher = if (!is.null(r$publisher)) as.character(r$publisher) else NULL,
      doi = if (!is.null(r$doi)) as.character(r$doi) else NULL,
      note = if (!is.null(r$abstract)) as.character(r$abstract) else NULL,
      url = if (!is.null(r$url)) as.character(r$url) else NULL
    )
    
    fields <- fields[!vapply(fields, is.null, logical(1))]
    do.call(utils::bibentry, fields)
  })
  
  if (length(entries) == 0) return(utils::bibentry())
  do.call(c, entries)
}
