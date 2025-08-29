#' @title Write a BIB export file
#' @description Write a BibTeX file with additional fields used by Web of Science/Clarivate for import.
#' @param bibs A \code{bibentry} object containing the bibliography entries.
#' @param path The file path where the BibTeX entries will be saved.
#' @return None. The function writes the output directly to the specified file.
#' @export
write_bib <- function(bibs, path) {
  stopifnot(inherits(bibs, 'bibentry'))
  con <- file(path, 'w')
  on.exit(close(con))
  
  for (i in seq_along(bibs)) {
    be <- bibs[i]
    
    # Get entry type and key
    entry_type <- if (!is.null(be$bibtype)) tolower(be$bibtype) else 'misc'
    key <- if (!is.null(be$key)) be$key else paste0('entry_', i)
    
    # Start BibTeX entry
    writeLines(paste0('@', entry_type, '{', key, ','), con)
    
    # Add standard fields
    if (!is.null(be$title)) {
      writeLines(paste0('  title = {', be$title, '},'), con)
    }
    
    if (!is.null(be$author)) {
      auth_str <- paste(sapply(be$author, function(p) {
        paste(p$family, paste(p$given, collapse = ' '), sep = ', ')
      }), collapse = ' and ')
      writeLines(paste0('  author = {', auth_str, '},'), con)
    }
    
    if (!is.null(be$journal)) {
      writeLines(paste0('  journal = {', be$journal, '},'), con)
    }
    
    if (!is.null(be$year)) {
      writeLines(paste0('  year = {', be$year, '},'), con)
    }
    
    if (!is.null(be$publisher)) {
      writeLines(paste0('  publisher = {', be$publisher, '},'), con)
    }
    
    if (!is.null(be$doi)) {
      writeLines(paste0('  doi = {', be$doi, '},'), con)
    }
    
    if (!is.null(be$url)) {
      writeLines(paste0('  url = {', be$url, '},'), con)
    }
    
    # Add Clarivate-specific fields
    writeLines(paste0('  id = {', key, '},'), con)  # ID field
    writeLines('  citations = {},', con)  # CI field for citations (empty)
    
    if (!is.null(be$note)) {
      writeLines(paste0('  abstract = {', be$note, '},'), con)
    }
    
    # Close entry (remove trailing comma)
    writeLines('}', con)
    writeLines('', con)  # Empty line between entries
  }
}
