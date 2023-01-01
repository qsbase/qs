#' qread_url
#'
#' A helper function that reads data from the internet to memory and deserializes the object with [qdeserialize()].
#'
#' See [qdeserialize()] for additional details.
#'
#' @usage qread_url(url, buffer_size, ...)
#'
#' @param url The URL where the object is stored
#' @param buffer_size The buffer size used to read in data (default `16777216L` i.e. 16 MB)
#' @param ... Arguments passed to [qdeserialize()]
#' @inherit qread return
#'
#' @export
#'
#' @examples
#'\dontrun{
#' x <- qread_url("http://example_url.com/my_file.qs")
#'}
qread_url <- function(url, buffer_size = 16777216L, ...) {
  con <- file(url, "rb", raw = TRUE)
  data <- list()
  i <- 1L
  data[[1L]] <- readBin(con, what = "raw", n = buffer_size)
  while(length(data[[i]]) != 0) {
    i <- i + 1L
    data[[i]] <- readBin(con, what = "raw", n = buffer_size)
  }
  data <- do.call(c, data)
  close(con)
  qdeserialize(data,...)
}
