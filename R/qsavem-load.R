#' qsavem
#'
#' Saves (serializes) multiple objects to disk.
#'
#' This function extends [qsave()] to replicate the functionality of [base::save()] to save multiple objects. Read them back with [qload()].
#'
#' @param ... Objects to serialize. Named arguments will be passed to [qsave()] during saving. Un-named arguments will be saved. A named `file` argument is required.
#'
#' @export
#'
#' @examples
#' x1 <- data.frame(int = sample(1e3, replace=TRUE),
#'                  num = rnorm(1e3),
#'                  char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'                  stringsAsFactors = FALSE)
#' x2 <- data.frame(int = sample(1e3, replace=TRUE),
#'                  num = rnorm(1e3),
#'                  char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'                  stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qsavem(x1, x2, file=myfile)
#' rm(x1, x2)
#' qload(myfile)
#' exists('x1') && exists('x2') # returns true
#'
#' # qs support multithreading
#' qsavem(x1, x2, file=myfile, nthreads=2)
#' rm(x1, x2)
#' qload(myfile, nthreads=2)
#' exists('x1') && exists('x2') # returns true
qsavem <- function (...) {
  full_call <- as.list(sys.call())[-1]
  objects <- list(...)
  unnamed <- which(names(objects) == "")
  unnamed_list <- objects[unnamed]
  names(unnamed_list) <- sapply(unnamed, function(i) parse(text = full_call[[i]]))
  named_list <- objects[-unnamed]
  named_list$x <- unnamed_list
  do.call(qsave,named_list)
}


#' qload
#'
#' Reads an object in a file serialized to disk using [qsavem()].
#'
#' This function extends qread to replicate the functionality of [base::load()] to load multiple saved objects into your workspace. `qload` and `qreadm` are alias of the same function.
#'
#' @param file The file name/path.
#' @param env The environment where the data should be loaded.
#' @param ... additional arguments will be passed to qread.
#'
#' @return Nothing is explicitly returned, but the function will load the saved objects into the workspace.
#' @export
#'
#' @examples
#' x1 <- data.frame(int = sample(1e3, replace=TRUE),
#'                  num = rnorm(1e3),
#'                  char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'                  stringsAsFactors = FALSE)
#' x2 <- data.frame(int = sample(1e3, replace=TRUE),
#'                  num = rnorm(1e3),
#'                  char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'                  stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qsavem(x1, x2, file=myfile)
#' rm(x1, x2)
#' qload(myfile)
#' exists('x1') && exists('x2') # returns true
#'
#' # qs support multithreading
#' qsavem(x1, x2, file=myfile, nthreads=2)
#' rm(x1, x2)
#' qload(myfile, nthreads=2)
#' exists('x1') && exists('x2') # returns true
qreadm <- function(file, env = parent.frame(), ...) {

  savelist <- qread(file, ...)

  if (!is.list(savelist) || is.null(names(savelist))) stop(paste0("Object read from ", file, " is not a named list."))

  invisible(list2env(savelist, env))

}


#' qreadm
#'
#' @rdname qreadm
#' @export
qload <- qreadm
