#' qcache
#'
#' Helper function for caching objects for long running tasks
#'
#' This is a (very) simple helper function to cache results of long running calculations. There are other packages specializing
#' in caching data that are more feature complete.
#'
#' The evaluated expression is saved with [qsave()] in `<cache_dir>/<name>.qs`.
#' If the file already exists instead, the expression is not evaluated and the cached result is read using [qread()] and returned.
#'
#' To clear a cached result, you can manually delete the associated `.qs` file, or you can call [qcache()] with `clear = TRUE`.
#' If `prompt` is also `TRUE` a prompt will be given asking you to confirm deletion.
#' If `name` is not specified, all cached results in `cache_dir` will be removed.
#'
#' @param expr The expression to evaluate.
#' @param name The cached expression name (see details).
#' @param envir The environment to evaluate `expr` in.
#' @param cache_dir The directory to store cached files in.
#' @param clear Set to `TRUE` to clear the cache (see details).
#' @param prompt Whether to prompt before clearing.
#' @param qsave_params Parameters passed on to `qsave`.
#' @param qread_params Parameters passed on to `qread`.
#'
#' @export
#'
#' @examples
#' cache_dir <- tempdir()
#'
#' a <- 1
#' b <- 5
#'
#' # not cached
#' result <- qcache({a + b},
#'                  name="aplusb",
#'                  cache_dir = cache_dir,
#'                  qsave_params = list(preset="fast"))
#'
#' # cached
#' result <- qcache({a + b},
#'                  name="aplusb",
#'                  cache_dir = cache_dir,
#'                  qsave_params = list(preset="fast"))
#'
#' # clear cached result
#' qcache(name="aplusb", clear=TRUE, prompt=FALSE, cache_dir = cache_dir)
qcache <- function(expr, name, envir = parent.frame(), cache_dir = ".cache", clear = FALSE, prompt = TRUE, qsave_params = list(), qread_params = list()) {
  if (clear) {
    if (missing(name)) {
      files <- list.files(cache_dir, pattern = ".qs$", full.names = TRUE)
    } else {
      stopifnot(length(name) == 1)
      files <- paste0(cache_dir, "/", name, ".qs")
      if (!file.exists(files)) {
        stop("Cached file does not exist")
      }
    }
    if (prompt) {
      rl <- readline("Clear cached file(s)? (yes/no) ")
      if (rl == "yes") {
        # nothing
      } else if (rl == "no") {
        return(FALSE)
      } else {
        stop("Please enter yes or no")
      }
    }
    rem <- file.remove(files)
    if (!(all(rem))) {
      rem <- paste0(files[!rem], collapse = ", ")
      stop(paste0("The following files couldn't be removed: ", rem))
    }
    return(TRUE)
  } else {
    stopifnot(length(name) == 1)
    file <- paste0(cache_dir, "/", name, ".qs")
    if (file.exists(file)) {
      print("cached")
      qread_params$file <- file
      return(do.call(qread, args = qread_params, envir = envir))
    } else {
      print("not cached")
      if (!dir.exists(cache_dir)) {
        dir.create(cache_dir, recursive = TRUE)
      }
      x <- eval(expr, envir = envir)
      qsave_params$x <- x
      qsave_params$file <- file
      do.call(qsave, qsave_params)
      return(x)
    }
  }
}
