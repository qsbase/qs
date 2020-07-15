#' qsavem
#' 
#' Saves (serializes) multiple objects to disk. 
#' @usage qsavem(file, ...)
#' @param file the file name/path.
#' @param ... objects to serialize. Named arguments will be passed to qsave during saving. Un-named arguments will be saved. 
#' @details 
#' This function extends qsave to replicate the functionality of base::save to save multiple objects. Read them back with qload.
#' @examples 
#' x1 <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
#' x2 <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qsavem(x1, x2, file=myfile)
#' rm(x1, x2)
#' qload(myfile)
#' exists(c('x1', 'x2')) # returns true
#' 
#' # qs support multithreading
#' qsavem(x1, x2, file=myfile, nthreads=2)
#' rm(x1, x2)
#' qload(myfile, nthreads=2)
#' exists(c('x1', 'x2')) # returns true
#' @export
qsavem <- function(file, ...) { 
  
  datalist = list(...)
  
  # get object names from the function call.
  full_call = paste0(deparse(sys.call()), collapse = "")
  drop_function_name = gsub('qsavem\\(([^)]+)\\)', '\\1', full_call)
  objnames = trimws(strsplit(drop_function_name, ",")[[1]])
  
  # named arguments (file, nthread, etc.) will be passed to qsave.
  # unnamed will be saved.
  unnamed = which(!grepl('=', objnames))
  named = objnames[-unnamed]
  unnamed = objnames[unnamed]
  
  # save un-named objects as a list.
  savelist = list()
  for(i in 1:length(unnamed)){
    savelist[[unnamed[i]]] <- datalist[[i]]
  }
  
  # put the final call together.
  eval(parse(
    text = paste0('qsave(savelist,  ', paste0(named, collapse =', '), ')')
  ))
  
}

#' qload
#' 
#' Reads an object in a file serialized to disk using qsavem.
#' @usage qload(file, env = parent.frame(), ...)
#' @param file the file name/path.
#' @param env the environment where the data should be loaded.
#' @param ... additional arguments will be passed to qread.
#' @return Nothing is explicitly returned, but the function will load the saved objects into the workspace.
#' @details 
#' This function extends qread to replicate the functionality of base::load to load multiple saved objects into your workspace. `qloadm` and `qsavem` are alias of the same function. 
#' @examples 
#' x1 <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
#' x2 <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qsavem(x1, x2, file=myfile)
#' rm(x1, x2)
#' qload(myfile)
#' exists(c('x1', 'x2')) # returns true
#' 
#' # qs support multithreading
#' qsavem(x1, x2, file=myfile, nthreads=2)
#' rm(x1, x2)
#' qload(myfile, nthreads=2)
#' exists(c('x1', 'x2')) # returns true
#' @export
qreadm <- function(file, env = parent.frame(), ...) {
  
  savelist = qread(file, ...)
  
  if(!is.list(savelist) || is.null(names(savelist))) stop(paste0("Object read from ", file, " is not a named list."))
  
  invisible(list2env(savelist, env))
  
}


#' qreadm
#' @usage qreadm(file, env = parent.frame(), ...)
#' @rdname qreadm
#' @export
qload <- qreadm