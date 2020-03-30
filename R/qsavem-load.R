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
  objnames = trimws(strsplit(gsub('qsavem\\(([^)]+)\\)', '\\1', paste0(deparse(sys.call()))), ",")[[1]])
  
  # save un-named objects as a list.
  savelist = list()
  unnamed = which(!grepl('=', objnames))
  for(i in unnamed){
    savelist[[objnames[i]]] <- datalist[[i]]
  }
    
  # put the final call together.
  eval(parse(
    text = paste0('qsave(savelist, ', paste0(objnames[-unnamed], collapse =', '), ')')
  ))
  
}

#' qload
#' 
#' Reads an object in a file serialized to disk using qsavem.
  #' @usage qload(file, use_alt_rep=FALSE, strict=FALSE, nthreads=1)
#' @param file the file name/path.
#' @param env the environment where the data should be loaded.
#' @param ... additional arguments will be passed to qread.
#' @return Nothing is explicitly returned, but the function will load the saved objects into the workspace.
#' @details 
#' This function extends qread to replicate the functionality of base::load to load multiple saved objects into your workspace.
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
qload <- function(file, env = parent.frame(), ...) {
  
  savelist = qread(file, ...)
  
  if(!is.list(savelist) || is.null(names(savelist))) stop(paste0("Object read from ", file, " is not a named list."))
  
  list2env(savelist, env)
  
}
