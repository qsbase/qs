.onAttach <- function(libname, pkgname) {
  packageStartupMessage("qs ", utils::packageVersion("qs"))
}
