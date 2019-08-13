.onAttach <- function(libname, pkgname) {
  msg <- c("qs v0.18.3. Fixed an issue reading large S4 objects. See ChangeLog.")
  packageStartupMessage(msg)
}