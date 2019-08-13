.onAttach <- function(libname, pkgname) {
  msg <- c("qs v0.18.3. Fixed an issue reading large S4 objects. Previous versions can't read v0.18+ objects. See ChangeLog.")
  packageStartupMessage(msg)
}