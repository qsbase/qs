.onAttach <- function(libname, pkgname) {
  packageStartupMessage("qs v.0.18.3. Fixed an issue reading large S4 objects.")
  packageStartupMessage("Objects saved in 0.18 cannot be read by earlier versions. See ChangeLog.")
}