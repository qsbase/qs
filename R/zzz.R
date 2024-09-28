.onAttach <- function(libname, pkgname) {
  packageStartupMessage("qs ", utils::packageVersion("qs"), ". Announcement: https://github.com/qsbase/qs/issues/103")
}
