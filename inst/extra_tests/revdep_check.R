# [1] "cfbfastR"        "drake"           "easyr"           "fastverse"
# [5] "glow"            "LAGOSNE"         "mlr3oml"         "multidplyr"
# [9] "mustashe"        "nflfastR"        "nflreadr"        "nlmixr"
# [13] "pins"            "ProjectTemplate" "reproducible"    "RxODE"
# [17] "SpaDES.core"     "targets"

# library(devtools)
# packages <- revdep("qs")
# packages <- setdiff(packages, c("stringfish", "glow"))
# print(packages)

# library(revdepcheck) # devtools::install_github("r-lib/revdepcheck")
# revdep_check(num_workers=12)

library(usethis)
use_revdep()

library(revdepcheck)
revdep_check(num_workers=12)
