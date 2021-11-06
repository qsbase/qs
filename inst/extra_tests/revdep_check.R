
# Reverse imports:	LAGOSNE, multidplyr, mustashe, RxODE, SpaDES.core
# Reverse linking to:	RxODE
# Reverse suggests:	dipsaus, drake, easyr, glow, mlr3oml, nflfastR, ProjectTemplate, reproducible, stringfish, targets

library(devtools)
#library(revdepcheck)
packages <- revdep("qs")
packages <- setdiff(packages, c("stringfish"))

print(packages)

install.packages(packages)
