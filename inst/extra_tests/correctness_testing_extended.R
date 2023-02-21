suppressMessages(library(Rcpp))
suppressMessages(library(dplyr))
suppressMessages(library(data.table))
suppressMessages(library(qs))
suppressMessages(library(ggplot2))
suppressMessages(library(sf))
suppressMessages(library(nanotime))
suppressMessages(library(R6))
suppressMessages(library(raster))
options(warn=1)

mcreadRDS <- function(file,mc.cores=min(parallel::detectCores(),4)) {
  con <- pipe(paste0("pigz -d -c -p",mc.cores," ",file))
  object <- readRDS(file = con)
  close(con)
  return(object)
}

if (Sys.info()[['sysname']] != "Windows") {
  myfile <- tempfile()
} else {
  myfile <- "N:/temp/ctest.z"
}

printCarriage <- function(x) {
  cat(x, "\r")
}

# since we can't directly test identity of complex objects, at least check objects print out the same
ggplot_bin <- function(g1) {
  png(myfile, width=500, height=500)
  plot(g1)
  dev.off()
  readBin(myfile, n=1e9, what="raw")
}

# we can test out environments with no special flags
serialize_identical <- function(x1, x2) {
  identical(serialize(x1, NULL), serialize(x2, NULL))
}
################################################################################################

qsave_rand <- function(x, file) {
  alg <- sample(c("lz4", "zstd", "lz4hc", "zstd_stream", "uncompressed"), 1)
  nt <- sample(5,1)
  sc <- sample(0:15,1)
  cl <- sample(10,1)
  ch <- sample(c(T,F),1)
  # cat(alg, nt, sc, cl, ch, "\n")
  qsave(x, file=file, preset = "custom", algorithm = alg,
      compress_level=cl, shuffle_control = sc, nthreads=nt, check_hash = ch)
}

qread_rand <- function(file) {
  ar <- sample(c(T,F),1)
  nt <- sample(5,1)
  # cat(ar, nt, "\n")
  x <- qread(file, use_alt_rep=ar, nthreads=nt, strict=T)
  return(x)
}

################################################################################################

# check parent env recursion
print("check recursive environments, promises and closures")
for(i in 1:10) {
  l1 <- round(exp(runif(1,0,log(1e6))))
  l2 <- round(exp(runif(1,0,log(1e7))))
  x0 <- new.env(parent = baseenv())
  x0$data <- rnorm(l1)
  x1 <- new.env(parent = x0)
  x1$a <- 1
  x1$f <- function() return(1)
  x1$data <- runif(l2)
  environment(x1$f) <- x1
  parent.env(x0) <- x1
  delayedAssign("v", c(not_a_variable, runif(1e100)), assign.env=x0, eval.env=x1)
  delayedAssign("w", c(v, rnorm(1e100)), assign.env=x1, eval.env=x0)
  qsave_rand(x1, myfile)
  x1r <- qread_rand(myfile)
  stopifnot(serialize_identical(x1, x1r))
  print("ok")
  gc()
}
rm(l1, l2, x0, x1, x1r)
gc()

print("mtcars ggplot example")
for(i in 1:10) {
  df <- mtcars %>% sample_n(1e5, replace=T)
  vars <- c("hp", "drat", "wt", "qsec")
  var <- sample(vars, 1)
  g1 <- ggplot(df, aes(x = mpg, y = !!as.symbol(var), color = factor(cyl))) +
    geom_smooth(formula = y ~ x + x^2 + x^3, method="lm") +
    geom_point(data=df %>% sample_n(1000)) +
    scale_color_viridis_d()
  qsave_rand(g1, myfile)
  g2 <- qread_rand(myfile)
  gb1 <- ggplot_bin(g1)
  gb2 <- ggplot_bin(g2)
  stopifnot(identical(gb1, gb2))
  print("ok")
  gc()
}
rm(df, g1, g2, gb1, gb2)
gc()

print("starmap ggplot")
g1 <- ggplot(starnames,
       aes(x = `RA(J2000)`, y=`Dec(J2000)`,
           color = `Const.`)) + geom_point(show.legend=F) +
  geom_text(aes(label = `IAU Name`), show.legend=F)
for(i in 1:10) {
  qsave_rand(g1, myfile)
  g2 <- qread_rand(myfile)
  gb1 <- ggplot_bin(g1)
  gb2 <- ggplot_bin(g2)
  stopifnot(identical(gb1, gb2))
  print("ok")
  gc()
}
rm(g1, g2, gb1, gb2)
gc()

print("github.com/traversc/qs/issues/9")
x <- data.table(x = 1:26, y = letters)
qsave_rand(x, file=myfile)
xu <- qread(myfile, use_alt_rep = T)
data.table::setnames(xu, 1, "a")
stopifnot(identical(c("a", "y"), colnames(xu)))
data.table::setnames(xu, 2, "b")
stopifnot(identical(c("a", "b"), colnames(xu)))

print("github.com/traversc/qs/issues/23")
for(i in 1:10) {
  z <- data.table(x=nanotime(runif(1e6)*1e18))
  qsave_rand(z, file=myfile)
  zu <- qread_rand(myfile)
  print(zu$x[1])
  stopifnot(identical(z$x, zu$x))
}
rm(z, zu)
gc()


# large S4 objects
# https://github.com/traversc/qs/issues/14
# Data is private, so not uploaded online
print("github.com/traversc/qs/issues/14 (this takes a long time)")
if (Sys.info()[['sysname']] != "Windows") {
  system("cat /mnt/n/R_stuff/qs_extended_tests/issue_14_data.rds > /dev/null")
  r <- mcreadRDS("/mnt/n/R_stuff/qs_extended_tests/issue_14_data.rds")
} else {
  r <- readRDS("N:/R_stuff/qs_extended_tests/issue_14_data.rds")
}
qsave(r, myfile)
ru <- qread(myfile)
stopifnot(identical(r, ru))
rm(r, ru)
gc()

# for(i in 1:5) {
#   qsave_rand(r, myfile)
#   ru <- qread_rand(myfile)
#   stopifnot(identical(r, ru))
#   print("ok")
#   gc()
# }
# rm(r, ru)
# gc()

# Efficient serialization of ggplot objects
# https://github.com/traversc/qs/issues/21
# Data is private, so not uploaded online
print("github.com/traversc/qs/issues/21")
print("testing issue 21 no longer works because of deprecated plot object")
# print("reading in initial data")
# if (Sys.info()[['sysname']] != "Windows") {
#   g1 <- readRDS("~/N/R_stuff/qs_extended_tests/issue_21_data.rds")
# } else {
#   g1 <- readRDS("N:/R_stuff/qs_extended_tests/issue_21_data.rds")
# }
# print("plotting data")
# gb1 <- ggplot_bin(g1)
# print("qs serialization")
# qsave(g1, myfile)
# print("qs deserialization")
# g2 <- qread(myfile)
# print("plotting data")
# gb2 <- ggplot_bin(g2)
# stopifnot(identical(gb1, gb2))
# rm(g1, gb1, g2, gb2)
# gc()

# for(i in 1:5) {
#   qsave_rand(g1, myfile)
#   g2 <- qread_rand(myfile)
#   gb2 <- ggplot_bin(g2)
#   stopifnot(identical(gb1, gb2))
#   print("ok")
#   rm(g2, gb2)
#   gc()
# }

print("https://github.com/traversc/qs/issues/6")
RiskModel <- R6Class(
  classname = "RiskModel",
  public = list(
  name = NULL,
  covMat = NULL, ## N * N
  initialize = function(name, covMat = NULL) {
    self$covMat <- covMat
    self$name <- name
  }
  ),
  private = NULL,
  active = NULL,
  inherit = NULL,
  lock_objects = TRUE,
  class = TRUE,
  portable = TRUE,
  lock_class = FALSE,
  cloneable = TRUE,
  parent_env = parent.frame()
)

for(i in 1:5) {
  x <- RiskModel$new("x", matrix(runif(10000^2,10000,10000)))
  qsave_rand(x, myfile)
  y <- qread_rand(myfile)
  stopifnot(identical(x$name, y$name))
  stopifnot(identical(x$covMat, y$covMat))
  print("ok")
  rm(x, y)
  gc()
}

print("https://github.com/traversc/qs/issues/27")
nx <- ny <- 2
N <- nx * ny
template <- raster(nrows = ny, ncols = nx, xmn = -nx / 2, xmx = nx / 2,
                   ymn = -ny / 2, ymx = ny / 2)

DEM <- raster(template)
DEM[] <- runif(N)
qsave(DEM, file = myfile)
DEM2 <- qread(myfile)
all.equal(DEM@legend@names, DEM2@legend@names)

print("https://github.com/traversc/qs/issues/29")
XClass <- R6Class( "XClass", active = list(r=function() runif(1)) )
x <- XClass$new()
qsave(x, file=myfile)
x2 <- qread(myfile)
stopifnot(is.numeric(x2$r))

print("https://github.com/traversc/qs/issues/43")
for(i in 1:200) {
  a <- paste0(rep(".", i), collapse = "")
  x <- list(a = a, # a= '....................................................................',
            b= cbind(integer(246722), as.data.frame(matrix(data=0, nrow=246722, ncol=8))))
  x2 <- qs::qdeserialize(qs::qserialize(x))
  stopifnot(identical(x, x2))
}


f1 <- compiler::cmpfun(function(x) { x <- x + 1L; environment() })
f2 <- compiler::cmpfun(function(x) { x <- x + 1.0; environment() })
# f3 <- compiler::cmpfun(function(x) { x <- x | TRUE; environment() })
# .Internal(inspect(f1(1L)))
# .Internal(inspect(f2(1.0)))
# .Internal(inspect(f3(TRUE))) # this doesn't work?


print("https://github.com/traversc/qs/issues/50")
for(i in 1:200) {
  x2 <- qs::qdeserialize(qs::qserialize(f1(1L)))
  stopifnot(identical(x2$x, 2L))
  x2 <- qs::qdeserialize(qs::qserialize(f2(1.0)))
  stopifnot(identical(x2$x, 2.0))
}

