library(testthat)
library(qs)

test_that("works as expected: defaults", {
    
    file = tempfile()

    x1 = iris
    x2 = mtcars
        
    qsavem(x1, x2, file = file)
    rm(x1, x2)

    qload(file = file)
    expect_equal(x1, iris)
    expect_equal(x2, mtcars)
  
})

test_that("works as expected: nthreads", {
    
    file = tempfile()

    x1 = iris
    x2 = mtcars
        
    qsavem(x1, x2, file = file, nthreads=2)
    rm(x1, x2)

    qload(file = file, nthreads=2)
    expect_equal(x1, iris)
    expect_equal(x2, mtcars)
  
})

test_that("issue #39", {
    
    file = tempfile()
    
    seurat = 1 
    lineages = 2
    T.markers = 3

    qsavem(file = file, seurat, lineages, T.markers)

    rm(seurat, lineages, T.markers)
    qload(file = file)
    expect_equal(c(seurat, lineages, T.markers), c(1, 2, 3))
    
    # alternate synatx
    qsavem(seurat, lineages, T.markers, file = file)
    
    rm(seurat, lineages, T.markers)
    qload(file = file)    
    expect_equal(c(seurat, lineages, T.markers), c(1, 2, 3))
    
    # alternate synatx
    qsavem(seurat, lineages, file = file, T.markers)
    
    rm(seurat, lineages, T.markers)
    qload(file = file)    
    expect_equal(c(seurat, lineages, T.markers), c(1, 2, 3))
  
})

test_that("issue #46", {
  test <- function() {
    result_file <- "test.qs"
    test1 <- rnorm(100)
    test2 <- rnorm(100)
    qsavem(test1, test2, file = result_file)
  }
  test()
})