context("qsavem-load")

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
