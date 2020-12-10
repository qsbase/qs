SHELL   := /bin/bash
PACKAGE := $(shell perl -aF: -ne 'print, exit if s/^Package:\s+//' DESCRIPTION)
VERSION := $(shell perl -aF: -ne 'print, exit if s/^Version:\s+//' DESCRIPTION)
BUILD   := $(PACKAGE)_$(VERSION).tar.gz

.PHONY: doc build install test vignette $(BUILD)

check: $(BUILD)
	R CMD check --as-cran $<

check-cran: $(BUILD)
	# R --interactive --no-save --args $< <<<'rhub::check_for_cran(commandArgs(T)[1])'
	# Rscript -e "rhub::check_on_solaris()"
	Rscript -e 'rhub::check("$(BUILD)", platform = c("ubuntu-gcc-devel", "windows-x86_64-devel", "solaris-x86-patched", "linux-x86_64-rocker-gcc-san"))'

build:
	autoconf
	chmod 755 cleanup
	chmod 755 configure
	find src/ -type f -exec chmod 644 {} \;
	chmod 644 ChangeLog DESCRIPTION Makefile NAMESPACE README.md
	./configure
	./cleanup
	Rscript -e "library(Rcpp); compileAttributes('.');"
	Rscript -e "devtools::load_all(); roxygen2::roxygenise('.');"
	find . -iname "*.a" -exec rm {} \;
	find . -iname "*.o" -exec rm {} \;
	find . -iname "*.so" -exec rm {} \;
	R CMD build .

install:
	autoconf
	chmod 755 cleanup
	chmod 755 configure
	find src/ -type f -exec chmod 644 {} \;
	chmod 644 ChangeLog DESCRIPTION Makefile NAMESPACE README.md
	./configure
	./cleanup
	Rscript -e "library(Rcpp); compileAttributes('.');"
	Rscript -e "devtools::load_all(); roxygen2::roxygenise('.');"
	find . -iname "*.a" -exec rm {} \;
	find . -iname "*.o" -exec rm {} \;
	find . -iname "*.so" -exec rm {} \;
	R CMD build . # --no-build-vignettes
	R CMD INSTALL $(BUILD) --configure-args="--with-simd=AVX2"
	# R CMD INSTALL $(BUILD) --configure-args="--with-simd=AVX2,--with-lz4-force-compile"

vignette:
	Rscript -e "rmarkdown::render(input='vignettes/vignette.rmd', output_format='all')"
	mv vignettes/vignette.md README.md
	sed -r -i 's/\((.+)\.png/\(vignettes\/\1\.png/' README.md

test:
	Rscript tests/correctness_testing.R filestream
	Rscript tests/correctness_testing.R fd
	Rscript tests/correctness_testing.R memory
	Rscript tests/regression_testing.R

testext:
	Rscript tests/correctness_testing_extended.R

testqsavem:
	Rscript tests/qsavemload_testing.R

bench:
	Rscript tests/benchmark_testing.R
