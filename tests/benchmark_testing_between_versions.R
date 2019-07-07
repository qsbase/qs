file <- "/tmp/test.z"

dataframeGen <- function() {
  nr <- 1e6
  data.frame(a=rnorm(nr), 
             b=rpois(100,nr),
             c=sample(starnames[["IAU Name"]],nr,T), 
             d=factor(sample(state.name,nr,T)), stringsAsFactors = F)
}

listGen <- function() {
  as.list(sample(1e6))
}

grid <- expand.grid(ver = c(14:17), data = c("list", "dataframe"), 
                    preset = c("fast", "balanced", "high"), 
                    reps=1:5, stringsAsFactors = F)

write_time <- numeric(nrow(grid))
read_time <- numeric(nrow(grid))
for(i in 1:nrow(grid)) {
  if(grid$data[i] == "list") {
    x <- listGen()
  } else if(grid$data[i] == "dataframe") {
    x <- dataframeGen()
  }
  if(grid$ver[i] == 14) {
    save <- qs141::qsave
    read <- qs141::qread
  } else if(grid$ver[i] == 15) {
    save <- qs151::qsave
    read <- qs151::qread
  } else if(grid$ver[i] == 16) {
    save <- qs161::qsave
    read <- qs161::qread
  } else if(grid$ver[i] == 17) {
    save <- function(...) qs::qsave(..., check_hash = F)
    read <- qs::qread
  }
  time <- as.numeric(Sys.time())
  save(x, file, preset = grid$preset[i])
  write_time[i] <- 1000 * (as.numeric(Sys.time()) - time)
  rm(x); gc()
  time <- as.numeric(Sys.time())
  x <- read(file)
  read_time[i] <- 1000 * (as.numeric(Sys.time()) - time)
  rm(x); gc()
}

grid$write_time <- write_time
grid$read_time <- read_time

grid %>% group_by(data, ver, preset) %>%
  summarize(n=n(), mean_read_time = mean(read_time),
            median_read_time = median(read_time),
            mean_write_time = mean(write_time),
            median_write_time = median(write_time)) %>% as.data.frame
