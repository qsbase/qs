suppressMessages(library(qs))
suppressMessages(library(dplyr))

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

# grid <- expand.grid(data = c("list", "dataframe"), 
#                     preset = c("fast", "balanced", "high", "archive"), 
#                     nt = c(1,4), reps=1:3, stringsAsFactors = F)
# grid <- grid %>% filter(! (preset == "archive" & nt == 4) )

grid <- expand.grid(data = c("list", "dataframe"), 
                    preset = c("fast", "balanced", "high"), 
                    nt = c(1,4), reps=1:25, stringsAsFactors = F)

write_time <- numeric(nrow(grid))
read_time <- numeric(nrow(grid))
for(i in 1:nrow(grid)) {
  print(i)
  if(grid$data[i] == "list") {
    x <- listGen()
  } else if(grid$data[i] == "dataframe") {
    x <- dataframeGen()
  }
  time <- as.numeric(Sys.time())
  qsave(x, file, preset = grid$preset[i], nthreads = grid$nt[i])
  write_time[i] <- 1000 * (as.numeric(Sys.time()) - time)
  rm(x); gc()
  time <- as.numeric(Sys.time())
  x <- qread(file, nthreads=grid$nt[i])
  read_time[i] <- 1000 * (as.numeric(Sys.time()) - time)
  rm(x); gc()
}

grid$write_time <- write_time
grid$read_time <- read_time

grid %>% group_by(data, preset, nt) %>%
  summarize(n=n(), mean_read_time = mean(read_time),
            median_read_time = median(read_time),
            mean_write_time = mean(write_time),
            median_write_time = median(write_time)) %>% as.data.frame
