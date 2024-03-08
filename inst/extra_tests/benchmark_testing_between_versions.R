library(qs257)
library(qs)


library(dplyr)
library(patchwork)
library(ggplot2)


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

grid <- expand.grid(ver = c(25:26, "26attributes"), data = c("list", "dataframe"),
                    preset = c("uncompressed", "fast", "balanced", "high", "archive"),
                    reps=1:5, stringsAsFactors = F) %>% sample_frac(1)

write_time <- numeric(nrow(grid))
read_time <- numeric(nrow(grid))
for(i in 1:nrow(grid)) {
  print(i)
  if(grid$data[i] == "list") {
    x <- listGen()
  } else if(grid$data[i] == "dataframe") {
    x <- dataframeGen()
  }
  if(grid$ver[i] == "25") {
    save <- function(...) qs257::qsave(..., check_hash = F)
    read <- qs257::qread
  } else if(grid$ver[i] == "26") {
    save <- function(...) qs::qsave(..., check_hash = F)
    read <- qs::qread
  } else if(grid$ver[i] == "26attributes") {
    save <- function(...) qs::qsave(..., check_hash = F)
    read <- qs::qattributes
  }
  file <- tempfile()
  write_time[i] <- if(grid$preset[i] == "archive") {
    time <- as.numeric(Sys.time())
    save(x, file, preset = "custom", algorithm = "zstd_stream", compress_level=5)
    1000 * (as.numeric(Sys.time()) - time)
  } else if(grid$preset[i] == "high") {
    time <- as.numeric(Sys.time())
    save(x, file, preset = "custom", algorithm = "zstd", compress_level=5)
    1000 * (as.numeric(Sys.time()) - time)
  } else if(grid$preset[i] == "balanced") {
    time <- as.numeric(Sys.time())
    save(x, file, preset = "custom", algorithm = "lz4", compress_level=1)
    1000 * (as.numeric(Sys.time()) - time)
  } else if(grid$preset[i] == "fast") {
    time <- as.numeric(Sys.time())
    save(x, file, preset = "custom", algorithm = "lz4", compress_level=100)
    1000 * (as.numeric(Sys.time()) - time)
  } else if(grid$preset[i] == "uncompressed") {
    if(grid$ver[i] <= 17) next;
    time <- as.numeric(Sys.time())
    save(x, file, preset = "custom", algorithm = "uncompressed")
    1000 * (as.numeric(Sys.time()) - time)
  }
  rm(x); gc()
  time <- as.numeric(Sys.time())
  x <- read(file)
  read_time[i] <- 1000 * (as.numeric(Sys.time()) - time)
  rm(x); gc()
  unlink(file)
}

grid$write_time <- write_time
grid$read_time <- read_time

gs <- grid %>% group_by(data, ver, preset) %>%
  summarize(n=n(), mean_read_time = mean(read_time),
            median_read_time = median(read_time),
            mean_write_time = mean(write_time),
            median_write_time = median(write_time)) %>% as.data.frame
print(gs)


g1 <- ggplot(grid, aes(x = preset, fill = as.factor(ver), group=as.factor(ver), y = read_time)) +
  geom_bar(stat = "summary", fun.y = "mean", position = "dodge", color = "black") +
  geom_point(position = position_dodge(width=0.9), shape=21, fill = NA) +
  facet_wrap(~data, scales = "free", ncol=2) +
  theme_bw() + theme(legend.position = "bottom") +
  guides(fill = guide_legend(nrow = 1)) +
  trqwe:::gg_rotate_xlabels(angle=45, vjust=1) +
  labs(fill = "Version", title = "read benchmarks")

g2 <- ggplot(grid, aes(x = preset, fill = as.factor(ver),  group=as.factor(ver), y = write_time)) +
  geom_bar(stat = "summary", fun.y = "mean", position = "dodge", color = "black") +
  geom_point(position = position_dodge(width=.9), shape=21, fill = NA, alpha=0.5) +
  facet_wrap(~data, scales = "free", ncol=2) +
  theme_bw() + theme(legend.position = "bottom") +
  guides(fill = guide_legend(nrow = 1)) +
  trqwe:::gg_rotate_xlabels(angle=45, vjust=1) +
  labs(fill = "Version", title = "write benchmarks")

g <- g1 + g2 + plot_layout(nrow=2, ncol=1)
plot(g)
