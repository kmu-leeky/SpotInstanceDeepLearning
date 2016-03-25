library(plyr)

addWorkloadClass <- function(path) {
  input = data.frame()
  file_list <- list.files()
  for (f in file_list) {
    temp_input = as.data.frame(read.table(f))
    input <- rbind(input, temp_input)
  }
  input = within(input, {round=ceiling(V2/10)})
  input = within(input, {iteration=ceiling(V3/600)})
  colnames(input) <- c("region", "rnd", "itr", "type", "price", "time", "switch", "round", "iteration")
  input
}


getPriceMean <- function(input) {
  ddply(input, .(region, iteration, round, type), summarise, mean=mean(price))
}

addOndemandPriceRatio <- function(summary) {
  summary$price_ratio <- NA
  for(i in 1:nrow(summary)) {
    self_entry = summary[i,]
    compare_price = summary[(summary$type=="onDemandOnly")&(summary$iteration==self_entry$iteration)&summary$round==self_entry$round&summary$region==self_entry$region,]$mean
    summary[i,"price_ratio"] <- (self_entry$mean/compare_price)
  }
  summary
}
