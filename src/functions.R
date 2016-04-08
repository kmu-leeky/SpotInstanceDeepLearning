parseName <- function(name) {
  c(strsplit(name, "_")[[1]][1], strsplit(name, "_")[[1]][2])
}

getCompleteHistory <- function(folder) {
  history <- list()
  for(fname in list.files(folder)) {
    print(fname)
    tryCatch ({
      history[[fname]] <- addDiffTime(as.data.frame(read.table(paste(folder,"/",fname,sep=""))))
    }, error=function(e) {print(e)})
  }
  history
}
# This function returns the ratio of price where it is larger or smaller than the target price
getPricePortion <- function(offset) {
  library(hash)
  for(name in names(complete_history)) {
    rp <- values(regular_price, name) * offset
    price_history <- complete_history[[name]]
    lt <- sum(price_history[which(price_history$price<rp),4])
    gt <- sum(price_history[which(price_history$price>rp),4])
    ratio = as.numeric(gt) / (as.numeric(lt)+as.numeric(gt))
    print(paste(name, ratio))
  }
}

# get the price of spot instance divided by on-demand instance price
# In this function, when the spot instance price higer than the regular price,
# the spot instance price is substitued to the regular price
divideSpotPriceByOndemandPrice <- function() {
  library(hash)
  for(name in names(complete_history)) {
    rp <- values(regular_price, name) * offset
    price_history <- complete_history[[name]]
    total_time <- as.integer(sum(price_history$duration), unit="secs")
    price_history$price[price_history$price>rp] <- rp
    sp <- sum(as.integer(price_history$duration, unit="secs") * price_history$price)
    odp <- total_time * rp
    ratio = sp / odp
    print(paste(name, ratio))
  }
}

spotInstancePriceRatioFig <- function() {
  ggplot(output, aes(AZs, ratio, colour=InstanceTypes, shape=InstanceTypes)) +geom_point(size = 3) + theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5), legend.position="top") + labs(x="Availability Zones", y="Spot Instance Price Ratio to On-Demand")  + geom_point(colour="grey90", size = 1.5) + ylim(c(0.0, 0.5))
}

divideOnlySpotPriceByOndemand <- function() {
  library(hash)
  output <- data.frame(stringsAsFactors=FALSE)
  for(name in names(complete_history)) {
    rp <- values(regular_price, name)
    price_history <- complete_history[[name]]
    lower_total_time <- as.integer(sum(price_history[which(price_history$price<rp),4]), unit="secs")
    sp <- sum(as.integer(price_history[which(price_history$price<rp),4], unit="secs") * price_history[which(price_history$price<rp),1])
    odp <- lower_total_time * rp
    ratio = sp / odp
    if(is.nan(ratio)) ratio = 1.0
    parsed_name <- parseName(name)
    output <- rbind(output, data.frame(parsed_name[1], parsed_name[2], ratio, stringsAsFactors=FALSE))
  }
  colnames(output) <- c("AZs", "InstanceTypes", "ratio")
  output
}

# Get the ratio of the price comparing to the on-demand price
getPriceRatioToOndemand <- function() {
  library(hash)
  for(name in names(complete_history)) {
    rp <- values(regular_price, name) * offset
    price_history <- complete_history[[name]]
    rp <- regular_price[[name]]
    price_history$ondemandRatio <- price_history$price / rp
    price_history$timePortion <- as.integer(price_history$duration, unit="secs")/as.integer(sum(price_history$duration), unit="secs")
    complete_history[[name]] <- price_history
  }
  complete_history
}

drawWeightedCDF <- function() {
  library(spatstat)
  for(name in names(complete_history)) {
    price_history <- complete_history[[name]]
    quartz()
    plot(ewcdf(price_history$ondemandRatio, price_history$timePortion), ylim=c(0.0, 1.0), xlim=c(0.0, 1.5), main=name)
    abline(v=1.0)
    title(name)
  }
}

getConsecutiveTimeStat <- function(offset=1.0) {
  library(hash)
  for(name in names(complete_history)) {
    rp <- values(regular_price, name) * offset
    price_history <- complete_history[[name]]
    lt <- sum(price_history[which(price_history$price<rp),4])
    gt <- sum(price_history[which(price_history$price>rp),4])
    ratio = as.numeric(gt) / (as.numeric(lt)+as.numeric(gt))
    
    consecutive_available = 0
    avail_times = numeric()
    paid_price = numeric()
    paid_time = numeric()
    for(i in 1:nrow(price_history)) {
      time_in_secs <- as.numeric(price_history[i, "duration"], unit="secs")
      if(price_history[i, "price"] < rp) {
        consecutive_available <- consecutive_available + time_in_secs
        paid_price[length(paid_price)+1] <- time_in_secs * as.numeric(price_history[i, "price"])
        paid_time[length(paid_time)+1] <- time_in_secs
      } else {
        if (consecutive_available > 0) {
          avail_times[length(avail_times)+1] <- consecutive_available
        }
        consecutive_available <- 0
      }
    }
    print(paste(name, ratio, min(avail_times), max(avail_times),  mean(avail_times), nrow(price_history), length(avail_times), rp, (sum(paid_price)/sum(paid_time))))
  }
}

genRandomDateTime <- function(st="2015-12-19T11:02:26+0900", et="2016-03-17T13:53:23+0900") {
  set.seed(Sys.time()+Sys.getpid())
  startTime = strptime(st, "%Y-%m-%dT%H:%M:%OS")
  timeDiff <- as.numeric(strptime(et, "%Y-%m-%dT%H:%M:%OS") - startTime, unit="secs")
  return (startTime + runif(1, 0, timeDiff))
}

# This returns a workload character. The first two element shows the base workload (number of iteration and time per iteration)
# The third and fourth fields represent the remaining workload
genWorkload <- function(num_iteration=(5:30), iteration_time=(60:1800)) {
  n_iter = sample(num_iteration, 1)
  t_iter = sample(iteration_time, 1)
  c(n_iter, t_iter, n_iter-1, t_iter, 0)
}

processWorkload <- function(exec_time, workload) {
  total_remain <- (workload[2]*workload[3] + workload[4] - exec_time)
#  print(paste("execution time", exec_time, "workloads", workload[1], workload[2],workload[3],workload[4], "remaining after execution", total_remain))
  if(total_remain <= 0) {
    remain_workload <- c(workload[1], workload[2], 0, 0, total_remain)
  } else {
    remain_workload <- c(workload[1], workload[2], as.integer(total_remain/workload[2]), total_remain%%workload[2], exec_time)
 }
 remain_workload
}

onDemandOnly <- function (ph, phIndex, tp, rp, ct, wl) {
  c(rp * wl[1] * wl[2] / 3600, wl[1] * wl[2], 0)
}

resetIteration <- function(workload) {
  c(workload[1], workload[2], workload[3], workload[2], workload[5])
}

# with checkpointing per iteration
spotInstanceOndemandMixture <- function(ph, phIndex, tp, rp, ct, wl) {
  consumed_price = 0
  consumed_time = 0
  total_switch = 0
  current_mode = "init"
  for (i in phIndex:1) {
    end_time = strptime(ph[i, "effectiveUntil"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - ct, unit="secs"))
    if (tp >= ph[i, "price"]) {
      unit_price = ph[i, "price"]
      if (current_mode == "spot") {
        wl = processWorkload(exec_time, wl)
      } else {
        total_switch = total_switch + 1
        wl = processWorkload(exec_time, resetIteration(wl))
        current_mode = "spot"
      }
    } else {
      unit_price = rp
      if (current_mode == "ondemand") {
        wl = processWorkload(exec_time, wl)
      } else {
        wl = processWorkload(exec_time, resetIteration(wl))
        total_switch = total_switch + 1
        current_mode = "ondemand"
      }
    }
#    print(paste("Processed workload", wl[1], wl[2], wl[3], wl[4], wl[5], exec_time, "current spot price is", ph[i, "price"]))
    if (wl[5] <= 0) {
#      print("Processing is over")
      consumed_price = consumed_price + (unit_price * (exec_time + wl[5]) / 3600)
      consumed_time = consumed_time + exec_time + wl[5]
      break
    }
    consumed_price = consumed_price + (unit_price * exec_time / 3600)
    consumed_time = consumed_time + exec_time
    ct = end_time
  }
  c(consumed_price, consumed_time, (total_switch-1))
}

# no checkpointing
spotInstanceOnDemandAlways <- function(ph, phIndex, tp, rp, ct, wl) {
  consumed_price = 0
  consumed_time = 0
  total_switch = 0
  in_spot_instance = 0
  for (i in phIndex:1) {
    if(tp >= ph[i, "price"]) {  # bid successful
      in_spot_instance = 1
      end_time = strptime(ph[i, "effectiveUntil"], "%Y-%m-%dT%H:%M:%OS")
      exec_time = round(as.numeric(end_time - ct, unit="secs"))
      wl = processWorkload(exec_time, wl)
#      print(paste("Processed workload", wl[1], wl[2], wl[3], wl[4], wl[5], exec_time, "current spot price is", ph[i, "price"]))
      if (wl[5] <= 0) {
#        print("Processing is over")
        consumed_price = consumed_price + (ph[i, "price"] * (exec_time + wl[5]) / 3600)
        consumed_time = consumed_time + exec_time + wl[5]
        break
      }
      ct = end_time
      consumed_time = consumed_time + exec_time
      consumed_price = consumed_price + (ph[i, "price"] * exec_time / 3600)
#      print(paste("current consumed price is", consumed_price))
    } else {
#      print(paste("outbid target price", tp, "regular price", rp, "current price", ph[i, "price"]))
      if (in_spot_instance > 0) {
        total_switch = 1
      }
      consumed_price = consumed_price + (rp * wl[1] * wl[2] / 3600)
      consumed_time = consumed_time + (wl[1] * wl[2])
      break
    }
  }
  c(consumed_price, consumed_time, total_switch)
}
library(hash)
activities <- hash()
.set(activities, "spotInstanceOnDemandAlways", spotInstanceOnDemandAlways)
.set(activities, "spotInstanceOndemandMixture", spotInstanceOndemandMixture)
.set(activities, "onDemandOnly", onDemandOnly)

runSimulation <- function(submit_time, workload, key, price_offset, output="") {
  library(hash)
  rp <- values(regular_price, key)
  target_price <- rp * price_offset
  price_history <- complete_history[[key]]
  current_time <- submit_time
#  print(workload)
  for (i in 1:nrow(price_history)) {
    ef = strptime(price_history[i, "effectiveFrom"], "%Y-%m-%dT%H:%M:%OS")
    eu = strptime(price_history[i, "effectiveUntil"], "%Y-%m-%dT%H:%M:%OS")
    if (submit_time > ef && submit_time < eu) {
      for(k in keys(activities)) {
        func = values(activities, k)
        stats = func[[1]](price_history, i, target_price, rp, current_time, workload)
        if(length(output) > 0) {
          cat(paste(key, workload[1], workload[2], k, stats[1], stats[2], stats[3]), file=output, append=TRUE, sep = "\n")
        } else {
          print(paste(key, workload[1], workload[2], k, stats[1], stats[2], stats[3]))
        }
      }
      break
    }
  }
}

simulationAcrossRegions <- function(numRegion=length(names(complete_history)), price_offset=1.0, output="") {
  submit_time <- genRandomDateTime()
  workload <- genWorkload()

  for (i in 1:numRegion) {
    region_key = names(complete_history)[i]
    tryCatch({
      runSimulation(submit_time, workload, region_key, price_offset, output)
    }, error = function(e) {print(e)})
    gc()
  }
}

runFromScratch <- function(n) {
  for (i in 1:n) {
    simulationAcrossRegions(output="output.out")
  }
}
