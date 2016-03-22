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
  rp * wl[1] * wl[2] / 3600
}

spotInstanceOndemandMixture <- function(ph, phIndex, tp, rp, ct, wl) {
  consumed_price = 0
  for (i in phIndex:1) {
    end_time = strptime(ph[i, "effectiveUntil"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - ct, unit="secs"))
    wl = processWorkload(exec_time, wl)
    unit_price = if (tp >= ph[i, "price"])  ph[i, "price"] else  rp
#    print(paste("Processed workload", wl[1], wl[2], wl[3], wl[4], wl[5], exec_time, "current spot price is", ph[i, "price"]))
    if (wl[5] <= 0) {
#      print("Processing is over")
      consumed_price = consumed_price + (unit_price * (exec_time + wl[5]) / 3600)
      break
    }
    consumed_price = consumed_price + (unit_price * exec_time / 3600)
    ct = end_time
  }
  consumed_price
}

spotInstanceOnDemandAlways <- function(ph, phIndex, tp, rp, ct, wl) {
  consumed_price = 0
  for (i in phIndex:1) {
    if(tp >= ph[i, "price"]) {  # bid successful
      end_time = strptime(ph[i, "effectiveUntil"], "%Y-%m-%dT%H:%M:%OS")
      exec_time = round(as.numeric(end_time - ct, unit="secs"))
      wl = processWorkload(exec_time, wl)
#      print(paste("Processed workload", wl[1], wl[2], wl[3], wl[4], wl[5], exec_time, "current spot price is", ph[i, "price"]))
      if (wl[5] <= 0) {
#        print("Processing is over")
        consumed_price = consumed_price + (ph[i, "price"] * (exec_time + wl[5]) / 3600)
        break
      }
      ct = end_time
      consumed_price = consumed_price + (ph[i, "price"] * exec_time / 3600)
#      print(paste("current consumed price is", consumed_price))
    } else {
#      print(paste("outbid target price", tp, "regular price", rp, "current price", ph[i, "price"]))
      consumed_price = consumed_price + (rp * wl[1] * wl[2] / 3600)
      break
    }
  }
  consumed_price
}
library(hash)
activities <- hash()
.set(activities, "spotInstanceOnDemandAlways", spotInstanceOnDemandAlways)
.set(activities, "spotInstanceOndemandMixture", spotInstanceOndemandMixture)
.set(activities, "onDemandOnly", onDemandOnly)

runSimulation <- function(key="us-east-1a_g2.2xlarge_linux", price_offset=1.0) {
  library(hash)
  rp <- values(regular_price, key)
  target_price <- rp * price_offset
  submitTime <- genRandomDateTime()
  price_history <- complete_history[[key]]
  workload <- genWorkload()
  current_time <- submitTime
  print(workload)
  for (i in 1:nrow(price_history)) {
    ef = strptime(price_history[i, "effectiveFrom"], "%Y-%m-%dT%H:%M:%OS")
    eu = strptime(price_history[i, "effectiveUntil"], "%Y-%m-%dT%H:%M:%OS")
    if (submitTime > ef && submitTime < eu) {
      for(k in keys(activities)) {
        func = values(activities, k)
        totalPrice = func[[1]](price_history, i, target_price, rp, current_time, workload)
        print(paste(k, "total price paid", totalPrice, "consumed time in hours", workload[1]*workload[2]/3600))
      }
      break
    }
  }
}

