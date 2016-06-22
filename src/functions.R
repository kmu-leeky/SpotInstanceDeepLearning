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
  output <- data.frame(stringsAsFactors=FALSE)
  for(name in names(complete_history)) {
    rp <- values(regular_price, name) * offset
    price_history <- complete_history[[name]]
    lt <- sum(price_history[which(price_history$price<rp),4])
    gt <- sum(price_history[which(price_history$price>rp),4])
    ratio = as.numeric(lt) / (as.numeric(lt)+as.numeric(gt))
    parsed_name <- parseName(name)
    output <- rbind(output, data.frame(parsed_name[1], parsed_name[2], ratio, stringsAsFactors=FALSE))
  }
  colnames(output) <- c("AZs", "InstanceTypes", "ratio")
  output
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
getPriceRatioToOndemand <- function(complete_history) {
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

genRandomDateTime <- function(st="2015-12-19T11:02:26+0900", et="2016-05-17T13:53:23+0900") {
  set.seed(Sys.time()+Sys.getpid())
  startTime = strptime(st, "%Y-%m-%dT%H:%M:%OS")
  timeDiff <- as.numeric(strptime(et, "%Y-%m-%dT%H:%M:%OS") - startTime, unit="secs")
  return (startTime + runif(1, 0, timeDiff))
}

# This returns a workload character. The first two element shows the base workload (number of iteration and time per iteration)
# The third and fourth fields represent the remaining workload
genWorkload <- function(num_iteration=(40:100), iteration_time=(60:1800), intensity="all") {
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

getLowestPriceAz <- function(entries, prev_running_az="") {
  min_azs = names(entries)[which(entries==min(entries[,2:length(entries)]))]
  if(prev_running_az %in% min_azs) {
    return (prev_running_az)
  } else {
    return (min_azs[1])
  }
}

spotInstanceAcrossRegionsUntilInterruption <- function(ph, phIndex, tp, rp, ct, wl) {
  consumed_price = 0
  consumed_time = 0
  total_switch = 0
  in_spot_instance = 0
  azs = colnames(ph)
  running_az = getLowestPriceAz(ph[phIndex,])
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  for(i in phIndex:2) {
    if (is.na(running_az)) {
      running_az = getLowestPriceAz(ph[i,])
    }
    cur_price = ph[i, running_az]
    if(tp < cur_price) {   # interruption find new spot instance
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
#        print(paste("interrupt from si", i, running_az, cur_price, total_switch))
      }
      running_az = getLowestPriceAz(ph[i,])
      cur_price = ph[i, running_az]
      if (tp < cur_price) { # there is no spot instance, use on-demand rp
        running_az = NA
        cur_price = rp
        in_spot_instance = 0
#        print(paste("there is no si using od", i, running_az, cur_price, total_switch))
      } else {
#        print(paste("got a new si", i, running_az, cur_price, total_switch))
        in_spot_instance = 1
      }
    } else {
      if (in_spot_instance == 0) {
        total_switch = total_switch + 1
#        print(paste("switch from od to si", i, running_az, cur_price, total_switch))
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)

    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] / 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
  }
  c(consumed_price, consumed_time, total_switch)
}

getIndexShouldMigrateWindow <- function(ph, phIndex, tp, rp, ct, wl, minGainTh) {
  total_required_time = wl[1] * wl[2]
}

getIndexShouldMigrateSequential <- function(ph, phIndex, tp, rp, ct, wl, minGainTh) {
  total_cost_savings = numeric()
  prev_running_az = ""
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  cur_az = ""
  az_before_interrupt = ""
  price_no_interrupt = 0
  time_before_switch = 0
  cost_saving_switch = 0.0
  interrupted = FALSE
  prev_interrupt_index = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  for(i in phIndex:2) {
    interrupted = FALSE
    running_az = getLowestPriceAz(ph[i,], prev_running_az)
    cur_price = ph[i, running_az]
    if (tp < cur_price) {  # should use on-demand instance as cur_price is the lowest one
      running_az = ""
      cur_price = rp
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
        interrupted = TRUE
      }
      in_spot_instance = 0
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
        interrupted = TRUE
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)

    if(interrupted == TRUE) {
      print(paste("time before switch is", time_before_switch, "cost savings", cost_saving_switch, i, "before interrupt:", az_before_interrupt, "prev az:", prev_running_az, "current az:", running_az, ph[i, prev_running_az], cur_price))
      if(length(cost_saving_switch) >0 && cost_saving_switch > minGainTh) {
        total_cost_savings = c(total_cost_savings, prev_interrupt_index)
      }
      prev_interrupt_index = i
      time_before_switch = 0
      cost_saving_switch = 0.0
      az_before_interrupt = prev_running_az
    }
    price_no_interrupt = ph[i, az_before_interrupt]
    time_before_switch = time_before_switch + exec_time
    cost_saving_switch = cost_saving_switch + exec_time * (price_no_interrupt - cur_price)/3600.0
    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] / 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
    prev_running_az = running_az
  }
  print(c(consumed_price, consumed_time, total_switch))
  c(c(consumed_price, consumed_time, total_switch), total_cost_savings)
}


spotInstanceBestPrice <- function(ph, phIndex, tp, rp, ct, wl, shouldMigrateIndex=NA) {
  prev_running_az = ""
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  for(i in phIndex:2) {
    if (is.na(shouldMigrateIndex) || i==phIndex || i %in% shouldMigrateIndex) {
      running_az = getLowestPriceAz(ph[i,], prev_running_az)
    } else {
      running_az = prev_running_az
    }

    cur_price = ph[i, running_az]
    print(paste(running_az, i, cur_price))
    if (tp < cur_price) {  # should use on-demand instance as cur_price is the lowest one
      running_az = ""
      cur_price = rp
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
      }
      in_spot_instance = 0
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)

    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] / 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
    prev_running_az = running_az
  }
  c(consumed_price, consumed_time, total_switch)
}

library(hash)
activities <- hash()
.set(activities, "spotInstanceOnDemandAlways", spotInstanceOnDemandAlways)
.set(activities, "spotInstanceOndemandMixture", spotInstanceOndemandMixture)
.set(activities, "onDemandOnly", onDemandOnly)
.set(activities, "spotInstanceAcrossRegionsUntilInterruption", spotInstanceAcrossRegionsUntilInterruption)
#.set(activities, "spotInstanceAcrossRegionsOnDemandMigration", spotInstanceAcrossRegionsOnDemandMigration)

runSpotInstanceSimAcrossRegion <- function(all_price_table, submit_time, workload, price_offset, output="") {
  current_time <- submit_time
  index = which(all_price_table$times==submit_time)
  print(paste("index is ", index))
  stat = spotInstanceAcrossRegionsUntilInterruption(all_price_table, index, 0.65, 0.65, current_time, workload)
  print(paste(submit_time, workload[1], workload[2], stat[1], stat[2], stat[3]))

#  stat = spotInstanceBestPrice(all_price_table, index, 0.65, 0.65, current_time, workload)
  stat = getIndexShouldMigrate(all_price_table, index, 0.65, 0.65, current_time,        workload, 0.015)
  print(paste(submit_time, workload[1], workload[2], stat[1], stat[2], stat[3]))
  stat <- stat[-c(1:3)]
  stat = spotInstanceBestPrice(all_price_table, index, 0.65, 0.65, current_time,       workload, stat)
  print(paste(submit_time, workload[1], workload[2], stat[1], stat[2], stat[3]))

}
siSim <- function(all_pt, price_offset=1.0, output="") {
  cand_time = all_pt$times 
  for ( i in (1:100)) {
#    submit_time <- genRandomDateTime()
    submit_time = cand_time[as.integer(runif(1, 100000, 500000))]
    workload <- genWorkload()


    tryCatch({
      runSpotInstanceSimAcrossRegion(all_pt, submit_time, workload, price_offset)
    }, error = function(e) {print(e)})
    gc()
  }
}

runSimulationPerRegion <- function(submit_time, workload, key, price_offset, output="") {
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

simulationAcrossRegions <- function(complete_history, price_offset=1.0, output="") {
  submit_time <- genRandomDateTime()
  workload <- genWorkload()
  numRegion=length(names(complete_history))

  for (i in 1:numRegion) {
    region_key = names(complete_history)[i]
    tryCatch({
      runSimulationPerRegion(submit_time, workload, region_key, price_offset, output)
    }, error = function(e) {print(e)})
    gc()
  }
}

runFromScratch <- function(n) {
  for (i in 1:n) {
    simulationAcrossRegions(output="output.out")
  }
}

getComparePic <- function(instanceType) {
  library(ggrepel)
  ggplot(subset(oneshot, InstanceTypes==instanceType), aes(spotToOndemandFromOne, spotLower)) + geom_point(size = 3) + labs(x="Cost Efficiency", y="System Availability")  + geom_point(colour="grey90", size = 1.5) + geom_label_repel(data=subset(oneshot, InstanceTypes==instanceType), mapping=aes(x=spotToOndemandFromOne, y=spotLower, label=AZs), box.padding = unit(0.5, "lines"), point.padding = unit(0.0, "lines"), fontface = 'bold', color = 'blue') +theme(legend.position="none")
}

getConsecutiveAvailableTime <- function(offset = 1.0) {
  library(hash)
  output <- data.frame(stringsAsFactors=FALSE)
  for(name in names(complete_history)) {
    rp <- values(regular_price, name) * offset
    price_history <- complete_history[[name]]
    cons_avail = 0
    parsed_name <- parseName(name)
    for (i in 1:nrow(price_history)) {
      entry = price_history[i,]
      if (entry$price < rp) {
        cons_avail = cons_avail + as.integer(entry$duration, unit="secs")
      } else {
        if(cons_avail > 0) {
          if(nrow(output) == 0) {
            output <- rbind(output, data.frame(parsed_name[1], parsed_name[2],cons_avail, stringsAsFactors=FALSE))
          } else {
            output[nrow(output)+1, ] = c(parsed_name[1], parsed_name[2], cons_avail)
          }
          cons_avail = 0
        }
      }
    }
  }
  colnames(output) <- c("AZs", "InstanceTypes", "consecutive_available")
  output
}

getConsecutiveAvailableCount <- function() {
  output <- data.frame(table(consecutive_available$AZs, consecutive_available$InstanceTypes))
  colnames(output) <- c("AZs", "InstanceTypes", "Freq")
}


#ggplot(subset(oneshot, InstanceTypes=="g2.2xlarge"), aes(spotToOndemandFromOne, spotLower)) + geom_point(size = 3) + labs(x="Cost Efficiency", y="Spot Instance Availability")  + geom_point(colour="grey90", size = 1.5) + geom_label_repel(data=subset(oneshot, InstanceTypes=="g2.2xlarge"), mapping=aes(x=spotToOndemandFromOne, y=spotLower, label=AZs), box.padding = unit(0.5, "lines"), point.padding = unit(0.1, "lines"), fontface = 'bold', color = 'blue') +theme(legend.position="none")

#ggplot(subset(oneshot, InstanceTypes=="g2.8xlarge"), aes(spotToOndemandFromOne, spotLower)) + geom_point(size = 3) + labs(x="Cost Efficiency", y="Spot Instance Availability")  + geom_point(colour="grey90", size = 1.5) + geom_label_repel(data=subset(oneshot, InstanceTypes=="g2.8xlarge"), max.iter=20000, mapping=aes(x=spotToOndemandFromOne, y=spotLower, label=AZs), box.padding = unit(0.5, "lines"), point.padding = unit(0.01, "lines"), fontface = 'bold', color = 'blue') +theme(legend.position="none") + xlim(0, 1.0) + ylim(0, 1.0)

mergeLogs <- function(paths) {
  library(hash)
  aggr_logs = hash()
  for(fname in list.files(paths[1])) {
    .set(aggr_logs, fname, hash())
  }
  for(p in paths) {
    for(fname in list.files(p)) {
      aggr_log = aggr_logs[[fname]]
      lines <- readLines(file(paste(p,"/",fname,sep=""),open="r"))
      for(line in lines) {
        ls = strsplit(line, "\t")
        if(has.key(ls[[1]][2], aggr_log) == FALSE) {
          .set(aggr_log, ls[[1]][2], ls[[1]][1])
        }
      }
      .set(aggr_logs, fname, aggr_log)
    }
  }
  aggr_logs
}

writeAggrLogsToFile <- function(aggr_logs, dest_folder) {
  for(az in keys(aggr_logs)) {
    filename=paste(dest_folder, "/", az, sep="")
    aggr_log = aggr_logs[[az]]
    if(is.null(aggr_log)) {
      next
    }
    for(k in rev(keys(aggr_log))) {
      cat(paste(aggr_log[[k]],"\t",k,sep=""), file=filename, append=TRUE, sep="\n")
    }
  }
}
buildAllPriceTable <- function(complete_history) {
  times = sort(unique(unlist(lapply(complete_history, function(x) as.character(x$effectiveFrom)))), TRUE)
  all_price_table <- data.frame(times, stringsAsFactors=FALSE)
  rlen = length(rownames(all_price_table))
  for(name in names(complete_history)) {
    ph <- complete_history[[name]]
    rph = length(rownames(ph))
    all_prices = vector(length=rlen)
    cur_index = 1
    print(name)
    for(i in (1:rph)) {
      cur_time = ph[i,]$effectiveFrom
      cur_price = ph[i,]$price
      for(j in (cur_index:rlen)) {
        all_prices[j] = cur_price
        cur_index = cur_index + 1
        if(cur_time == all_price_table[j,"times"]) {
          break
        }
      }
    }
    all_price_table[,name] <- all_prices
  }
  all_price_table
}

convertStringToSeconds <- function(time_string) {
  as.numeric(strptime(time_string, "%Y-%m-%dT%H:%M:%OS"), unit="secs")
}

buildAllPaymentTable <- function(all_price_table, od_price = 0.65) {
  azs = names(all_price_table)[-c(1)]
  rlen = length(rownames(all_price_table))
  od_payments = vector(length=rlen)
  elapsed_hours = vector(length=rlen)
  end_time_secs = convertStringToSeconds(all_price_table[1,"times"])
  payment_table = as.data.frame(matrix(0,ncol=length(azs)+1, nrow=rlen))
  for (i in 1:rlen) {
    start_time_secs = convertStringToSeconds(all_price_table[i,"times"])
    elapsed_time_hrs = (end_time_secs - start_time_secs)/3600.0
    od_payments[i] = elapsed_time_hrs * od_price
    elapsed_hours[i] = elapsed_time_hrs
    end_time_secs = start_time_secs
  }
  for(az in azs) {
    az_payment = vector(length=rlen)
    for(i in 1:rlen) {
      az_payment[i] = all_price_table[i, az] * elapsed_hours[i]
    }
    print(az)
    payment_table[, az] = az_payment
  }
  payment_table[, "ondemand"] = od_payments
  payment_table
}

time_windows=c(3600, 21600, 43200, 86400, 259200, 604800, 1209600)

buildPriceRelationHash <- function(azs, tws, num_entry=10000) {
  price_relation_hash <- hash()
  for(az in azs) {
    h = hash()
    for(old in tws) {
      for(new in tws) {
        key = paste(old, "-", new, sep="")
        .set(h, key, matrix(ncol=2, nrow=num_entry))
      }
    }
    .set(price_relation_hash, az, h)
  }
  price_relation_hash
}

getPriceWindow <- function(price_relation, all_pt, time_windows, time_to_secs=time_to_secs, max_od=TRUE, rp="ondemand", avg_out=TRUE, num_sample=10000) {
  start = nrow(all_pt) - as.integer(nrow(all_pt) * 0.8)
  end = nrow(all_pt) - as.integer(nrow(all_pt) * 0.2)
  azs = colnames(all_pt)[1:(ncol(all_pt)-2)] # remove the first column (times)
  for(ns in 1:num_sample) {
    if((ns %% 10) == 0) print(ns)
    submit_time = all_pt[as.integer(runif(1, start, end)), "times"]
    index = which(all_pt$times==submit_time)
    newer_tws = getIndexOfTimeDiff(time_to_secs, index, time_windows, 1)
    older_tws = getIndexOfTimeDiff(time_to_secs, index, time_windows, -1)
    iterateWindows(price_relation, all_pt, newer_tws, time_windows, azs, index, ns, 1, max_od, avg_out, rp)
    iterateWindows(price_relation, all_pt, older_tws, time_windows, azs, index, ns, -1, max_od, avg_out, rp)
  }
}
# getPriceWindow(price_relation, all_pt_g2_2x, time_windows, time_to_secs, TRUE, 0.65, TRUE, 10000)
# time_to_secs = convertTimeToSeconds(all_pt_g2_2x)
iterateWindows <- function(price_relation, all_pt, window_itr, time_windows, azs, base_index, cur_index, dir, max_od, avg_out, rp) {
  col_index = ifelse(dir>0, 2, 1)
  for(wi in 1:length(window_itr)) {
    price_means = vector(length=length(azs))
    od_prices = all_pt[window_itr[wi]:base_index, rp]
    for(i in 1:length(azs)) {
      if(max_od) {
        
        mean_price = mean(pmin(all_pt[window_itr[wi]:base_index, azs[i]], od_prices))
      } else {
        mean_price = mean(all_pt[window_itr[wi]:base_index, azs[i]])
      }
      price_means[i] = mean_price
    }
    if(avg_out) {
      pm = mean(price_means)
      price_means = price_means - pm
    }
    for(i in 1:length(azs)) {
      for(tw in time_windows) {
        if(dir > 0) {
          time_key=paste(tw,"-",time_windows[wi],sep="")
        } else {
          time_key=paste(time_windows[wi],"-",tw,sep="")
        }
        price_relation[[azs[i]]][[time_key]][cur_index, col_index] = price_means[i]
      }
    }
  }
}

iteratePriceRelationPerAzTw <- function(price_relation) {
  azs = names(price_relation)
  for(az in azs) {
    print(az)
    tws = names(price_relation[[az]])
    for(tw in tws) {
      print(paste(tw, cor(price_relation[[az]][[tw]])[1,2]))
    }
  }
}

iteratePriceRelationPerTw <- function(price_relation, time_windows=time_windows) {
  azs = names(price_relation)
  tws = names(price_relation[[azs[1]]])
  l = length(time_windows)
  out_matrix = matrix(nrow=l, ncol=l)
  rownames(out_matrix) = time_windows
  colnames(out_matrix) = time_windows
  for(tw in tws) {
    commands = "cor(rbind("
    for(az in azs) {
      commands = paste(commands, "price_relation[[\"", az, "\"]][[\"", tw, "\"]],", sep="")
    }
    commands = substr(commands, 1, nchar(commands)-1)
    commands = paste(commands, "))", sep="")
    output = eval(parse(text=commands))
    cor_coef = output[1,2]
    times = unlist(strsplit(tw,"-"))
    out_matrix[times[1], times[2]] = output[1,2]
#    print(paste(tw, output[1,2]))
  }
  out_matrix
}

convertTimeToSeconds <- function(all_pt) {
  sapply(all_pt[,"times"], function(x) as.numeric(strptime(x, "%Y-%m-%dT%H:%M:%OS"), unit="secs"))
}
getIndexOfTimeDiff <- function(time_secs, base_time_index, time_windows, direction) {
  bt = time_secs[base_time_index]
  new_index = vector(length=length(time_windows))
  for (i in 1:length(time_windows)) {
    target = bt + time_windows[i] * direction
    new_index[i] = unname(which.min(abs(time_secs - target)))[1]
  }
  new_index
}
