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

getLowestPriceAz <- function(rindex, prev_running_az="", exclude_azs=NULL) {
  if(!is.null(exclude_azs)) {
    sorted_names = names(ph)[order(ph[rindex, 2:length(ph)])+1] # should make +1 as "times" column is excluded
    for(sn in sorted_names) {
      if(!has.key(sn, exclude_azs)) {
        return (sn)
      }
    }
    return(prev_running_az)
  } else {
    min_azs = names(ph)[which(ph[rindex,2:length(ph)]==min(ph[rindex, 2:length(ph)]))+1] #+1 as "times" excluded
    if(prev_running_az %in% min_azs) {
      return (prev_running_az)
    } else {
      return (min_azs[1])
    }
  }
}

spotInstanceAcrossRegionsUntilInterruption <- function(phIndex, tp, rp, ct, wl) {
  consumed_price = 0
  consumed_time = 0
  total_switch = 0
  in_spot_instance = 0
  azs = colnames(ph)
  running_az = getLowestPriceAz(phIndex)
  prev_running_az = running_az
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  migration_plan = hash()
  .set(migration_plan, as.character(phIndex), running_az)
  for(i in phIndex:2) {
    if (prev_running_az == "ondemand") {
      running_az = getLowestPriceAz(i)
    }
    cur_price = ph[i, running_az]
    if(tp < cur_price) {   # interruption find new spot instance
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
#        print(paste("interrupt from si", i, running_az, cur_price, total_switch))
      }
      running_az = getLowestPriceAz(i)
      cur_price = ph[i, running_az]
      if (tp < cur_price) { # there is no spot instance, use on-demand rp
        running_az = "ondemand"
        cur_price = rp
        in_spot_instance = 0
        if(prev_running_az != "ondemand") {
          .set(migration_plan, as.character(i), "ondemand")
        }
#        print(paste("there is no si using od", i, running_az, cur_price, total_switch))
      } else {
#        print(paste("got a new si", i, running_az, cur_price, total_switch))
        in_spot_instance = 1
        .set(migration_plan, as.character(i), running_az)
      }
    } else {
      if (in_spot_instance == 0) {
        total_switch = total_switch + 1
        .set(migration_plan, as.character(i), running_az)
#        print(paste("switch from od to si", i, running_az, cur_price, total_switch))
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)
    prev_running_az = running_az
    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] / 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
  }
  for(k in rev(keys(migration_plan))) {
    print(paste(k, migration_plan[[k]]))
  }
  list("stat"=c(consumed_price, consumed_time, total_switch), "migration_plan"=migration_plan)
}

getIndexShouldMigrateWindow <- function(ph, phIndex, tp, rp, ct, wl, minGainTh) {
  total_required_time = wl[1] * wl[2]
}

ph <<- all_pt_g2_2x
shouldNotMigrateIndex <<- NULL
setPriceTable <- function(pt) {
  ph <<- pt
}

mergeMigrationsDivideQunquer <- function (phIndex, wl, minGainTh) {
  setPriceTable(all_payment_g2_2x)
  migration_plan = spotInstanceBestPriceFromPaymentTable(phIndex, wl)[["migrate_summary"]]
  num_migration = nrow(migration_plan)
  max_step = ceiling(log(num_migration, 2))
  for(i in 1:max_step) {
    spacing = 2^i
    max_win_index = ceiling(num_migration/spacing)
    for (j in 1:max_win_index) {
      print(paste(i, j, ((j-1)*spacing+1), min((j*spacing),num_migration)))
      #migration_plan[((j-1)*spacing+1):min((j*spacing),num_migration),]
    }
  }
}


# res=getOptMigPlanRmLstGain(186660, 0.65, 0.65, wl, 0.015)
#
# From a best running scenario, remove transitions one by one that shows the least cost benefit
getOptMigPlanRmLstGain <- function (phIndex, tp, rp, wl, minGainTh) {
  loop_continue = TRUE
  shouldNotMigrateIndex <<- hash()
  org_wl = wl
  while(loop_continue) {
    min_cost_gain_amt = 1000000.0
    min_cost_gain_index = ""
    min_cost_gain_end_index = ""
    min_cost_gain_az=NA
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
    current_time = strptime(all_pt_g2_2x[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
    wl = org_wl
    migration_plan = hash()
    for(i in phIndex:2) {
      interrupted = FALSE
#      running_az_time = system.time(selectOptimalAz(i, prev_running_az, NA))
      running_az = selectOptimalAz(i, prev_running_az, NA)
#      running_az_time = system.time(getLowestPriceAz(i, prev_running_az))
#      running_az = getLowestPriceAz(i, prev_running_az)
#      print(paste("running_az_time", running_az_time, running_az))
#      running_az = getLowestPriceAz(i, prev_running_az)  
      cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
      if (tp <= cur_price) {  # should use on-demand instance as cur_price is the lowest one
        running_az = "ondemand"
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
  #      print(paste("time before switch is", time_before_switch, "cost savings", cost_saving_switch, i, "before interrupt:",             az_before_interrupt, "prev az:", prev_running_az, "current az:", running_az, ph[i, prev_running_az], cur_price))
# If the minimal cost gain region was ondemand, we cannot avoid it because there is no option other than using on-demand instances
        if(length(cost_saving_switch) >0 && min_cost_gain_amt > cost_saving_switch && cost_saving_switch > 0 && prev_running_az!="ondemand") {
          min_cost_gain_index = as.character(prev_interrupt_index)
          min_cost_gain_end_index = as.character((i+1))
          min_cost_gain_amt = cost_saving_switch
          min_cost_gain_az = prev_running_az
          print(paste("interrupt", min_cost_gain_amt, "start index", min_cost_gain_index, "end index", min_cost_gain_end_index))
        }
        prev_interrupt_index = i
        time_before_switch = 0
        cost_saving_switch = 0.0
        .set(migration_plan, as.character(i), running_az)
        az_before_interrupt = prev_running_az
      }
      if(az_before_interrupt == "ondemand") {
        price_no_interrupt = rp
      } else {
        price_no_interrupt = ifelse(length(ph[i, az_before_interrupt])>0 && ph[i, az_before_interrupt]<rp, ph[i, az_before_interrupt], rp)
      }

      time_before_switch = time_before_switch + exec_time
      cost_saving_switch = cost_saving_switch + exec_time * (price_no_interrupt - cur_price)/3600.0
 #           print(paste(i, az_before_interrupt, price_no_interrupt, running_az, cur_price,    exec_time))

      if (wl[5] <= 0) {
        consumed_price = consumed_price + (wl[5] / 3600)
        consumed_time = consumed_time + wl[5]
        break
      }

      current_time = end_time
      prev_running_az = running_az
    }
    if(min_cost_gain_amt > minGainTh) {
      loop_continue = FALSE
      print(paste("setting flag to false", min_cost_gain_amt))
    } else {
      print(paste("min_cost_gain_amt", min_cost_gain_amt, "min_cost_gain_index", min_cost_gain_index, "end index", min_cost_gain_end_index, "min_cost_gain_az", min_cost_gain_az))
      for (idx in min_cost_gain_index:min_cost_gain_end_index) {
        idx = as.character(idx)
        if(has.key(idx, shouldNotMigrateIndex)) {
          .set(shouldNotMigrateIndex[[idx]], min_cost_gain_az, min_cost_gain_amt)
        } else {
          h <- hash()
          .set(h, min_cost_gain_az, min_cost_gain_amt)
          .set(shouldNotMigrateIndex, idx, h)
        }
      }
    }
    mp_keys = keys(migration_plan)
    if(length(mp_keys) < 30) {
      for(k in mp_keys) {
        print(paste(k, migration_plan[[k]]))
      }
    }
    print(paste("number of migration:", length(migration_plan), "cost benefit", consumed_price))
  }
  for(k in rev(keys(migration_plan))) {
    print(paste(k, migration_plan[[k]]))
  }
  list("stat"=c(consumed_price, consumed_time, total_switch), "migration_plan" = migration_plan)  
}

# Assuming each switch(migration) is independent, get indexes of migrations whose cost gain is larger than minGainTh
# As it simply counts the cost gain per each transition, the value is not always current
getIndexShouldMigrateSequential <- function(phIndex, tp, rp, wl, minGainTh) {
  setPriceTable(all_pt_g2_2x)
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
    running_az = getLowestPriceAz(i, prev_running_az)
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
#    print(paste("exec_time", exec_time, "wl", wl))
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
  list("stat"=c(consumed_price, consumed_time, total_switch), "should_migrate_indexes"=total_cost_savings)
}

selectOptimalAz <- function(i, prev_running_az, shouldMigrateIndex=NA) {
  if (is.null(shouldNotMigrateIndex) && is.na(shouldMigrateIndex)) {
    running_az = getLowestPriceAz(i, prev_running_az)
  } else if (!is.null(shouldNotMigrateIndex)) {
    i_key = as.character(i)
    if(has.key(i_key, shouldNotMigrateIndex)) {
#      running_az = getLowestPriceAz(i, prev_running_az, shouldNotMigrateIndex[[i_key]])
      #disable migration in the instance
      running_az = prev_running_az
    } else {
      running_az = getLowestPriceAz(i, prev_running_az)
    }
  } else if (!is.na(shouldMigrateIndex)){
    if(i %in% shouldMigrateIndex) {
      running_az = getLowestPriceAz(i, prev_running_az)
    } else {
      running_az = prev_running_az
    }
  }

  running_az
}

spotInstanceBestPriceFromPaymentTable <- function(phIndex, wl) {
  setPriceTable(all_payment_g2_2x)
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  prev_running_az = getLowestPriceAz(phIndex, "ondemand")
  migrate_indexes = vector()
  migrate_azs = vector()
  migrate_indexes = append(migrate_indexes, phIndex)
  migrate_azs = append(migrate_azs, prev_running_az)

  for(i in phIndex:2) {
    running_az = selectOptimalAz(i, prev_running_az)
    cur_payment = ph[i, running_az]
#    print(paste(running_az, i, cur_price))
    if (running_az == "ondemand") {  # should use on-demand instance as cur_price is the lowest   one
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
        migrate_indexes = append(migrate_indexes, i)
        migrate_azs = append(migrate_azs, running_az)
      }
      in_spot_instance = 0
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
        migrate_indexes = append(migrate_indexes, i)
        migrate_azs = append(migrate_azs, running_az)
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + cur_payment

    if (wl[5] <= 0) {
      consumed_price = consumed_price + 0.65*(wl[5] / 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
    prev_running_az = running_az
  }
  migrate_summary = data.frame(migrate_indexes, migrate_azs)
  list("stat"=c(consumed_price, consumed_time, total_switch),                           "migrate_summary"=migrate_summary)
}

spotInstanceBestPrice <- function(phIndex, tp, rp, wl, shouldMigrateIndexes=NA, shouldNotMigrateIndexes=NULL) {
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  prev_running_az = getLowestPriceAz(phIndex, "")
  migrate_indexes = vector()
  migrate_azs = vector()
  migrate_benefit = vector()

  for(i in phIndex:2) {
    running_az = selectOptimalAz(i, prev_running_az, shouldMigrateIndexes)
    cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
#    print(paste(running_az, i, cur_price))
    if (tp < cur_price) {  # should use on-demand instance as cur_price is the lowest one
      running_az = "ondemand"
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
  migrate_summary = data.frame(migrate_indexes, migrate_azs, migrate_benefit)
  list("stat"=c(consumed_price, consumed_time, total_switch), "migrate_summary"=migrate_summary)
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
  result = getIndexShouldMigrateSequential(all_price_table, index, 0.65, 0.65, current_time,        workload, 0.015)
  stat = result[["stat"]]
  print(paste(submit_time, workload[1], workload[2], stat[1], stat[2], stat[3]))
  result = spotInstanceBestPrice(all_price_table, index, 0.65, 0.65, current_time, workload, result[["should_migrate_indexes"]])
  stat = result[["stat"]]
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

time_windows=c(0, 3600, 21600, 43200, 86400, 259200, 604800, 1209600)

#ml_dataset = buildMlDataset(azs, time_windows)
buildMlDataset <- function(azs, tws, num_entry=10000) {
  price_relation_hash <- hash()
  n_windows = length(tws) 
  for(az in azs) {
    h = hash()
    .set(h, "before", matrix(ncol=n_windows, nrow=num_entry))
    .set(h, "after", matrix(ncol=n_windows, nrow=num_entry))
    .set(price_relation_hash, az, h)
  }
  price_relation_hash
}

#price_relation = buildPriceRelationHash(azs, time_windows)
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

getPriceWindow <- function(pr_str, all_pt_str, time_windows, time_to_secs=time_to_secs, max_od=TRUE, rp="ondemand", avg_out=TRUE, num_sample=10000) {
  price_relation = eval(parse(text=pr_str))
  all_pt = eval(parse(text=all_pt_str))
  if(num_sample %% 4 != 0) {
    stop("The number of samples should be divided by four")
  }
  train_start = nrow(all_pt) - as.integer(nrow(all_pt) * 0.5)
  train_end = nrow(all_pt) - as.integer(nrow(all_pt) * 0.2)
  eval_start = nrow(all_pt) - as.integer(nrow(all_pt) * 0.8)
  eval_end = nrow(all_pt) - as.integer(nrow(all_pt) * 0.6)
  train_sample = num_sample * 0.75
  azs = colnames(all_pt)[2:(ncol(all_pt)-1)] # remove the first column (times)
  for(ns in 1:num_sample) {
    if((ns %% 10) == 0) print(ns)
    if (ns < train_sample) {
      submit_time = all_pt[as.integer(runif(1, train_start, train_end)), "times"]
    } else {
      submit_time = all_pt[as.integer(runif(1, eval_start, eval_end)), "times"]
    }
    index = which(all_pt$times==submit_time)
    newer_tws = getIndexOfTimeDiff(time_to_secs, index, time_windows, 1)
    older_tws = getIndexOfTimeDiff(time_to_secs, index, time_windows, -1)
#    iterateWindowsCor(pr_str, all_pt_str, newer_tws, time_windows, azs, index, ns, 1, max_od, avg_out, rp)
#    iterateWindowsCor(pr_str, all_pt_str, older_tws, time_windows, azs, index, ns, -1, max_od, avg_out, rp)
    iterateWindowsMl(pr_str, all_pt_str, newer_tws, time_windows, azs, index, ns, "after", max_od, avg_out, rp)
    iterateWindowsMl(pr_str, all_pt_str, older_tws, time_windows, azs, index, ns, "before", max_od, avg_out, rp)
  }
}

# getPriceWindow("ml_dataset", "all_payment_g2_2x", time_windows, time_to_secs, TRUE, 0.65, TRUE, 10000)
# getPriceWindow("price_relation", "all_payment_g2_2x", time_windows, time_to_secs, TRUE, 0.65, TRUE, 10000)
# time_to_secs = convertTimeToSeconds(all_pt_g2_2x)
iterateWindowsCor <- function(pr_str, all_pt_str, window_itr, time_windows, azs, base_index, cur_index, dir, max_od, avg_out, rp) {
  price_relation = eval(parse(text=pr_str))
  all_pt = eval(parse(text=all_pt_str))
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

iterateWindowsMl <- function(pr_str, all_pt_str, window_itr, time_windows, azs, base_index, cur_index, key_bfr_aftr, max_od, avg_out, rp) {
  price_relation = eval(parse(text=pr_str))
  all_pt = eval(parse(text=all_pt_str))
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
      price_relation[[azs[i]]][[key_bfr_aftr]][cur_index, wi] = price_means[i]
    }
  }
}

#linearRegressionAcrossAzs("ml_dataset", time_windows)
linearRegressionAcrossAzs <- function(ml_data_string, time_windows) {
  ml_dataset = eval(parse(text=ml_data_string))
  tw_len = length(time_windows)
  azs = names(ml_dataset)
  x_key = "before"
  y_key = "after"
  total_num_sample = nrow(ml_data_short[[names(ml_data_short)[1]]][["after"]])
  train_end = total_num_sample * 0.75
  models <<- hash()
  for (az in azs) {
    .set(models, az, hash())
  }
  for (az in azs) {
    train_x = as.data.frame(ml_dataset[[az]][[x_key]][1:train_end, 1:tw_len])
    eval_x = as.data.frame(ml_dataset[[az]][[x_key]][(train_end+1):total_num_sample, 1:tw_len])
    V1<-train_x$V1;V2<-train_x$V2;V3<-train_x$V3;V4<-train_x$V4;V5<-train_x$V5;V6<-train_x$V6;V7<-train_x$V7;V8<-train_x$V8
    for (i in 1:tw_len) {
      train_y = ml_dataset[[az]][[y_key]][1:train_end, i]
      eval_y = ml_dataset[[az]][[y_key]][(train_end+1):total_num_sample, i]
      model <- lm(train_y ~ V1 + V2 + V3 + V4 + V5 + V6 + V7 + V8)
      .set(models[[az]], as.character(time_windows[i]), model)
      predict_y = predict(model, eval_x)
      print(paste(az, (time_windows[i]/3600), sqrt(mean((predict_y - eval_y)^2))))
      #print(summary(model))

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
