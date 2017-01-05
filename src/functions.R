parseName <- function(name) {
  c(strsplit(name, "_")[[1]][1], strsplit(name, "_")[[1]][2])
}

mergeTwoHashTable<-function(src, dest) {
  keys = names(src)
  for(k in keys) {
    .set(dest, k, src[[k]])
  }
  dest
}

#boxplot(all_interrupts, xlab="EC2 instance types (-Optimized)", ylab="Average number of interruptions per day")
fillInterruptTable <- function(dfs) {
  target_df = dfs[1]
  for(df in dfs) {
    cn = colnames(df)
    rn = rownames(df)
    print(df)
    for(c in cn) {
      for(r in rn) {
        if(!is.na(df[r,c])) {
          target_df[r,c] = df[r,c]
          print(target_df)
        }
      }
    }
  }
  target_df
}

getCompleteHistory <- function(folder, instance_types=c(), azs=c()) {
  history <- list()
  for(fname in list.files(folder)) {
    print(fname)
    if(length(instance_types) > 0) {
      it = parseName(fname)[2]
      az = parseName(fname)[1]
      if(!(it %in% instance_types) || (length(azs) > 0 && !(az %in% azs))) {
        print(paste(fname, "is not in the instance types. skip it"))
        next
      }
    }
    tryCatch ({
      history[[fname]] <- addDiffTime(as.data.frame(read.table(paste(folder,"/",fname,sep=""), stringsAsFactors=FALSE), stringsAsFactors=FALSE))
    }, error=function(e) {print(e)})
  }
  history
}

addDiffTime <- function(x) {
  x$V3 <- rowShift(x$V2, -1)
  x$V4 <- strptime(x$V3, "%Y-%m-%dT%H:%M:%OS") - strptime(x$V2, "%Y-%m-%dT%H:%M:%OS")
  colnames(x) <- c("price", "effectiveFrom", "effectiveUntil", "duonalNormration")
  x$duration[1] <- 0
  x
}

addFromToSeconds <- function(complete_history) {
  ns = names(complete_history)
  for (n in ns) {
    price_table = complete_history[[n]]
    print(nrow(price_table))
    price_table[, "FromSeconds"] = sapply(price_table[,"effectiveFrom"], function(x) as.numeric(strptime(x, "%Y-%m-%dT%H:%M:%OS"), unit="secs"))
    complete_history[[n]] = price_table
    print(n)
  }
  complete_history
}

rowShift <- function(x, shiftLen = 1L) {
  r <- (1L + shiftLen):(length(x) + shiftLen)
  r[r<1] <- NA
  return(x[r])
}

azToRegionName <- function(az) {
  if(grepl("^us-west-1", az)) return("US West (N. California)")
  else if(grepl("^us-west-2", az)) return("US West (Oregon)")
  else if(grepl("^us-east-1", az)) return("US East (N. Virginia)")
  else if(grepl("^ap-southeast-1", az)) return("Asia Pacific (Singapore)")
  else if(grepl("^ap-southeast-2", az)) return("Asia Pacific (Sydney)")
  else if(grepl("^ap-northeast-1", az)) return("Asia Pacific (Tokyo)")
  else if(grepl("^eu-central-1", az)) return("EU (Frankfurt)")
  else if(grepl("^eu-west-1", az)) return("EU (Ireland)")
  else return (NA)
}

# aggregate(latency~sourceAz+destination, data=upload_times, mean)
buildUploadTime <- function(path) {
  times <- data.frame()
  options(stringsAsFactors = FALSE)
  for(fname in list.files(path)) {
    print(fname)
    df = as.data.frame(read.table(paste(path,"/",fname,sep="")), stringsAsFactors=FALSE)
    df$V2 <- unlist(lapply(df$V2, function(x) paste(strsplit(x, "-")[[1]][5], strsplit(x, "-")[[1]][6], strsplit(x, "-")[[1]][7], sep="-")))
    times <- rbind(times, df)
  }
  colnames(times) <- c("sourceAz", "destination", "latency")
  times
}

buildCheckpointTime <- function(path) {
  checkpoint_time <- data.frame(stringsAsFactors=FALSE)
  for(fname in list.files(path)) {
    print(fname)
    df = as.data.frame(read.table(paste(path,"/",fname,sep="")), stringsAsFactors=FALSE)
    checkpoint_time <- rbind(checkpoint_time, df)
  }
  colnames(checkpoint_time) <- c("AZ", "Size", "Latency")
  mean_time = setNames(aggregate(Latency~Size, data=checkpoint_time, mean), c("Size", "mean"))
  min_time = setNames(aggregate(Latency~Size, data=checkpoint_time, min), c("Size", "min"))
  max_time = setNames(aggregate(Latency~Size, data=checkpoint_time, max), c("Size", "max"))
  checkpoint_time = merge(merge(mean_time, min_time), max_time)
  checkpoint_time
}

createCheckpointTimeFigure <- function(checkpoint_time, path) {
  pdf(path)
  par(mar=c(5,5,1,2)+0.1)
  plot(checkpoint_time$mean, ylim=c(0.05, 4.5), pch=19, xaxt="n", ann = FALSE, cex.axis=1.5)
  mtext(side=2, text="latency of checkpoint (seconds)", line=3,cex=1.3)
  mtext(side=1, text="checkpoint file size (log scale)", line=4, cex=1.3)
  text(1:11, par("usr")[3] - 0.2, labels = c("1MB", "2MB", "4MB", "8MB", "16MB", "32MB", "64MB", "128MB", "256MB", "512MB", "1024MB"), srt = 45, pos = 1, xpd = TRUE, cex=1.2)
  axis(side=1, at=1:11, labels=FALSE)
  arrows(1:11, checkpoint_time$min, 1:11,checkpoint_time$max, length=0.05, angle=90, code=3)
  dev.off()
}

getTemporalPriceDiff<-function(hourly_price) {
  ntimes<-ncol(hourly_price)
  azs <- rownames(hourly_price)
  diffs = vector()
  for (az in azs) {
    total_diff = 0
    for (i in 1:(ntimes-1)) {
      diff=abs(hourly_price[az, i] - hourly_price[az, i+1])
      total_diff = total_diff + diff
    }
    diffs = append(diffs, (total_diff/ntimes))
    print(paste(az, total_diff))
  }
  out_df = data.frame(diffs)
  rownames(out_df) = azs
  colnames(out_df) = c("priceDiff")
  out_df
}

createAvailabilityGainFigure <- function() {
  avail = read.csv("/User/kyungyonglee/Documents/Research/Writing/InterRegionTensorFlow/availability.csv")
  gain = read.csv("/User/kyungyonglee/Documents/Research/Writing/InterRegionTensorFlow/cost_gain.csv")
  ratio=avail/gain
  ratio$az = gain$az
  gpu_ratio = ratio$GPU
  names(gpu_ratio) <- ratio$az
  par(mar=c(8,4,1,2)+0.1)
  barplot(sort(gpu_ratio,TRUE),las=2,ylab="availability and cost gain", cex.axis=1.3, cex.names=1.1)
}
readSpotStartTime <- function(input_file) {
  conn <- file(input_file,open="r")
  linn <-readLines(conn)
  output_df = data.frame(matrix(nrow=length(linn), ncol=3))
  # each line is in the format of "Body": "1471390605439,eu-central-1b,90021",
  for (i in 1:length(linn)){
    rm_quote = gsub('"', "", linn[i])
    output_df[i, 1] = strsplit(rm_quote, "[,|:]")[[1]][2]
    output_df[i, 2] = strsplit(rm_quote, "[,|:]")[[1]][3]
    output_df[i, 3] = as.numeric(strsplit(rm_quote, "[,|:]")[[1]][4])
  }
  colnames(output_df) <- c("submit-time", "az", "latency")
  output_df
}

#createAcfFigures(g2_hourly_price, c("eu-central-1a", "us-east-1e", "us-west-1a", "eu-west-1a"), "/Users/kyungyonglee/Documents/Research/Writing/InterRegionTensorFlow/figures")
createAcfFigures <- function(hourly_matrix, target_azs, out_folder) {
  for(az in target_azs) {
    pdf(paste(out_folder,"/g2-acf-",az,".pdf",sep=""))
    val_acf<-acf(hourly_matrix[az,], lag.max=168, plot=FALSE)
    par(mar=c(5,5,1,2)+0.1)
    plot(val_acf, xlab="LAG (Hours)", type="l", xaxt="n", main="", cex.axis=1.7, cex.lab=1.7)
    axis(1, at=seq(0, 168, 24), labels=seq(0, 168, 24), cex.axis=1.7)
    abline(v=seq(0, 168, 24), lty=3)
    dev.off()
  }
}


buildRegularPrice <- function(price_file_csv, folder) {
  regular_price = hash()
  price_file = read.csv(price_file_csv)
  price_file = price_file[which(price_file$TermType=="OnDemand"&price_file$Operating.System=="Linux"&price_file$Tenancy=="Shared"),]
  for (fname in list.files(folder)) {
    az = strsplit(fname, "_")[[1]][1]
    instance_type = strsplit(fname, "_")[[1]][2]
    price = price_file[which(price_file$Location==azToRegionName(az)&price_file$Instance.Type==instance_type),"PricePerUnit"]
    .set(regular_price, fname, price)
  }
  regular_price
}

# This function returns the ratio of price where it is larger or smaller than the target price
getPricePortion <- function(offset) {
  library(hash)
  output <- data.frame(stringsAsFactors=FALSE)
  for(name in names(complete_history)) {
    if(!is.numeric(values(regular_price, name))) next
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
divideSpotPriceByOndemandPrice <- function(offset=1.0) {
  library(hash)
  for(name in names(complete_history)) {
    if(!is.numeric(values(regular_price, name))) next
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

convertAzToContinent <- function(az) {
  az_split = strsplit(az, "-")
  az_split[[1]][1]
}

getSpotInstanceAvailability <- function(complete_history, target_instances = c("g2.2xlarge", "c4.2xlarge", "m4.2xlarge", "r3.2xlarge", "i2.2xlarge"), offset=1.0) {
  ns = names(complete_history)
  availability = generateEmptyDf(ns, target_instances)
  for(name in ns) {
    price_table = complete_history[[name]]
    r_key = parseName(name)[1]
    c_key = parseName(name)[2]
    if(c_key %in% target_instances && is.numeric(values(regular_price, name))) {
      availability[r_key, c_key] = sum(price_table[which(price_table$ondemandRatio<=offset), "timePortion"])
    }
  }

  if(length(target_instances) > 1) {
    output = availability[!is.na(availability$g2.2xlarge),]
  } else {
    output = availability
  }
  output
}

# get the number of changes from ondemand and spot instances daily
getNumberOfInterruption <- function(complete_history) {
  ns = names(complete_history)
  target_instances = c("g2.2xlarge", "c4.2xlarge", "m4.2xlarge", "r3.2xlarge", "i2.2xlarge")
  interrupts = generateEmptyDf(ns, target_instances)
  for(name in ns) {
    price_table = complete_history[[name]]
    r_key = parseName(name)[1]
    c_key = parseName(name)[2]
    n_days = (price_table$FromSeconds[1]-price_table$FromSeconds[nrow(price_table)])/(3600*24)
    print(paste(name, n_days))
    if(c_key %in% target_instances && is.numeric(values(regular_price, name))) {
      interrupts[r_key, c_key] = countNumberOfInterrupts(regular_price[[name]], price_table$price)/n_days
    }
  }
# interrupts[!is.na(interrupts$g2.2xlarge),]
  interrupts
}

countNumberOfInterrupts <- function(rp, prices) {
  num_int = 0
  in_spot = ifelse(prices[length(prices)] >= rp, FALSE, TRUE)
  for(i in length(prices):1) {
    if(prices[i] >= rp) {
      if(in_spot == TRUE) {
        num_int = num_int + 1
      }
      in_spot = FALSE
    } else {
      in_spot = TRUE
    }
  }
  num_int
}

generateEmptyDf <- function(all_names, col_names) {
  row_name = vector()
  for(name in all_names) {
    n = parseName(name)
    row_name = append(row_name, n[1])
  }

  row_name = unique(row_name)
  out_df = as.data.frame(matrix(nrow = length(row_name), ncol = length(col_names)))
  colnames(out_df) <- col_names
  rownames(out_df) <- row_name
  out_df
}

spotInstancePriceRatioFig <- function() {
  ggplot(output, aes(AZs, ratio, colour=InstanceTypes, shape=InstanceTypes)) +geom_point(size = 3) + theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5), legend.position="top") + labs(x="Availability Zones", y="Spot Instance Price Ratio to On-Demand") + geom_point(colour="grey90", size = 1.5) + ylim(c(0.0, 0.5))
}

divideOnlySpotPriceByOndemand <- function(complete_history, offset=1.0) {
  library(hash)
  output <- data.frame(stringsAsFactors=FALSE)
  for(name in names(complete_history)) {
    if(!is.numeric(values(regular_price, name))) next
    rp <- values(regular_price, name)
    threshold = rp * offset
    price_history <- complete_history[[name]]
    lower_total_time <- as.integer(sum(price_history[which(price_history$price<threshold),4]), unit="secs")
    sp <- sum(as.integer(price_history[which(price_history$price<threshold),4], unit="secs") * price_history[which(price_history$price<threshold),1])
    odp <- lower_total_time * rp
    ratio = sp / odp
    if(is.nan(ratio)) ratio = 1.0
    parsed_name <- parseName(name)
    output <- rbind(output, data.frame(parsed_name[1], parsed_name[2], ratio, stringsAsFactors=FALSE))
  }
  colnames(output) <- c("AZs", "InstanceTypes", paste("ratio-",offset,sep=""))
  output
}

getCostGainWithDifferentOffset <- function(complete_history) {
  range = seq(0.3, 1.0, 0.1)
  output <- divideOnlySpotPriceByOndemand(complete_history, 0.2)
  for(r in range) {
    r_available = divideOnlySpotPriceByOndemand(complete_history, r)
    output <-merge(output, r_available)
  }
  rownames(output) <- output$AZs
  subset(output, select=-c(AZs, InstanceTypes))
}

getAvailabilityWithDifferentOffset <- function(complete_history) {
  range = seq(0.3, 1.0, 0.1)
  output <- getSpotInstanceAvailability(complete_history, c("g2.2xlarge"), 0.2)
  colnames(output) <- "0.2"
  for(r in range) {
    r_available = getSpotInstanceAvailability(complete_history, c("g2.2xlarge"), r)
    colnames(r_available) <- as.character(r)
    output <-cbind(output, r_available)
  }
  output
}

# generateCostGainAvailabilityPlot(g2_cost_gain, g2_availability, "/Users/kyungyonglee/Documents/Research/Writing/InterRegionTensorFlow/figures/")
generateCostGainAvailabilityPlot <- function(cost_gain, availability, out_path="") {
  azs = rownames(cost_gain)
  for (az in azs) {
    if(out_path == "") {
      dev.new()
    } else {
      pdf(paste(out_path, "/g2-cost-gain-availability-", az, ".pdf", sep=""))
    }
    par(mar=c(5,5,1,2)+0.1)
    plot(colnames(g2_cost_gain), 1-g2_cost_gain[az,], ylim=c(0.0,1.0), ylab="Ratio", xlab="Spot instance bid price ratio", pch=c(15), cex.axis=1.5, cex.lab=1.5)
    legend(x="bottomright", c("paid cost ratio", "availability rate"), lty=c(0, 1), pch=c(15, NA), cex=1.5)
    par(new=TRUE)
    plot(colnames(g2_cost_gain), g2_availability[az,], type="l", axes=FALSE, bty="n", xlab="", ylab="",ylim=c(0.0,1.0))
    if(out_path != "") {
      dev.off()
    }
  }
}
# Get the ratio of the price comparing to the on-demand price
getPriceRatioToOndemand <- function(complete_history, offset=1.0) {
  library(hash)
  for(name in names(complete_history)) {
    if(!is.numeric(values(regular_price, name))) next
    rp <- values(regular_price, name) * offset
    price_history <- complete_history[[name]]
    rp <- regular_price[[name]]
    price_history$ondemandRatio <- price_history$price / rp
    price_history$timePortion <- as.integer(price_history$duration, unit="secs")/as.integer(sum(price_history$duration), unit="secs")
    complete_history[[name]] <- price_history
  }
  complete_history
}

#heat_matrix_r3_2x=genHeatMap(complete_history, "r3.2xlarge")
#heat_matrix_g2_2x=genHeatMap(complete_history, "g2.2xlarge")
#heat_matrix_c4_2x=genHeatMap(complete_history, "c4.2xlarge")
#heat_matrix_m4_2x=genHeatMap(complete_history, "m4.2xlarge")
#heat_matrix_i2_2x=genHeatMap(complete_history, "i2.2xlarge")
genHeatMap <- function(complete_history, instance_type) {
  elements = names(complete_history)
  spacing = 3600
  start_time = complete_history[[1]][nrow(complete_history[[1]]), "FromSeconds"] + 86400
  end_time = complete_history[[1]][1, "FromSeconds"] - 86400
  out_matrix = matrix(, nrow=0, ncol=((end_time-start_time)/spacing))
  az_names = vector()
  for(e in elements) {
    ns = parseName(e)
    if(ns[2] == instance_type && ns[1]!="ap-southeast-2c") {
      spot_price = complete_history[[e]]
      cur_price = vector()
      for(current in seq(start_time, end_time, spacing)) {
        maxless <- max(spot_price$FromSeconds[spot_price$FromSeconds < current])
        price = spot_price[which(spot_price$FromSeconds == maxless), "ondemandRatio"]
        cur_price = append(cur_price, price) 
      }
      print(paste(ns[1], length(cur_price)))
      out_matrix = rbind(out_matrix, cur_price)
      if (length(cur_price) > 0) {
        az_names = append(az_names, ns[1])
      }
    }
  }
  print(paste(nrow(out_matrix), length(az_names)))
  rownames(out_matrix) <- az_names
  out_matrix
}

library(gplots)
#drawHeatmap(heat_matrix_r3[,1400:2096], "/Users/kyungyonglee/Documents/Research/Writing/InterRegionTensorFlow/figures/heatmap-r2-2x.pdf")
drawHeatmap <- function(mat, fname = "") {
  ncol_mat = ncol(mat)
  col_name=c()
  for(i in 1:ncol_mat) {
    if(i %% 24 == 0) {
      col_name=append(col_name, i/24)
    } else {
      col_name=append(col_name, NA)
    }
  }
  if(fname=="") {
    dev.new()
  } else {
    pdf(fname)
  }
  lwid=c(0.001, 4.999)
  lhei=c(0.01, 4.0, 0.1)
  lmat=rbind(c(0,3), c(2, 1), c(0, 4))
#  col=paste("gray",seq(10,40,10),sep="")
# heatmap.2(mat,col=cls, breaks=brks, Rowv=NA, Colv=NA, trace='none', dendrogram='none', density.info="none", lmat=lmat, lhei=lhei, lwid=lwid, labCol=col_name,srtCol=0, cexCol=1.0,cexRow=1.3,xlab="The day since July 1st. 2016.",adjCol=c(0,1), margins =c(4, 11), key=FALSE)
  heatmap.2(mat,col=cls, breaks=brks, Rowv=NA, Colv=NA, trace='none', dendrogram='none', density.info="none", keysize=1, lmat=lmat, lhei=lhei, lwid=lwid, labCol=col_name,srtCol=0, cexCol=1.0,cexRow=1.3,xlab="The day since July 1st. 2016.",adjCol=c(0,1), key.par=list(mar=c(6,5.0,1.5,5.0)),key.xlab="spot instance price ratio to the on-demand instance price",key.title="", margins =c(4,11), key=FALSE) 
  if(fname != "") {
    dev.off()
  }
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
    print(paste(name, ratio, min(avail_times), max(avail_times), mean(avail_times), nrow(price_history), length(avail_times), rp, (sum(paid_price)/sum(paid_time))))
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
# print(paste("execution time", exec_time, "workloads", workload[1], workload[2],workload[3],workload[4], "remaining after execution", total_remain))
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
# print(paste("Processed workload", wl[1], wl[2], wl[3], wl[4], wl[5], exec_time, "current spot price is", ph[i, "price"]))
    if (wl[5] <= 0) {
# print("Processing is over")
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
    if(tp >= ph[i, "price"]) { # bid successful
      in_spot_instance = 1
      end_time = strptime(ph[i, "effectiveUntil"], "%Y-%m-%dT%H:%M:%OS")
      exec_time = round(as.numeric(end_time - ct, unit="secs"))
      wl = processWorkload(exec_time, wl)
# print(paste("Processed workload", wl[1], wl[2], wl[3], wl[4], wl[5], exec_time, "current spot price is", ph[i, "price"]))
      if (wl[5] <= 0) {
# print("Processing is over")
        consumed_price = consumed_price + (ph[i, "price"] * (exec_time + wl[5]) / 3600)
        consumed_time = consumed_time + exec_time + wl[5]
        break
      }
      ct = end_time
      consumed_time = consumed_time + exec_time
      consumed_price = consumed_price + (ph[i, "price"] * exec_time / 3600)
# print(paste("current consumed price is", consumed_price))
    } else {
# print(paste("outbid target price", tp, "regular price", rp, "current price", ph[i, "price"]))
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

spotInstanceAcrossRegionsUntilInterruption <- function(phIndex, tp, rp, wl) {
  consumed_price = 0
  consumed_time = 0
  total_switch = 0
  in_spot_instance = 0
  instance_start_time = 0
  setPriceTable(all_pt_g2_2x)
  azs = names(ph)
  partial_pricing = c(0,0) # first element is price, second is time
  running_az = getLowestPriceAz(phIndex)
  prev_running_az = running_az
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  migration_plan = hash()
  .set(migration_plan, as.character(phIndex), running_az)
  for(i in phIndex:2) {
    switched=FALSE
    if (prev_running_az == "ondemand") {
      running_az = getLowestPriceAz(i)
    }
    cur_price = ph[i, running_az]
    if(tp < cur_price) { # interruption find new spot instance
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
        switched=TRUE
        consumed_price = consumed_price - partial_pricing[1]
        instance_start_time = getInstanceStartTime(start_latency) 
# print(paste("interrupt from si", i, running_az, cur_price, total_switch))
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
# print(paste("there is no si using od", i, running_az, cur_price, total_switch))
      } else {
# print(paste("got a new si", i, running_az, cur_price, total_switch))
        in_spot_instance = 1
        .set(migration_plan, as.character(i), running_az)
      }
    } else {
      if (prev_running_az == "ondemand") {
        total_switch = total_switch + 1
        switched=TRUE
        instance_start_time = getInstanceStartTime(start_latency)
        .set(migration_plan, as.character(i), running_az)
# print(paste("switch from od to si", i, running_az, cur_price, total_switch))
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    prev_running_az = running_az
    current_time = end_time

    if(instance_start_time > 0) {
      instance_start_time = instance_start_time - exec_time
      consumed_time = consumed_time + exec_time
      if (instance_start_time < 0) {
        exec_time = instance_start_time * -1
        consumed_time = consumed_time - exec_time
      } else {
        next
      }
    }
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)
    partial_pricing = calculatePartialPricing(exec_time, cur_price, partial_pricing)
    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] * cur_price / 3600)
      consumed_time = consumed_time + wl[5]
      break
    }
  }
  list("stat"=c(consumed_price, consumed_time, total_switch), "migration_plan"=migration_plan)
}

calculatePartialPricing <- function(exec_time, current_price, partial_pricing) {
  cur_time = partial_pricing[2] + exec_time
  if (cur_time < 3600) {
    partial_pricing[1] = partial_pricing[1] + (exec_time * current_price / 3600)
    partial_pricing[2] = cur_time
  } else {
    partial_pricing[2] = cur_time %% 3600
    partial_pricing[1] = partial_pricing[2] * current_price / 3600
  }
  partial_pricing
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
# running_az_time = system.time(selectOptimalAz(i, prev_running_az, NA))
      running_az = selectOptimalAz(i, prev_running_az, NA)
# running_az_time = system.time(getLowestPriceAz(i, prev_running_az))
# running_az = getLowestPriceAz(i, prev_running_az)
# print(paste("running_az_time", running_az_time, running_az))
# running_az = getLowestPriceAz(i, prev_running_az) 
      cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
      if (tp <= cur_price) { # should use on-demand instance as cur_price is the lowest one
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
  # print(paste("time before switch is", time_before_switch, "cost savings", cost_saving_switch, i, "before interrupt:", az_before_interrupt, "prev az:", prev_running_az, "current az:", running_az, ph[i, prev_running_az], cur_price))
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
 # print(paste(i, az_before_interrupt, price_no_interrupt, running_az, cur_price, exec_time))

      if (wl[5] <= 0) {
        consumed_price = consumed_price + (wl[5] * cur_price/ 3600)
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
  prev_running_az = getLowestPriceAz(phIndex, "")
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
    if (tp < cur_price) { # should use on-demand instance as cur_price is the lowest one
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
# print(paste("exec_time", exec_time, "wl", wl))
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)

    if(interrupted == TRUE) {
# print(paste("time before switch is", time_before_switch, "cost savings", cost_saving_switch, i, "before interrupt:", az_before_interrupt, "prev az:", prev_running_az, "current az:", running_az, ph[i, prev_running_az], cur_price))
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
      consumed_price = consumed_price + (wl[5] * cur_price / 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
    prev_running_az = running_az
  }
# print(c(consumed_price, consumed_time, total_switch))
  list("stat"=c(consumed_price, consumed_time, total_switch), "should_migrate_indexes"=total_cost_savings)
}

selectOptimalAz <- function(i, prev_running_az, shouldMigrateIndex=NA) {
  if (is.null(shouldNotMigrateIndex) && is.na(shouldMigrateIndex)) {
    running_az = getLowestPriceAz(i, prev_running_az)
  } else if (!is.null(shouldNotMigrateIndex)) {
    i_key = as.character(i)
    if(has.key(i_key, shouldNotMigrateIndex)) {
# running_az = getLowestPriceAz(i, prev_running_az, shouldNotMigrateIndex[[i_key]])
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
  setPriceTable(all_pt_g2_2x)
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
# print(paste(running_az, i, cur_price))
    if (running_az == "ondemand") { # should use on-demand instance as cur_price is the lowest one
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
  list("stat"=c(consumed_price, consumed_time, total_switch), "migrate_summary"=migrate_summary)
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
  instance_start_time = 0
  partial_pricing = c(0,0)
  if(length(shouldMigrateIndexes) == 0) {
    shouldMigrateIndexes = NA
  }
  if(length(shouldNotMigrateIndexes) == 0) {
    shouldNotMigrateIndexes = NULL
  }
  for(i in phIndex:2) {
    running_az = getLowestPriceAz(i, prev_running_az)
    cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
# print(paste(running_az, i, cur_price))
    if (tp < cur_price) { # should use on-demand instance as cur_price is the lowest one
      running_az = "ondemand"
      cur_price = rp
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
        instance_start_time = getInstanceStartTime(start_latency)
        consumed_price = consumed_price + ((3600/partial_pricing[2])*partial_pricing[1])
      }
      in_spot_instance = 0
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
        instance_start_time = getInstanceStartTime(start_latency)
        consumed_price = consumed_price + ((3600/partial_pricing[2])*partial_pricing[1])
# print(paste(cur_price, partial_pricing[2], partial_pricing[1]))
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    current_time = end_time
    prev_running_az = running_az

    if(instance_start_time > 0) {
      instance_start_time = instance_start_time - exec_time
      consumed_time = consumed_time + exec_time
      if (instance_start_time < 0) {
        exec_time = instance_start_time * -1
        consumed_time = consumed_time - exec_time
      } else {
        next
      }
    } 

    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)
    partial_pricing = calculatePartialPricing(exec_time, cur_price, partial_pricing)

    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] * cur_price/ 3600)
      consumed_time = consumed_time + wl[5]
      break
    }
  }
  migrate_summary = data.frame(migrate_indexes, migrate_azs, migrate_benefit)
  list("stat"=c(consumed_price, consumed_time, total_switch), "migrate_summary"=migrate_summary)
}

genMlSubmitTime <- function(all_pt_str) {
  all_pt = eval(parse(text=all_pt_str))
  time_start = nrow(all_pt) - as.integer(nrow(all_pt) * 0.8)
  time_end = nrow(all_pt) - as.integer(nrow(all_pt) * 0.6)
  azs = colnames(all_pt)[2:(ncol(all_pt)-1)] # remove the first column (times)
  submit_time = all_pt[as.integer(runif(1, time_start, time_end)), "times"]
  index = which(all_pt$times==submit_time)
  index
}

getBestAzUsingModel <- function(phIndex, prev_running_az, time_to_secs, time_windows, max_od=TRUE, avg_out = TRUE, key="3600", rp=0.65){
  setPriceTable(all_payment_g2_2x)
  window_itr = getIndexOfTimeDiff(time_to_secs, phIndex, time_windows, -1)
  records = as.data.frame(matrix(nrow=length(azs), ncol=length(time_windows)))
  rownames(records) = azs
  for(wi in 1:length(window_itr)) {
    price_means = vector(length=length(azs))
    od_prices = ph[window_itr[wi]:phIndex, "ondemand"]
    for(i in 1:length(azs)) {
      if(max_od) {
        mean_price = mean(pmin(ph[window_itr[wi]:phIndex, azs[i]], od_prices))
      } else {
        mean_price = mean(ph[window_itr[wi]:phIndex, azs[i]])
      }
      price_means[i] = mean_price
    }
    if(avg_out) {
      pm = mean(price_means)
      price_means = price_means - pm
    }
    for(i in 1:length(azs)) {
      records[azs[i], wi] = price_means[i]
    }
  }
  predicts = double()
  for(i in 1:length(azs)) {
    model = models[[azs[i]]][[key]]
    predicts[i] = predict(model, records[azs[i],])
  }
  names(predicts) = azs
  ordered_predicts = order(predicts)
  setPriceTable(all_pt_g2_2x)
  for (op in ordered_predicts) {
   az = names(predicts)[op]
   if (ph[phIndex, az] < rp) {
     return (names(predicts)[op])
   }
  }
  return ("ondemand")
# min_expected_az = names(predicts)[which(min(predicts)==predicts)]
}

getMostAvailableAz <- function(phIndex, rp=0.65) {
  setPriceTable(all_pt_g2_2x)
  total_row = length(all_pt_g2_2x[[azs[1]]])
  max_length = 0
  max_len_az = ""
  for(az in azs) {
    win_len = min(which(ph[[az]][phIndex:total_row]>rp))
    if(max_length < win_len) {
      max_len_az = az
      max_length = win_len
    }
  }
  max_len_az
}

spotInstanceWithMostAvailableUntilInterrupt <- function(phIndex, tp, rp, wl) {
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  prev_running_az = getMostAvailableAz(phIndex)
  running_az = prev_running_az
  migrate_indexes = vector()
  migrate_azs = vector()
  migrate_benefit = vector()
  migration_plan = hash()
  .set(migration_plan, as.character(phIndex), running_az)
  for(i in phIndex:2) {
    if (prev_running_az == "ondemand") {
      running_az = getMostAvailableAz(i)
# print("previously in ondemand")
    }
    cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
# print(paste(running_az, i, cur_price))
    if (tp < cur_price) { # should be migrated to either ondemand or other az
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
 # print(paste("interrupt from si", i, running_az, cur_price, total_switch))
      }
      running_az = getMostAvailableAz(i)
      cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
      if (tp < cur_price) { # there is no spot instance, use on-demand rp
        running_az = "ondemand"
        cur_price = rp
        in_spot_instance = 0
        if(prev_running_az != "ondemand") {
          .set(migration_plan, as.character(i), "ondemand")
        }
  # print(paste("there is no si using od", i, running_az, cur_price, total_switch))
      } else {
# print(paste("got a new si", i, running_az, cur_price, total_switch))
        in_spot_instance = 1
        .set(migration_plan, as.character(i), running_az)
      }
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
   # print(paste("different prev_running_az and cur running_az", i, running_az, cur_price, total_switch))
        .set(migration_plan, as.character(i), running_az)
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)

    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] * cur_price/ 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
    prev_running_az = running_az
  }
  list("stat"=c(consumed_price, consumed_time, total_switch), "migration_plan"=migration_plan)
}

spotInstanceUsingModelsUntilInterrupt <- function(phIndex, tp, rp, wl) {
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  prev_running_az = getBestAzUsingModel(phIndex, "", time_to_secs, time_windows)
  running_az = prev_running_az
  migrate_indexes = vector()
  migrate_azs = vector()
  migrate_benefit = vector()
  migration_plan = hash()
  .set(migration_plan, as.character(phIndex), running_az)
  for(i in phIndex:2) {
    if (prev_running_az == "ondemand") {
      running_az = getBestAzUsingModel(i, "", time_to_secs, time_windows)
# print("previously in ondemand")
    }
    cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
# print(paste(running_az, i, cur_price))
    if (tp < cur_price) { # should be migrated to either ondemand or other az
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
 # print(paste("interrupt from si", i, running_az, cur_price, total_switch))
      }
      running_az = getBestAzUsingModel(i, "", time_to_secs, time_windows)
      cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
      if (tp < cur_price) { # there is no spot instance, use on-demand rp
        running_az = "ondemand"
        cur_price = rp
        in_spot_instance = 0
        if(prev_running_az != "ondemand") {
          .set(migration_plan, as.character(i), "ondemand")
        }
  # print(paste("there is no si using od", i, running_az, cur_price, total_switch))
      } else {
# print(paste("got a new si", i, running_az, cur_price, total_switch))
        in_spot_instance = 1
        .set(migration_plan, as.character(i), running_az)
      }
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
   # print(paste("different prev_running_az and cur running_az", i, running_az, cur_price, total_switch)) 
        .set(migration_plan, as.character(i), running_az)
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)

    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] * cur_price/ 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
    prev_running_az = running_az
  }
  list("stat"=c(consumed_price, consumed_time, total_switch), "migration_plan"=migration_plan)
}

spotInstanceUsingModelsPeriodicCheck <- function(phIndex, tp, rp, wl, period=3600) {
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  elapsed_period = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  prev_running_az = getBestAzUsingModel(phIndex, "", time_to_secs, time_windows)
  running_az = prev_running_az
  migrate_indexes = vector()
  migrate_azs = vector()
  migrate_benefit = vector()
  migration_plan = hash()
  .set(migration_plan, as.character(phIndex), running_az)
  for(i in phIndex:2) {
    if (prev_running_az == "ondemand" || elapsed_period > period) {
      running_az = getBestAzUsingModel(i, "", time_to_secs, time_windows)
# print(paste("previously in ondemand or elapsed time", elapsed_period, running_az))
      elapsed_period = 0
    }
    cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
# print(paste(running_az, i, cur_price))
    if (tp < cur_price) { # should be migrated to either ondemand or other az
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
# print(paste("interrupt from si", i, running_az, cur_price, total_switch))
      }
      running_az = getBestAzUsingModel(i, "", time_to_secs, time_windows)
      cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
      if (tp < cur_price) { # there is no spot instance, use on-demand rp
        running_az = "ondemand"
        cur_price = rp
        in_spot_instance = 0
        if(prev_running_az != "ondemand") {
          .set(migration_plan, as.character(i), "ondemand")
        }
# print(paste("there is no si using od", i, running_az, cur_price, total_switch))
      } else {
# print(paste("got a new si", i, running_az, cur_price, total_switch))
        in_spot_instance = 1
        .set(migration_plan, as.character(i), running_az)
      }
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
# print(paste("different prev_running_az and cur running_az", i, running_az, cur_price, total_switch))
        .set(migration_plan, as.character(i), running_az)
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    elapsed_period = elapsed_period + exec_time
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)

    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] * cur_price/ 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
    prev_running_az = running_az
  }
  list("stat"=c(consumed_price, consumed_time, total_switch), "migration_plan"=migration_plan)
}

spotInstanceUsingModelsMigrationThreshold <- function(phIndex, tp, rp, wl, min_time=600, period = 3600, threshold=0.9) {
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  elapsed_period = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  prev_running_az = getBestAzUsingModel(phIndex, "", time_to_secs, time_windows)
  running_az = prev_running_az
  migrate_indexes = vector()
  migrate_azs = vector()
  migrate_benefit = vector()
  migration_plan = hash()
  .set(migration_plan, as.character(phIndex), running_az)
  for(i in phIndex:2) {
 
    if (prev_running_az == "ondemand" || elapsed_period > period) {
      candidate_az = getBestAzUsingModel(i, "", time_to_secs, time_windows)
      candidate_price = ifelse(candidate_az=="ondemand", rp, ph[i, candidate_az])
      prev_az_price = ifelse(prev_running_az=="ondemand", rp, ph[i, prev_running_az])
      if (candidate_price <= (prev_az_price*threshold)) {
        running_az = candidate_az
      }
      elapsed_period = 0
    } else {
      running_az = prev_running_az
    }

    cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
 # print(paste(running_az, i, cur_price))
    if (tp < cur_price) { # should be migrated to either ondemand or other az
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
     # print(paste("interrupt from si", i, running_az, cur_price, total_switch))
      }
      running_az = getBestAzUsingModel(i, "", time_to_secs, time_windows)
      cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
      if (tp < cur_price) { # there is no spot instance, use on-demand rp
        running_az = "ondemand"
        cur_price = rp
        in_spot_instance = 0
        if(prev_running_az != "ondemand") {
          .set(migration_plan, as.character(i), "ondemand")
        }
    # print(paste("there is no si using od", i, running_az, cur_price, total_switch))
      } else {
# print(paste("got a new si", i, running_az, cur_price, total_switch))
        in_spot_instance = 1
        .set(migration_plan, as.character(i), running_az)
      }
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
# print(paste("different prev_running_az and cur running_az", i, running_az, cur_price, total_switch))
        .set(migration_plan, as.character(i), running_az)
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    elapsed_period = elapsed_period + exec_time
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)

    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] * cur_price/ 3600)
      consumed_time = consumed_time + wl[5]
      break
    }

    current_time = end_time
    prev_running_az = running_az
  }
  list("stat"=c(consumed_price, consumed_time, total_switch), "migration_plan"=migration_plan)
}

# period means the periodic time pass before making migration
# time_th means the amount of time that should be lowest
spotInstanceBestPriceThreshold <- function(phIndex, tp, rp, wl, period=3600, time_th=0, threshold=0.9) {
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  prev_running_az = getLowestPriceAz(phIndex, "")
  migrate_indexes = vector()
  migrate_azs = vector()
  migrate_benefit = vector()
  shouldMigrateIndexes = NA
  shouldNotMigrateIndexes = NULL
  elapsed_time = 0
  instance_start_time = 0
  partial_pricing = c(0, 0)
  current_lowest_price_az = ""
  current_lowest_price_time = 0
  for(i in phIndex:2) {
    if (elapsed_time >= period) {
      candidate_az = selectOptimalAz(i, prev_running_az, shouldMigrateIndexes)
      candidate_price = ifelse(candidate_az=="ondemand", rp, ph[i, candidate_az])
      prev_price = ifelse(prev_running_az=="ondemand", rp, ph[i, prev_running_az])
      if (candidate_az != prev_running_az) {
        time_th_condition = current_lowest_price_time >= time_th
        price_th_condition = prev_price * threshold >= candidate_price
        running_az = ifelse((prev_price >= rp) | (price_th_condition&time_th_condition), candidate_az, prev_running_az)
      } else {
        running_az = candidate_az
      }
      elapsed_time = 0
    } else {
      if (time_th > 0) {
        candidate_az = selectOptimalAz(i, prev_running_az, shouldMigrateIndexes)
      }
      running_az = prev_running_az
      if (running_az == "ondemand" | ph[i, running_az] >= rp) {
        running_az = selectOptimalAz(i, prev_running_az, shouldMigrateIndexes)
      }
    }
    cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
# print(paste(running_az, i, cur_price))
    if (tp < cur_price) { # should use on-demand instance as cur_price is the lowest one
      running_az = "ondemand"
      cur_price = rp
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
        instance_start_time = getInstanceStartTime(start_latency)
        if (period == 0 | period %% 3600 != 0) {
          consumed_price = consumed_price + ((3600/partial_pricing[2])*partial_pricing[1])
        }
#        print(paste(i, cur_price, prev_price, running_az, prev_running_az, candidate_az))
      }
      in_spot_instance = 0
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
        instance_start_time = getInstanceStartTime(start_latency)
        if (period == 0 | period %% 3600 != 0) {
          consumed_price = consumed_price + ((3600/partial_pricing[2])*partial_pricing[1])
        }
#        print(paste(i, cur_price, prev_price, running_az, prev_running_az))
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    if(time_th > 0) {
      if (current_lowest_price_az == candidate_az) {
        current_lowest_price_time = current_lowest_price_time + exec_time
      } else {
        current_lowest_price_az = candidate_az
        current_lowest_price_time = exec_time
      }
    }
    current_time = end_time
    prev_running_az = running_az

    if(instance_start_time > 0) {
      instance_start_time = instance_start_time - exec_time
      consumed_time = consumed_time + exec_time
      if (instance_start_time < 0) {
        exec_time = instance_start_time * -1
        consumed_time = consumed_time - exec_time
      } else {
        next
      }
    }

    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)
    partial_pricing = calculatePartialPricing(exec_time, cur_price, partial_pricing)
    elapsed_time = elapsed_time + exec_time
    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] * cur_price/ 3600)
      consumed_time = consumed_time + wl[5]
      break
    }
  }
  migrate_summary = data.frame(migrate_indexes, migrate_azs, migrate_benefit)
  list("stat"=c(consumed_price, consumed_time, total_switch), "migrate_summary"=migrate_summary)
}

spotInstanceHourlyMigration <- function(phIndex, tp, rp, wl, threshold=1.0) {
  in_spot_instance = 0
  total_switch = 0
  consumed_time = 0
  consumed_price = 0
  current_time = strptime(ph[phIndex, "times"], "%Y-%m-%dT%H:%M:%OS")
  prev_running_az = getLowestPriceAz(phIndex, "")
  migrate_indexes = vector()
  migrate_azs = vector()
  migrate_benefit = vector()
  shouldMigrateIndexes = NA
  shouldNotMigrateIndexes = NULL
  elapsed_time = 0
  instance_start_time = 0
  partial_pricing = c(0, 0)
  for(i in phIndex:2) {
    if (elapsed_time >= 3600) {
      candidate_az = selectOptimalAz(i, prev_running_az, shouldMigrateIndexes)
      candidate_price = ifelse(candidate_az=="ondemand", rp, ph[i, candidate_az])
      prev_price = ifelse(prev_running_az=="ondemand", rp, ph[i, prev_running_az])
      running_az = ifelse(prev_price * threshold > candidate_price, candidate_az, prev_running_az)
      elapsed_time = 0
    } else {
      running_az = prev_running_az
      if (running_az == "ondemand" | ph[i, running_az] >= rp) {
        running_az = selectOptimalAz(i, prev_running_az, shouldMigrateIndexes)
      }
    }
    cur_price = ifelse(running_az=="ondemand", rp, ph[i, running_az])
# print(paste(running_az, i, cur_price))
    if (tp < cur_price) { # should use on-demand instance as cur_price is the lowest one
      running_az = "ondemand"
      cur_price = rp
      if (in_spot_instance == 1) {
        total_switch = total_switch + 1
        instance_start_time = getInstanceStartTime(start_latency)
# consumed_price = consumed_price + ((3600/partial_pricing[2])*partial_pricing[1])
      }
      in_spot_instance = 0
    } else {
      if (running_az != prev_running_az) {
        total_switch = total_switch + 1
        instance_start_time = getInstanceStartTime(start_latency)
 # consumed_price = consumed_price + ((3600/partial_pricing[2])*partial_pricing[1])
      }
      in_spot_instance = 1
    }
    end_time = strptime(ph[i-1, "times"], "%Y-%m-%dT%H:%M:%OS")
    exec_time = round(as.numeric(end_time - current_time, unit="secs"))
    current_time = end_time
    prev_running_az = running_az

    if(instance_start_time > 0) {
      instance_start_time = instance_start_time - exec_time
      consumed_time = consumed_time + exec_time
      if (instance_start_time < 0) {
        exec_time = instance_start_time * -1
        consumed_time = consumed_time - exec_time
      } else {
        next
      }
    }

    elapsed_time = elapsed_time + exec_time

    if (elapsed_time > 3600) {
      exec_time = exec_time - (elapsed_time - 3600)
      current_time = current_time - (elapsed_time - 3600)
    }
    wl = processWorkload(exec_time, wl)
    consumed_time = consumed_time + exec_time
    consumed_price = consumed_price + (cur_price * exec_time / 3600)
    partial_pricing = calculatePartialPricing(exec_time, cur_price, partial_pricing)
    if (wl[5] <= 0) {
      consumed_price = consumed_price + (wl[5] * cur_price / 3600)
      consumed_time = consumed_time + wl[5]
      break
    }
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

runSpotInstanceSimAcrossRegion <- function(index, workload, price_offset, run_index, output="") {
#print(paste("index is ", index))
  stat = spotInstanceAcrossRegionsUntilInterruption(index, 0.65, 0.65, workload)[["stat"]]
  print(paste("spotInstanceAcrossRegionsUntilInterruption", index, workload[1], workload[2], stat[1], stat[2], stat[3]))
  performance[["spotInstanceAcrossRegionsUntilInterruption"]][run_index,]=c(stat[1], stat[3])

  result = getIndexShouldMigrateSequential(index, 0.65, 0.65, workload, 0.015)
  stat = result[["stat"]]
  print(paste("spotInstanceBestPrice", index, workload[1], workload[2], stat[1], stat[2], stat[3]))
  performance[["spotInstanceBestPrice"]][run_index,]=c(stat[1], stat[3])

  result = spotInstanceBestPrice(index, 0.65, 0.65, workload, result[["should_migrate_indexes"]])
  stat = result[["stat"]]
  print(paste("spotInstanceBestPriceWithShouldMigrate", workload[1], workload[2], stat[1], stat[2], stat[3]))
  performance[["spotInstanceBestPriceWithShouldMigrate"]][run_index,]=c(stat[1], stat[3])

  result = spotInstanceUsingModelsUntilInterrupt(index, 0.65, 0.65, workload)
  stat = result[["stat"]]
  print(paste("spotInstanceUsingModelsUntilInterrupt", workload[1], workload[2], stat[1], stat[2], stat[3]))
  performance[["spotInstanceUsingModelsUntilInterrupt"]][run_index,]=c(stat[1], stat[3])

  result = spotInstanceUsingModelsPeriodicCheck(index, 0.65, 0.65, workload)
  stat = result[["stat"]]
  print(paste("spotInstanceUsingModelsMigrationThreshold", workload[1], workload[2], stat[1], stat[2], stat[3]))
  performance[["spotInstanceUsingModelsMigrationThreshold"]][run_index,]=c(stat[1], stat[3])
  
  result = spotInstanceBestPriceThreshold(index, 0.65, 0.65, workload)
  stat = result[["stat"]]
  print(paste("spotInstanceBestPriceThreshold", workload[1], workload[2], stat[1], stat[2], stat[3]))
  performance[["spotInstanceBestPriceThreshold"]][run_index,]=c(stat[1], stat[3])

  result = spotInstanceWithMostAvailableUntilInterrupt(index, 0.65, 0.65, workload)
  stat = result[["stat"]]
  print(paste("spotInstanceWithMostAvailableUntilInterrupt", workload[1], workload[2], stat[1], stat[2], stat[3]))
  performance[["spotInstanceWithMostAvailableUntilInterrupt"]][run_index,]=c(stat[1], stat[3])
}
siSim <- function(price_offset=1.0, output="", num_run=100) {
  if(FALSE) {
    performance <<- hash()
    .set(performance, "spotInstanceAcrossRegionsUntilInterruption", data.frame(cost=numeric(0), migration=numeric(0)))
    .set(performance, "spotInstanceBestPrice", data.frame(cost=numeric(0), migration=numeric(0)))
    .set(performance, "spotInstanceBestPriceWithShouldMigrate", data.frame(cost=numeric(0), migration=numeric(0)))
    .set(performance, "spotInstanceUsingModelsUntilInterrupt", data.frame(cost=numeric(0), migration=numeric(0)))
    .set(performance, "spotInstanceUsingModelsMigrationThreshold", data.frame(cost=numeric(0), migration=numeric(0)))
    .set(performance, "spotInstanceBestPriceThreshold", data.frame(cost=numeric(0), migration=numeric(0)))
    .set(performance, "spotInstanceWithMostAvailableUntilInterrupt", data.frame(cost=numeric(0), migration=numeric(0)))
  }
  sample_df = performance[["spotInstanceAcrossRegionsUntilInterruption"]]
  result_start_index = nrow(sample_df)
  for ( i in (1:num_run)) {
# submit_time <- genRandomDateTime()
    workload <- genWorkload()
    index = genMlSubmitTime("all_payment_g2_2x")
    tryCatch({
      runSpotInstanceSimAcrossRegion(index, workload, price_offset, (i+result_start_index))
    }, error = function(e) {print(e)})
    gc()
  }
}

printPerformanceSummary <- function() {
  ns <- names(performance)
  for (n in ns) {
    print(paste(n, "cost", mean(performance[[n]]$cost), "migration", mean(performance[[n]]$migration)))
  }
}

runSimulationPerRegion <- function(submit_time, workload, key, price_offset, output="") {
  library(hash)
  rp <- values(regular_price, key)
  target_price <- rp * price_offset
  price_history <- complete_history[[key]]
  current_time <- submit_time
# print(workload)
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
  ggplot(subset(oneshot, InstanceTypes==instanceType), aes(spotToOndemandFromOne, spotLower)) + geom_point(size = 3) + labs(x="Cost Efficiency", y="System Availability") + geom_point(colour="grey90", size = 1.5) + geom_label_repel(data=subset(oneshot, InstanceTypes==instanceType), mapping=aes(x=spotToOndemandFromOne, y=spotLower, label=AZs), box.padding = unit(0.5, "lines"), point.padding = unit(0.0, "lines"), fontface = 'bold', color = 'blue') +theme(legend.position="none")
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


#ggplot(subset(oneshot, InstanceTypes=="g2.2xlarge"), aes(spotToOndemandFromOne, spotLower)) + geom_point(size = 3) + labs(x="Cost Efficiency", y="Spot Instance Availability") + geom_point(colour="grey90", size = 1.5) + geom_label_repel(data=subset(oneshot, InstanceTypes=="g2.2xlarge"), mapping=aes(x=spotToOndemandFromOne, y=spotLower, label=AZs), box.padding = unit(0.5, "lines"), point.padding = unit(0.1, "lines"), fontface = 'bold', color = 'blue') +theme(legend.position="none")

#ggplot(subset(oneshot, InstanceTypes=="g2.8xlarge"), aes(spotToOndemandFromOne, spotLower)) + geom_point(size = 3) + labs(x="Cost Efficiency", y="Spot Instance Availability") + geom_point(colour="grey90", size = 1.5) + geom_label_repel(data=subset(oneshot, InstanceTypes=="g2.8xlarge"), max.iter=20000, mapping=aes(x=spotToOndemandFromOne, y=spotLower, label=AZs), box.padding = unit(0.5, "lines"), point.padding = unit(0.01, "lines"), fontface = 'bold', color = 'blue') +theme(legend.position="none") + xlim(0, 1.0) + ylim(0, 1.0)

mergeLogs <- function(paths) {
  library(hash)
  aggr_logs = hash()
  for(fname in list.files(paths[1])) {
    .set(aggr_logs, fname, hash())
  }
  org_files = keys(aggr_logs)
  print(org_files)
  for(p in paths) {
    for(fname in list.files(p)) {
      if(!(fname %in% org_files)) next
      print(paste(p,fname))
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
# iterateWindowsCor(pr_str, all_pt_str, newer_tws, time_windows, azs, index, ns, 1, max_od, avg_out, rp)
# iterateWindowsCor(pr_str, all_pt_str, older_tws, time_windows, azs, index, ns, -1, max_od, avg_out, rp)
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
# print(paste(tw, output[1,2]))
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

# start_latency<-genStartLatency("/Users/kyungyonglee/Documents/Research/SpotInstance/SpotInstanceDeepLearning/src/ec2_start_latency_only")
genStartLatency <- function(path) {
  start_latency <<- as.numeric(unlist(read.table(path)))
  start_latency
}

getInstanceStartTime <- function(sl) {
  as.integer(sample(sl)[1]/1000)
}

runAllSims <- function(num_run=1000) {
  hn <- system("hostname", intern = TRUE)
  for (i in 1:num_run) {
    start_index = as.integer(runif(1, nrow(all_pt_g2_2x)*0.1, nrow(all_pt_g2_2x)))
    for(j in c(1, 4, 24, 72, 168)) {
      workload <- c(j, 3600, j-1, 3600, 0)
      stat = spotInstanceHourlyMigration(start_index, 0.65, 0.65, workload)$stat
      postToSqs(paste(hn,i,start_index,j,"hourly",stat[1],stat[2],stat[3],sep=","))
      stat = spotInstanceBestPrice(start_index, 0.65, 0.65, workload)$stat
      postToSqs(paste(hn,i,start_index,j,"best-price",stat[1],stat[2],stat[3],sep=","))
      stat = spotInstanceAcrossRegionsUntilInterruption(start_index, 0.65, 0.65, workload)$stat
      postToSqs(paste(hn,i,start_index,j,"interrupt",stat[1],stat[2],stat[3],sep=","))
    }
  }
}


runThresholdSims <- function(num_run=1000) {
  hn <- system("hostname", intern = TRUE)
  for (i in 1:num_run) {
    start_index = as.integer(runif(1, nrow(all_pt_g2_2x)*0.1, nrow(all_pt_g2_2x)))
    for (j in c(4, 24, 72, 168)) {
      workload <- c(j, 3600, j-1, 3600, 0)
      for (period in c(0, 60, 600, 1800, 3600, 7200)) {
          stat = spotInstanceBestPriceThreshold(start_index, 0.65, 0.65, workload, 0, period, 1.0)$stat
          postToSqs(paste(hn,i,start_index,j,period,1.0,stat[1],stat[2],stat[3],sep=","))
      }
      for (price_diff in c(0.2, 0.4, 0.6, 0.8, 0.9)) {
        stat = spotInstanceBestPriceThreshold(start_index, 0.65, 0.65, workload, 0, 0, price_diff)$stat
        postToSqs(paste(hn,i,start_index,j,0,price_diff,stat[1],stat[2],stat[3],  sep=","))
      }
    }
  }
}

runThresholdInterrupt <- function(num_run=1000) {
  hn <- system("hostname", intern = TRUE)
  for (i in 1:num_run) {
    start_index = as.integer(runif(1, nrow(all_pt_g2_2x)*0.1, nrow(all_pt_g2_2x)))
    for (j in c(24, 168)) {
      workload <- c(j, 3600, j-1, 3600, 0)
      stat = spotInstanceAcrossRegionsUntilInterruption(start_index, 0.65, 0.65, workload)$stat
      postToSqs(paste(hn,i,start_index,j,-1,-1,stat[1],stat[2],stat[3],sep=","))
      for (period in c(0, 600, 1800, 3600, 7200)) {
        for (price_diff in c(0.2, 0.4, 0.6, 0.8, 1.0)) {
          stat = spotInstanceBestPriceThreshold(start_index, 0.65, 0.65, workload, period, price_diff)$stat
          postToSqs(paste(hn,i,start_index,j,period,price_diff,stat[1],stat[2],stat[3],sep=","))
        }
      }
    }
  }
}


postToSqs <- function(msg) {
  cmd = paste("aws sqs send-message --queue-url https://sqs.us-east-1.amazonaws.com/647071230612/DeepSpotInstanceSimulation --message-body ", '"', msg, '" --region us-east-1', sep="")
  print(cmd)
#  system(cmd)
}

getPivotValue <- function(table, hour, target, pivot="interrupt", pivot_col="V5") {
  pivot_price = table[which(table[,pivot_col]==pivot & table$V4==hour), target]
  pivot_price
}

# t<-read.csv("/Users/kyungyonglee/Documents/Research/SpotInstance/SpotInstanceDeepLearning/src/interrupt_hourly_bestprice_simulation",header=F)
# mean_hour=aggregate(V7~V5+V4, data=t, mean)
# mean_hour_norm<-conditionalNorm(mean_hour, "V7")
# mean_price=aggregate(V6~V5+V4, data=t, mean)
# mean_price_norm<-conditionalNorm(mean_price, "V6")
# mean_price_time_norm$V5<-factor(mean_price_time_norm$V5, c("0", "1", "10", "30", "60", "120"))  to change order of a bar plot

# with dataset of price,time threshold
# mean_price_time = subset(mean_price[which(mean_price$V6==1.0),], select=-c(V6))
# mean_price_time_norm<-conditionalNorm(mean_price_time, "V7", 0)
# mean_price_time_norm$V5<-factor(mean_price_time_norm$V5, c("0", "1", "10", "30", "60", "120"))
# ggplot(mean_price_time_norm[which(mean_price_time_norm$V4!=1),], aes(factor(V4), V10, fill = V5)) + geom_bar(stat="identity", position = "dodge", colour="grey") + scale_fill_brewer("time threshold(secs)") + geom_hline(yintercept=1.0) + theme(legend.position="top", axis.text.x = element_text(size=20), axis.text.y = element_text(size=20), text = element_text(size=20),plot.margin=unit(c(0,0.5,0,0.5), "cm")) + ylab("normalized price") + xlab("workload running time (hours)")

conditionalNorm <- function(table, target, pivot="interrupt", pivot_col="V4") {
  norm_table = table
  for(i in 1:nrow(norm_table)) {
    norm_table[i,"V10"] = 
      norm_table[i, target] / getPivotValue(norm_table, norm_table[i, "V4"], target, pivot, pivot_col)
  }
  norm_table
}

# createSimulationLatencyFigure(mean_hour_norm)
createSimulationPriceFigure <- function(mean_price_norm) {
  ggplot(mean_price_norm[which(mean_price_norm$V4!=1),], aes(factor(V4), V10, fill = V5)) + geom_bar(stat="identity", position = "dodge", colour="grey") + scale_fill_brewer() + geom_hline(yintercept=1.0) + theme(legend.position="top", legend.title=element_blank(), axis.text.x = element_text(size=20), axis.text.y = element_text(size=20), text = element_text(size=20), plot.margin = unit(c(0,0.5,0,0.5), "cm")) + ylab("normalized price") + xlab("workload running time (hours)") + coord_cartesian(ylim = c(0.0, 1.3))
}

# createSimulationLatencyFigure(mean_price_norm)
createSimulationLatencyFigure <- function(mean_price_norm) {
  ggplot(mean_price_norm[which(mean_price_norm$V4!=1),], aes(factor(V4), V10, fill = V5)) + geom_bar(stat="identity", position = "dodge", colour="grey") + scale_fill_brewer() + geom_hline(yintercept=1.0) + theme(legend.position="top", legend.title=element_blank(), axis.text.x = element_text(size=20), axis.text.y = element_text(size=20), text = element_text(size=20), plot.margin = unit(c(0,0.5,0,0.5), "cm")) + ylab("normalized task completion time") + xlab("workload running time (hours)") + coord_cartesian(ylim = c(0.0, 1.3))
}

# threshold_sim <- read.csv("/Users/kyungyonglee/Documents/Research/SpotInstance/SpotInstanceDeepLearning/src/best_price_threshold_time_price",header=F)
# threshold_aggregate<-aggregate(cbind(V7,V8,V9)~V4+V5+V6, data=threshold_sim, mean)
# thresholdSimulationToMatrix(threshold_aggregate, "V7")
thresholdSimulationToMatrix <- function(aggregated_result, target) {
  workloads = c(4, 24, 72, 168)
#  workloads = c(24, 168)
  aggr_out=list()
  for (w in workloads) {
    partial_table = aggregated_result[which(aggregated_result$V4==w),]
    times = unique(partial_table$V5)
    prices = rev(unique(partial_table$V6))
    if(-1 %in% times) {
      times = times[-which(times == -1)]
    }
    if(-1 %in% prices) {
      prices = prices[-which(prices == -1)]
    }
    pivot_value = partial_table[which(partial_table$V5==-1 & partial_table$V6==-1), target]
    out_matrix = matrix(data=NA, nrow=length(times), ncol=length(prices), dimnames=list(as.character(times), as.character(prices)))
    for (t in times) {
      for (p in prices) {
        rn = as.character(t)
        cn = as.character(p)
        out_value = ifelse(length(pivot_value)==0, partial_table[which(partial_table$V5==t & partial_table$V6==p), target], partial_table[which(partial_table$V5==t & partial_table$V6==p),target]/pivot_value)
        out_matrix[rn,cn] = out_value
      }
    }
    aggr_out[[as.character(w)]]=out_matrix
  }
  aggr_out
}

