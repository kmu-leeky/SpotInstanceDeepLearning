library(plyr)

parseName <- function(name) {
  c(strsplit(name, "_")[[1]][1], strsplit(name, "_")[[1]][2])
}

addWorkloadClass <- function(path) {
  input = data.frame()
  file_list <- list.files(path)
  for (f in file_list) {
    temp_input = as.data.frame(read.table(f))
    input <- rbind(input, temp_input)
  }
  input = within(input, {round=ceiling(V2/10)})
  input = within(input, {iteration=ceiling(V3/600)})
  colnames(input) <- c("region", "rnd", "itr", "type", "price", "time", "switch", "round", "iteration")
  input
}

separatekeyCol <- function(input) {
  names = unlist(strsplit(as.character(input$region), "_"))
  input = subset(input, select=-region)
  azs = names[seq(1, length(names), 3)]
  its = names[seq(2, length(names), 3)]
  input$AZs = azs
  input$InstanceTypes = its
  input
}

getPriceMean <- function(input) {
  ddply(input, .(region, iteration, round, type), summarise, mean=mean(price))
}

addOndemandPriceRatio <- function(summary) {
  summary$price_ratio <- NA
  for(i in 1:nrow(summary)) {
    self_entry = summary[i,]
    compare_price = summary[(summary$type=="onDemandOnly")&(summary$iteration==self_entry$iteration)&summary$round==self_entry$round&summary$AZs==self_entry$AZs&summary$InstanceTypes==self_entry$InstanceTypes,]$mean
    summary[i,"price_ratio"] <- (self_entry$mean/compare_price)
  }
  summary
}

addCheckpointPriceRatio <- function(summary) {
  summary$price_ratio <- NA
  for(i in 1:nrow(summary)) {
    self_entry = summary[i,]
    compare_price =                                                                     summary[(summary$type=="spotInstanceOnDemandAlways")&(summary$iteration==self_entry$iteration)&summary$round==self_entry$round&summary$AZs==self_entry$AZs&summary$InstanceTypes==self_entry$InstanceTypes,]$mean
    summary[i,"checkpoint_ratio"] <- (compare_price - self_entry$mean) / compare_price
  }
  summary
}

getCheckpointHeavyWorkload <- function(){
  heavyWorkload<-mSimResult[mSimResult$iteratioin==3&mSimResult$round==3,]
  heavyWorkload <- addOndemandPriceRatio(heavyWorkload)
  heavyWorkload<-addCheckpointPriceRatio(heavyWorkload)
  checkpoint_heavy= heavyWorkload[heavyWorkload$type=="spotInstanceOndemandMixture", c("AZs", "InstanceTypes", "checkpoint_ratio")]
}

getMergedCheckpointAvail <- function() {
  checkpoint_ratio_freq_merge = merge(aggr_consec_avail, checkpoint_heavy, by=c("AZs", "InstanceTypes"))
  checkpoint_ratio_freq_merge
}

buildPriceRatioBarPlot <- function() {
  az_ratio = dcast(mSimResult, iteration+round+type+InstanceTypes~AZs, value.var="price_ratio")
  sst<- az_ratio[az_ratio$iteration==1&az_ratio$round==1&az_ratio$InstanceTypes=="g2.2xlarge",]
  ssst = subset(sst, select=-c(iteration,round,InstanceTypes))
  rownames(ssst) = ssst[,1]
  ssst = subset(ssst, select=-c(type))
  barplot(as.matrix(ssst),beside=T)
}

build2xMultiBarPlots <- function(store=FALSE) {
  instance_type=c("g2.2xlarge")
  col_indexes = c(2,15,20,8,4,12)
  az_names = c("ap-ne-1c", "us-e-1e", "us-w-2c", "eu-c-1b", "ap-se-1b", "us-e-1a")
  buildMultiBarPlots(instance_type, col_indexes, az_names, store)
}

build8xMultiBarPlots <- function(store=FALSE) {
  instance_type=c("g2.8xlarge")
  col_indexes = c(7,8,20,9,12,17)
  az_names = c("eu-c-1a","eu-c-1b","us-w-2c","eu-w-1a","us-e-1a","us-w-1b")
  buildMultiBarPlots(instance_type, col_indexes, az_names, store)
}
buildMultiBarPlots <- function(its, col_indexes, az_names, store=FALSE) {
  for(iter in c(1,2,3)) {
    for(rnd in c(1,2,3)) {
      if(iter != rnd) {
        next
      }
      for(it in its) {
        if(!store) {
          dev.new()
        } else {
          itfname = gsub("[.]", "_", it)
          pdf(paste("../Writing/SpotInstance/price_simulation_",itfname,"_iter_",iter,"_rnd_",rnd,".pdf",sep=""))
        }
        sst <- az_ratio[az_ratio$iteration==iter&az_ratio$round==rnd&az_ratio$InstanceTypes==it,]
        ssst = subset(sst, select=-c(iteration,round,InstanceTypes))
        rownames(ssst) = ssst[,1]
        ssst = subset(ssst, select=-c(type))
        ssst = ssst[,col_indexes]
        title = paste("iteration:", iter, "round:", rnd, "instance type:", it)
        barplot(as.matrix(ssst[-c(1),]),beside=T, legend.text=TRUE, args.legend = list(legend=c("OD+SI Restart", "OD+SI Checkpointing"),cex=1.3),    density=10,border = TRUE,col=c("red", "blue"), angle=c(0, 45),space=c(0.3, 1), ylim=c(0,1.0), cex.axis=1.5, cex.names=1.5, cex.lab=1.5,xaxt="n")
        text(cex=1.5, x=bp[2,]+1.0, y=-0.03, label=az_names, xpd=TRUE, srt=30, pos=2)
        if(store) {
          dev.off()
        }
      }
    }
  }
}

getConditionalAvailableTime <- function() {
  cond_time <- c(60, 600, 3600, 36000, 86400, 604800)
  for (ct in cond_time) {
    new_col = numeric()
    for(i in 1:nrow(consecutive_available)) {
      span = consecutive_available[i,]$consecutive_available
      remain = -1
      if (span > ct) {
        remain = span - ct
      }
      new_col <- append(new_col, remain)
    }
    col_name = paste("second", as.character(ct),sep="")
    consecutive_available[,eval(col_name)] = new_col
  }
  consecutive_available
}

plotConditionalAvailable <- function() {
  a = getConditionalAvailableTime()
  plot(ecdf(a[a$second60>0,]$second60))
  lines(ecdf(a[a$second600>0,]$second600))
  lines(ecdf(a[a$second3600>0,]$second3600))
  lines(ecdf(a[a$second36000>0,]$second36000))
  lines(ecdf(a[a$second86400>0,]$second86400))
  lines(ecdf(a[a$second604800>0,]$second604800))
}
