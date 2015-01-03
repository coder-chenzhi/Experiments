# Tables of task events need some pre-process, due to empty cell of machineID .

setwd("E:\\data\\GoogleCluster\\task_events")
files = dir(getwd())
#files=c('part-00000-of-00500.csv','part-00001-of-00500.csv')
for (f in files) {
  print(paste('Start to fill file ', f, ' at ', Sys.time()))
  data = read.csv(f, header=FALSE)
  machineID=unique(data[,5]) # find all possible machineID
  machineID=machineID[!is.na(machineID)] # get rid of na
  set.seed(17)
  # fill empty machineID with random exist machineID
  data[,5] = ifelse(is.na(data[,5]), sample(machineID, sum(is.na(data[,5])), replace=TRUE), data[,5])
  write.table(data, paste('full-',f), quote=FALSE, sep=",", na='',row.names=FALSE, col.names=FALSE)
  print(paste('Fill file ', f, ' done at ', Sys.time()))
}
print('All done!!!')
