# Authors: Maksym Bondarenko mb4@soton.ac.uk
# Date :  October 2017
# Version 0.1
#
#' wpGetAvalMem function will return avalible
#' of the system memory in GB
#' Tested on Windows 10
#'
#' @rdname wpGetAvalMem
#' @return numeric
wpGetAvalMem <- function(){

  OS = tolower(wpGetOS())

  if(OS == 'windows'){
    memavail = shell('wmic OS get FreePhysicalMemory /Value',intern=T)
    memavail = memavail[grep('FreePhysicalMemory', memavail)]
    memavail = as.numeric(gsub('FreePhysicalMemory=','',memavail))
  }else if (OS == 'osx'){
    memavail = as.numeric(unlist(strsplit(system("sysctl hw.memsize", intern = T), split = ' '))[2])/1e3
  }else{
    memavail = as.numeric(system(" awk '/MemFree/ {print $2}' /proc/meminfo", intern=T))
  }

  return(memavail/ (1024 * 1024))
}
