#  Authors: Maksym Bondarenko mb4@soton.ac.uk
#  Date :  October 2017
#  Version 0.1
#
#' wpZonalStatistics function compute zonal statistics. That is,
#' cross-tabulate the values of a Raster* object
#' based on a "zones" RasterLayer. NA values are removed.
#' Function uses DoParallel library to work with a big raster data
#'
#' @param x Raster* object
#' @param y RasterLayer object with codes representing zones
#' @param fun The function to be applied. Either as character: 'mean', 'min', 'max' and 'sum'
#' @param cores Integer. Number of cores for parallel calculation
#' @param minblk Integer. Minimum number of blocks
#' @param na.rm using na.rm = TRUE for missing data
#' @param silent If FALSE then the progress will be shown
#' @rdname wpZonalStatistics
#' @return A data.frame with a value for each zone (unique value in zones)
#' @export
#' @examples
#' wpZonalStatistics( x=rasterObj1, y=rasterObj2, cores=2, minblk=4  )
#'
wpZonalStatistics <- function(x, y, fun='mean', cores=NULL, minblk=NULL, na.rm=TRUE, silent=TRUE) {

  #chack_pkg_load("raster","doParallel")

  fun <- tolower(fun)
  if(length(fun) > 1){
    fun <- fun[1]
  }

  if (! fun %in% c('sum', 'mean', 'sd', 'min', 'max', 'count')) {
    stop("fun can be 'sum', 'mean', 'sd', 'min', 'max', or 'count'")
  }
  
  # get real physical cores in a computer
  max.cores <- detectCores(logical = TRUE)

  if (is.null(cores)) {
    cores <- max.cores - 1
  }

  if (cores > max.cores) {
    stop(paste0("Number of cores ",cores," more then real physical cores in PC ",max.cores ))
  }

  if (is.null(minblk)) {
    minblk <- wpGetBlocksNeed(x,cores,n=1)
  }

  compareRaster(c(x, y))
  stopifnot(hasValues(x))
  stopifnot(hasValues(y))

  layernames <- names(x)

  blocks <- blockSize(x,minblocks=minblk)

  tStart <- Sys.time()

  cl <- makeCluster(cores)
  
  # broadcast the data and functions to all worker
  # processes by clusterExport
  # clusterExport(cl, c(x,"y", "blocks"))

  registerDoParallel(cl)


  result <- foreach(i = 1:blocks$n, .combine = rbind, .packages='raster') %dopar%
  {

    df.x <- data.frame( getValues(x, row=blocks$row[i], nrows=blocks$nrows[i]) )
    df.y <- data.frame( getValues(y, row=blocks$row[i], nrows=blocks$nrows[i]) )


    if ( fun == 'mean' | fun == 'sd' ) {

      df.fun <- aggregate(x = (df.x), by = list(df.y[,1]), FUN = 'sum', na.rm=na.rm)
      df.length <- aggregate(x = (df.x), by = list(df.y[,1]), FUN = function(x, na.rm=na.rm) length(stats::na.omit(x)), na.rm=na.rm)

      colnames(df.length) <- c(layernames,'length')
      colnames(df.fun) <- c(layernames,'sum')
      
      df <- merge(df.fun, df.length, all = TRUE, by = layernames)
      
      if (fun == 'sd'){
        
        df.sq <- aggregate(x = (df.x^2), by = list(df.y[,1]), FUN='sum', na.rm=na.rm)
        colnames(df.sq) <- c(layernames,'sq')
        df <- merge(df, df.sq, all=TRUE, by=layernames)
        
      }

    } else if ( fun == 'count') {
      
      df <- aggregate(x = (df.x), by = list(df.y[,1]), FUN = function(x, na.rm=na.rm) length(stats::na.omit(x)), na.rm=na.rm)
      
      colnames(df) <- c(layernames,'count')
      
    } else {      

      df <- aggregate(x = (df.x), by = list(df.y[,1]), FUN = fun, na.rm=na.rm)

      colnames(df) <- c(layernames,fun)
    }

    return(df)
  }

  stopCluster(cl)

  if ( fun == 'mean' | fun == 'sd') {

    df1 <- aggregate(x = result$sum, by = list(result[[1]]), FUN = 'sum', na.rm=na.rm)
    df2 <- aggregate(x = result$length, by = list(result[[1]]), FUN = 'sum', na.rm=na.rm)
    df1$x <- df1$x / df2$x
    
    if (fun == 'sd'){
      
      df3 <- aggregate(x = result$sq, by = list(result[[1]]), FUN = 'sum', na.rm=na.rm)
      df1$x <- sqrt(( (df3$x / df2$x) - (df1$x)^2 ) * (df2$x / (df2$x - 1)))
      colnames(df1) <- c(layernames, 'sd')
      
    } else{
      
      colnames(df1) <- c(layernames,'mean')
      
    }

  } else if ( fun == 'count') {
    
    df1 <- aggregate(x = result[[2]], by = list(result[[1]]), FUN = 'sum', na.rm=na.rm)
    
    colnames(df1) <- c(layernames,'count')
    
  }else{

    df1 <- aggregate(x = result[[2]], by = list(result[[1]]), FUN = fun, na.rm=na.rm)
    
    colnames(df1) <- c(layernames,fun)

  }

  tEnd <-  Sys.time()

  if (!silent) print(paste("Elapsed Processing Time:", wpTimeDiff(tStart,tEnd)))

  return(df1)
}
