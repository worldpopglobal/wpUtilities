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
#' @param silent If FALSE then the progress will be shown
#' @rdname wpZonalStatistics
#' @return A data.frame with a value for each zone (unique value in zones)
#' @export
#' @examples
#' wpZonalStatistics( x=rasterObj1, y=rasterObj2, cores=2, minblk=4  )
#'
wpZonalStatistics <- function(x, y, fun='mean', cores=NULL, minblk=NULL, silent=TRUE) {

  #chack_pkg_load("raster","doParallel")

  fun <- tolower(fun)

  if (! fun %in% c('sum', 'mean', 'min', 'max', 'count')) {
    stop("fun can be 'sum', 'mean', 'min', 'max', or 'count'")
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


    if ( fun == 'mean' ) {

      df.fun <- aggregate(x = (df.x), by = list(df.y[,1]), FUN = 'sum')
      df.length<- aggregate(x = (df.x), by = list(df.y[,1]), FUN = "length")

      colnames(df.length) <- c(layernames,'length')
      colnames(df.fun) <- c(layernames,'sum')

      df <- merge(df.fun, df.length, all = TRUE, by = layernames)

    } else if ( fun == 'max' | fun == 'min' | fun == 'sum') {

      df <- aggregate(x = (df.x), by = list(df.y[,1]), FUN = fun)

      colnames(df) <- c(layernames,fun)
    }

    return(df)
  }

  stopCluster(cl)

  if ( fun == 'mean' ) {

    df1 <- aggregate(x = result$sum, by = list(result[[1]]), FUN = "sum")
    df2 <- aggregate(x = result$length, by = list(result[[1]]), FUN = "sum")
    df1$x <- df1$x/df2$x
    colnames(df1) <- c(layernames,'mean')

  }else{

    df1 <- aggregate(x = result[[2]], by = list(result[[1]]), FUN = fun)
    colnames(df1) <- c(layernames,fun)

  }

  tEnd <-  Sys.time()

  if (!silent) print(paste("Elapsed Processing Time:", wpTimeDiff(tStart,tEnd)))

  return(df1)
}
