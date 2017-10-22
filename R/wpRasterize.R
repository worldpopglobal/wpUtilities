# Authors: Maksym Bondarenko mb4@soton.ac.uk
# Date :  October 2017
# Version 0.1
#
#' wpStartRasterize function will return a string with OS
#' @param x Raster* object
#' @param df data.frame of points
#' @param minblk Integer. Minimum number of blocks
#' @param NAflag NO data value will be used for a new raster
#' @param datatype Type of raster. Avalible are INT1S/INT2S/INT4S/FLT4S/LOG1S/INT1U/INT2U/INT4U/FLT8S
#' @param filename File of a new raster file.
#' @param overwrite Overwrite existing file
#' @param silent If FALSE then the progress will be shown
#' @rdname wpStartRasterize
#' @return Raster* object
wpStartRasterize <- function(x,
                             df,
                             minblk,
                             NAflag,
                             datatype,
                             filename,
                             overwrite=TRUE,
                             silent=TRUE) {

  tStart <- Sys.time()

  layernames <- names(x)


  blocks <- blockSize(x,minblocks=minblk)

  cat('\n')
  cat('Rasterising')
  # To let user know how many blocks will be used
  if (minblk != blocks$n) cat(paste0('\nTotal blocks ',blocks$n))
  cat('\n')

  cl <- getCluster()

  #on.exit( returnCluster() )
  nodes <- length(cl)


  if (is.null(minblk)) {
    minblk <- nodes
  }

  clusterExport(cl, c("blocks", "x","df"), envir=environment())


        clRasteriseFun <- function(i) {
          # tryCatch({
                v <- data.frame( getValues(x, row=blocks$row[i], nrows=blocks$nrows[i]) )
                colnames(v) <- c("v1")
                colnames(df) <- c("v1","v2")
                v <- plyr::join(v,df,type="left",by = "v1")[-1]

          #  }, error = function(e) return(paste0("The block '", blocks$row[i], "'",
          #                                       " caused the error: '", e, "'")))

          return(v[[1]])
        }

  # get all nodes going
  for (i in 1:nodes) {
    sendCall(cl[[i]], clRasteriseFun, i, tag=i)
  }

  out <- setValues(x, 0)

  out <- writeStart(out,
                    filename=filename,
                    format="GTiff",
                    datatype=datatype,
                    overwrite=overwrite,
                    options=c("COMPRESS=LZW"),
                    NAflag=NAflag)

  for (i in 1:blocks$n) {

    d <- recvOneData(cl)

    if (! d$value$success) {
      stop('cluster error')
    }

    tEnd <-  Sys.time()

    b <- d$value$tag

    wpProgressMessage(i, max=blocks$n, label= paste0("received block ",b, " Processing Time: ", wpTimeDiff(tStart,tEnd)))

    out <- writeValues(out, d$value$value, blocks$row[b])

    # need to send more data
    #
    ni <- nodes + i
    if (ni <= blocks$n) {
      sendCall(cl[[d$node]], clRasteriseFun, ni, tag=ni)
    }
  }

  out <- writeStop(out)

  return(out)
}


#' wpRasterize function will transfer values associated with 'object' type spatial data (data.frame)
#' to raster cells. Function is using paralle library to work with a big raster data. The raster
#' file will be splited to blocks and processed per block
#'
#' @param x Raster* object
#' @param df data.frame of points
#' @param cores number of cores
#' @param minblk Integer. Minimum number of blocks
#' @param NAflag NO data value will be used for a new raster
#' @param datatype Type of raster. Avalible are INT1S/INT2S/INT4S/FLT4S/LOG1S/INT1U/INT2U/INT4U/FLT8S
#' @param filename File of a new raster file.
#' @param overwrite Overwrite existing file
#' @param silent If FALSE then the progress will be shown
#' @rdname wpRasterize
#' @return Raster* object
#' @export
#' @examples
#' wpRasterize( x=rasterObj, df=df, cores=2,minblk=4,NAflag=-99999,datatype='INT1U' )
#'
wpRasterize<- function(x,
                       df,
                       cores=NULL,
                       minblk=NULL,
                       NAflag=NULL,
                       datatype=NULL,
                       filename=rasterTmpFile(),
                       overwrite=TRUE,
                       silent=TRUE) {


  stopifnot(hasValues(x))

  # get real physical cores
  max.cores <- detectCores(logical = TRUE)

  if (is.null(NAflag)) NAflag=-99999
  if (is.null(datatype)) datatype='FLT4S'

  # if user did not tell how many cores to use then max-1 will be taken
  #
  if (is.null(cores)) {
    cores <- max.cores - 1
  }

  # if user did not tell how many minblk to use then minblk will
  # calculated by wpGetBlocksNeed() function
  #
  if (is.null(minblk)) {
    minblk <- wpGetBlocksNeed(x,cores,n=1)
  }

  cores <- as.integer(cores)
  minblk <- as.integer(minblk)

  if (cores > max.cores) {
    stop(paste0("Number of cores ",cores," more then real physical cores in PC ",max.cores ))
  }

  if (!is.data.frame(df)) stop(paste0("df should be a data.frame"))
  if (!is(NAflag, "numeric")) stop(paste0("NAflag should be  numeric"))
  if (!is(cores,  "integer")) stop(paste0("cores should be integer value"))
  if (!is(minblk, "integer")) stop(paste0("minblk should be integer"))
  if (!is(overwrite, "logical")) stop(paste0("overwrite should be  logical (e.g., TRUE, FALSE)"))
  if (!is(silent, "logical")) stop(paste0("silent should be logical (e.g., TRUE, FALSE)"))

  datatype <- toupper(datatype)

  if (!(datatype %in% c('INT1S', 'INT2S', 'INT4S', 'FLT4S', 'LOG1S', 'INT1U', 'INT2U', 'INT4U', 'FLT8S'))) {
    stop('not a valid data type. Avalible are INT1S/INT2S/INT4S/FLT4S/LOG1S/INT1U/INT2U/INT4U/FLT8S')
  }

  if (!file.exists(dirname(filename))){
    stop(paste0("Directory  ",dirname(filename)," for file ", basename(filename) ," does not exist"))
  }

  if ( file.exists(filename) & overwrite==FALSE) {
    stop(paste0("File ",filename," exist. Use option overwrite=TRUE"))
  } else{
    if ( file.exists(filename) ) file.remove(filename)
  }

  if (cores > max.cores) {
    stop(paste0("Number of cores ",cores," more then real physical cores in PC ",max.cores ))
  }

  beginCluster(n=cores)

  out <- wpStartRasterize(x, df, minblk, NAflag, datatype, filename, overwrite, silent)

  endCluster()

  return(out)
}
