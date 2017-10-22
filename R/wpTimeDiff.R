#  Authors: Maksym Bondarenko mb4@soton.ac.uk
#  Date :  October 2017
#  Version 0.1
#
#' wpTimeDiff function will time difference in humon readable format h:m:s
#'
#'
#' @param start time starting
#' @param end time endfing
#' @param frm format ooutput
#' @rdname wpTimeDiff
#' @return character
#' @export
#' @examples
#' wpTimeDiff( start=Sys.time(), end=Sys.time(), frm="hms" )
#'
wpTimeDiff <- function(start, end, frm="hms") {

  dsec <- as.numeric(difftime(end, start, units = c("secs")))
  hours <- floor(dsec / 3600)

  if (frm == "hms" ){
    minutes <- floor((dsec - 3600 * hours) / 60)
    seconds <- dsec - 3600*hours - 60*minutes

    out=paste0(
      sapply(c(hours, minutes, seconds), function(x) {
        formatC(x, width = 2, format = "d", flag = "0")
      }), collapse = ":")

    return(out)
  }else{
    return(hours)
  }
}
