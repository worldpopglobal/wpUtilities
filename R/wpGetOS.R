# Authors: Maksym Bondarenko mb4@soton.ac.uk
# Date :  October 2017
# Version 0.1
#
#' wpGetOS function will return a string with OS
#' of the system
#' Tested on Windows 10

#' @rdname wpGetOS
#' @return string
wpGetOS <- function(){

  sysinf <- Sys.info()

  if (!is.null(sysinf)){

        OS <- tolower(sysinf['sysname'])

        if(OS == 'Windows'){

            return('windows')

        } else if (OS == 'Darwin') {

            return('osx')

        } else if (OS == 'linux') {

            return('linux')

        }

  } else { ## other OS
    OS <- .Platform$OS.type
    if (grepl("^darwin", R.version$os))
      return('osx')
    if (grepl("linux-gnu", R.version$os))
      return('linux')
  }

}
