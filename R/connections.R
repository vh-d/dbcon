#' Test a DBI connection
#' @export
test_con <- function(dbcon) {
  return(
    try(
      DBI::dbGetQuery(conn      = dbcon,
                      statement = "SELECT 1 FROM DUAL"), # todo: some more universal select for any type db?
      silent = TRUE)
    == 1)
}


#' Check whether .dbcon environment (for storing connections) exists
#' @export
dbcon_env_exists <- function(){
  if (exists(".dbcon", envir = .GlobalEnv)) return(TRUE)
  return(FALSE)
}

#' Get/find a connection by name
#' @export
get_con <- function(con_name) {
  if (dbcon_env_exists()) {
    if (exists(con_name, envir = .GlobalEnv[[".dbcon"]])) {
      return(.GlobalEnv[[".dbcon"]][[con_name]])
    } else stop("Connection ", con_name, "not found.")
  } else stop("Connection ", con_name, "not found. In particular '.dbcon' environment does not exist (yet)!")
}

#' Check whether a connections exists
con_exists <- function(con_name) {
  if (dbcon_env_exists() && exists(con_name, envir = .GlobalEnv$.dbcon)) return(TRUE)
  return(FALSE)
}

#' Is the connection open?
con_is_open <- function(con_name) {
  dbcon_env_exists() &&
    con_exists(con_name) &&
    tryCatch(is.list(DBI::dbGetInfo(.GlobalEnv$.dbcon[[con_name]])),
             error = function(e) return(FALSE))
}

#' Create connection checker
con_checker <- function(con_name) {
  function() {
    con_is_open(con_name)
  }
}

# Create function that checks for an existing DB connection and creates/open a new one if its not found open
#' @export
db_connector <- function(con_name,
                         def_driver,
                         checker = con_checker(con_name = con_name),
                         def_args = NULL) {

  function(...,
           drv = def_driver) {

    if (dbcon_env_exists()) {
      if (checker()) return(invisible(TRUE))
    } else {
      .GlobalEnv$.dbcon <- new.env()
    }

    # arguments:
    con_args <- list(drv = drv)
    # user's new arguments
    users_args <- list(...)
    con_args <- c(con_args, users_args)
    # developer's default arguments if not in conflict
    # todo: prevent conflicts by matching (the same way R does)
    if (!is.null(def_args)) con_args <- c(con_args, def_args[setdiff(names(def_args), names(users_args))])

    # try to connect
    # todo: exceptions
    tryCatch(new_con <- do.call(DBI::dbConnect, args = con_args),
             error = function(e) stop("Error while connecting to ", con_name, ": ", e))

    # save it to a known environment
    .GlobalEnv$.dbcon[[con_name]] <- new_con

    # exit
    return(invisible(TRUE))
  }
}

# todo: more granular testing: 1. environment exists 2. connection exists 3. connection is open/closed
#' Create DBI disconnetor
#' @export
db_disconnector <- function(con_name,
                            checker = con_checker(con_name = con_name)) {
  function() {
    if (dbcon_env_exists()) {
      if (con_exists(con_name)) {
        if (checker()) {
          message("Closing connection to ", con_name, " ...", appendLF = FALSE)
          try(DBI::dbDisconnect(conn = .GlobalEnv$.dbcon[[con_name]]))
          message("ok")

          return(invisible(TRUE))
        } else {
          message("Connection to ", con_name, " already closed.")
          return(invisible(TRUE))
        }
      } else {
        message("Connection to ", con_name, " not found!")
        return(invisible(FALSE))
      }
    } else {
      message("Connection to ", con_name, " not found!")
      return(invisible(FALSE))
    }
  }
}
