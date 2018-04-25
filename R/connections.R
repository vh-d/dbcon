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


#' Check whether a connections exists
con_exists <- function(con_name) {
  if (dbcon_env_exists() && exists(con_name, envir = .GlobalEnv$.dbcon)) return(TRUE)
  return(FALSE)
}

#' Is the connection open?
con_is_open <- function(con_name, dbname) {
  if (dbcon_env_exists() &&
      con_exists(con_name) &&
      tryCatch(DBI::dbGetInfo(.GlobalEnv$.dbcon[[con_name]]),
               error = function(e) return(list(dbname = "")))$dbname == dbname)
    return(TRUE)
  # else:
  return(FALSE)
}

#' Create connection checker
#' @export
con_checker <- function(con_name, dbname) {
  function() {
    con_is_open(con_name, dbname)
  }
}

# Create function that checks for an existing DB connection and creates/open a new one if its not found open
#' @export
db_connector <- function(con_name,
                         def_driver,
                         checker,
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
    con_args <- append(con_args, users_args)
    # developer's default arguments if not in conflict
    # todo: prevent conflicts by matching (the same way R does)
    if (!is.null(def_args)) con_args <- append(con_args, def_args[!(names(def_args) %in% names(users_args))])

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
db_disconnector <- function(con_name, checker) {
  function() {
    if (dbcon_env_exists()) {
      if (con_exists(con_name)) {
        if (checker()) {
          message("Closing connection to ", con_name, " ...")
          try(DBI::dbDisconnect(conn = .GlobalEnv$.dbcon[[con_name]]))
          message("ok")

          return(TRUE)
        } else {
          message("Connection to ", con_name, " already closed.")
          return(TRUE)
        }
      } else {
        message("Connection to ", con_name, " not found!")
        return(FALSE)
      }
    } else {
      message("Connection to ", con_name, " not found!")
      return(FALSE)
    }
  }
}
