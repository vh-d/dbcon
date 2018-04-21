#' login dialog asking for username and password
#' alternative to getPass() from `getPass` package
#' @param title title of the dialog
#' @param default_user default user name
#' @export
login_dialog <- function(title = "", default_user = "") {

    require(gWidgets2tcltk)
    # require(gWidgets2)

    main <- gwindow(title,
                    width = 400,
                    height= 200,
                    visible = FALSE,
                    toolkit = "tcltk")       ## a parent container
    box <- gvbox(cont = main)

    row1 <- ggroup(horizontal = FALSE, container=box)
    lbl_username <- glabel(container=row1, text="Username: ")
    txt_username <- gedit(text = default_user, container=row1)

    # a row and components
    row2 <- ggroup(horizontal = FALSE, container=box)
    lbl_password <- glabel(container=row2, text="Password: ")
    txt_password <- gedit(container=row2)
    visible(txt_password) <- FALSE

    # a row for button
    row3 <- ggroup(horizontal = FALSE, container=box)
    btn_login  <- gbutton(container = row3, text="Login")
    btn_cancel <- gbutton(container = row3, text="cancel")

    # Event handler for login button
    do_login <- function(obj){
      dispose(main)
      .GlobalEnv$dialogOpen <- FALSE
    }

    # Event handler for register button
    do_cancel <- function(obj){
      dispose(main)
      .GlobalEnv$dialogOpen <- FALSE
    }

    # Registering Events
    addHandlerClicked(btn_login,  do_login)
    addHandlerClicked(btn_cancel, do_cancel)

    # try to login after enter is hit in one of the edit fields
    # addHandlerChanged(txt_username, do_login)
    addHandlerChanged(txt_password, do_login)

    visible(main) <- TRUE
    .GlobalEnv$dialogOpen <- TRUE

    while (dialogOpen) {

    }

    return(
      list(username = svalue(txt_username),
           password = svalue(txt_password)))
}
