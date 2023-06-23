#' fof Flow of funds
#'
#' fof Flow of funds (FOF), ALL data, national, from a recent release.
#'
#' @format ## `fof`
#' A data frame with1,673,378 rows and 6 columns:
#' \describe{
#'   \item{date}{Y-d-m format}
#'   \item{name}{fof variable name}
#'   \item{value}{amount, in units}
#'   \item{freq}{A or Q}
#'   \item{description}{FRB description of the variable}
#'   \item{units}{Millions of dollars, etc.}
#' }
#' @source <https://www.federalreserve.gov/releases/z1/default.htm>
#' @examples
#'   head(fof)
"fof"


