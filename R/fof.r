#' fof Flow of funds.
#'
#' fof Flow of funds (FOF), ALL data, national, from a recent release.
#'
#' @source Federal Reserve Board Z1 release (http://www.federalreserve.gov/releases/z1/current/)
#'
#' Download latest csv zip release from https://www.federalreserve.gov/releases/z1/, read and combine files
#' @format Data frame with 1 row per variable per date, NA records have been removed.
#' \describe{
#' \item{date}{date: first day of quarter for Q data, first day of year for A data}
#' \item{year}{integer: calendar year}
#' \item{freq}{factor: Frequency of data (A or Q)}
#' \item{variable}{factor: FOF variable name, EXCLUDING suffix that denotes frequency}
#' \item{value}{double: $ amount, in units described in FOF documentation}
#' \item{src}{factor: Name of the file, within the zip archive, from which the data element came - usually signifies table number}
#' \item{line_rowcol}{character: Line number or row-column in Flow of Funds table or matrix}
#' \item{lineno}{numeric: Line number in table}
#' \item{description}{factor: Long description of variable, factor}
#' \item{table}{factor: Flow of Funds table or matrix name}
#' \item{units}{factor: Units - e.g., Millions of dollars}
#' }
#' @examples
#'   head(fof)
"fof"

