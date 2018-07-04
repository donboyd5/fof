#' fofu Flow of funds - unique values.
#'
#' fofu Flow of funds (FOF), ALL data, national, from a recent release, only the unique values.
#'
#' @source Federal Reserve Board Z1 release (http://www.federalreserve.gov/releases/z1/current/)
#'
#' Download latest csv zip release from https://www.federalreserve.gov/releases/z1/, read and combine files
#' @format Data frame with 1 row per variable per date, NA and duplicate records have been removed.
#' \describe{
#' \item{date}{date: first day of quarter for Q data, first day of year for A data}
#' \item{year}{integer: calendar year}
#' \item{freq}{factor: Frequency of data (A or Q)}
#' \item{variable}{factor: FOF variable name, EXCLUDING suffix that denotes frequency}
#' \item{value}{double: $ amount, in units described in FOF documentation}
#' \item{description}{factor: Long description of variable, factor}
#' \item{units}{factor: Units - e.g., Millions of dollars}
#' }
#' @examples
#'   head(fofu)
"fofu"

