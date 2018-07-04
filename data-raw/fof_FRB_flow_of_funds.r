
# 07/04/2018

#****************************************************************************************************
#                Libraries and global functions ####
#****************************************************************************************************
library("magrittr")
library("plyr") # needed for ldply; must be loaded BEFORE dplyr
library("tidyverse")
options(tibble.print_max = 60, tibble.print_min = 60) # if more than 60 rows, print 60 - enough for states
# ggplot2 tibble tidyr readr purrr dplyr


library("hms") # hms, for times.
library("stringr") # stringr, for strings.
library("lubridate") # lubridate, for date/times.
library("forcats") # forcats, for factors.
library("readxl") # readxl, for .xls and .xlsx files.
library("btools")


#****************************************************************************************************
#                Download Flow of Funds data ####
#****************************************************************************************************
# fget <- "https://www.federalreserve.gov/releases/z1/current/z1_csv_files.zip"
# fget <- "https://www.federalreserve.gov/releases/z1/20180308/z1_csv_files.zip"
# https://www.federalreserve.gov/releases/z1/20180308/z1_csv_files.zip

# Go to:
#   https://www.federalreserve.gov/releases/z1/
# and get url of latest release

fget <- "https://www.federalreserve.gov/releases/z1/20180607/z1_csv_files.zip"

zfn <- "z1_csv_files.zip"

temp <- tempfile()
download.file(fget, temp, mode="wb")

lfiles <- unzip(temp, list=TRUE) # data frame of files in the zip archive
lfiles
csvfiles <- str_subset(lfiles$Name, "csv/")
ddfiles <- str_subset(lfiles$Name, "data_dictionary/")


#****************************************************************************************************
#                first get the csv files ####
#****************************************************************************************************
fcsv <- function(fn) {
  print(fn)
  tmp <- read_csv(unz(temp, fn))
  if(str_detect(tmp$date[1], "Q")) freq <- "Q" else
    freq <- "A"
  tmp <- tmp %>% mutate(src=str_sub(fn, 5, -5), date=as.character(date), freq=freq)
  tmpl <- tmp %>% gather(variable, value, -date, -freq, -src) %>%
    mutate(value=cton(value))
  # some files have 4 versions of the same data, all with same numbers
  # gather puts _1, _2, _3, _4 on variable
  # get unique values
  tmplu <- tmpl %>% mutate(variable=ifelse(str_detect(variable, "_"), str_sub(variable, 1, -3), variable)) %>%
    unique
  return(tmplu)
}
csvdf <- ldply(csvfiles, fcsv, .progress="text") %>% as_tibble()

csvdf2 <- csvdf %>% mutate(olddate=date,
                     date=as.Date(parse_date_time(date, orders = "Yq")),
                     date=safe.ifelse(freq=="A", as.Date(paste0(olddate, "-01-01")), date)) %>%
  select(-olddate)

# quick check to be sure we only have Q and A
csvdf2 %>% mutate(l2=str_sub(variable, -2, -1)) %>%
  group_by(freq, l2) %>%
  summarise(n=n())

csvdf3 <- csvdf2 %>% mutate(variable=str_sub(variable, 1, -3),
                            year=as.integer(year(date))) %>%
  filter(!is.na(value))
csvdf3


#****************************************************************************************************
#                now the data dictionary files ####
#****************************************************************************************************
fdd <- function(fn) {
  print(fn)
  cnames <- c("variable", "description", "line_rowcol", "table", "units")
  tmp <- read_tsv(unz(temp, fn), col_names=cnames)
  tmp <- tmp %>% mutate(src=str_sub(fn, 17, -5),
                        variable=str_sub(variable, 1, -3))
  return(tmp)
}

dd.df <- ldply(ddfiles, fdd, .progress="text") %>% as_tibble()
glimpse(dd.df)

dd.df2 <- dd.df %>%
  mutate(lineno=ifelse(str_detect(line_rowcol, "Line"), str_sub(line_rowcol, 5, -1), NA) %>%
           as.numeric)

# unique(dd.df$src)


#****************************************************************************************************
#                Create fof file and save ####
#****************************************************************************************************
fof <- csvdf3 %>% left_join(dd.df2) %>%
  select(date, year, freq, variable, value, src, line_rowcol, lineno, description, table, units) %>%
  mutate_at(vars(src, description, freq, variable, description, line_rowcol, table, units), factor) # reduces size by ~ 1/3
glimpse(fof)

fof %>% filter(src=="l120b", date==max(date)) %>% arrange(lineno)


max(fof$date)
# count(fof, freq)
# count(fof, is.na(value))

devtools::use_data(fof, overwrite=TRUE)


#****************************************************************************************************
#                Create a second file, fofu, with unique values, and save ####
#****************************************************************************************************
# since fof variables can appear in multiple tables, there are multiple copies in the fof file
# fofu has unique values - it is a slimmed down file

# create a "best" data definition for each variable
dd.type <- dd.df2 %>%
  mutate(srctype=ifelse(str_sub(src, 1, 1)=="l", "level", NA),
         srctype=ifelse(str_sub(src, 1, 1)=="f", "flow", srctype),
         srctype=ifelse(str_sub(src, 1, 1)=="b", "balance", srctype),
         srctype=ifelse(str_sub(src, 1, 1)=="d", "debt", srctype),
         srctype=ifelse(str_sub(src, 1, 1)=="r", "change", srctype),
         srctype=ifelse(str_sub(src, 1, 1)=="a", "fofmatrix", srctype),
         srctype=ifelse(str_sub(src, 1, 1)=="s", "supplement", srctype),
         srctype=ifelse(is.na(srctype), "other", srctype))
count(dd.type, srctype) # good - no others
glimpse(dd.type)

vpriority <- c("level", "flow", "balance", "debt", "change", "fofmatrix", "supplement")
dd.best <- dd.type %>%
  mutate(priority=factor(srctype, levels=vpriority, labels=1:length(vpriority)) %>%
           as.integer) %>%
  group_by(variable) %>%
  arrange(priority, lineno) %>%  # note that NA comes last when sorting
  filter(row_number()==1)

glimpse(dd.best)

# data_frame(a=c(1, 2, NA, 3, 1)) %>% arrange(a) # note that NA comes last when sorting

# get the unique data
glimpse(fof)
datau <- fof %>% select(date, year, freq, variable, value) %>% unique
anyDuplicated(datau %>% select(-value)) # good, there are no cases of different values

# now put the best description for each variable on the data
intersect(names(datau), names(dd.best))
fofu <- datau %>%
  left_join(dd.best %>% select(-priority, -src, -srctype, -table, -lineno, -line_rowcol))
glimpse(fofu)

memory()

devtools::use_data(fofu, overwrite=TRUE)

#****************************************************************************************************
#                CAUTION: ONLY unlink when all done, or file will have to be re-downloaded ####
#****************************************************************************************************
unlink(temp)

