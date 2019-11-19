#!/usr/bin/env Rscript

# library("magrittr")
# library("jsonlite")

library("SparkR")

sparkR.session()

# Command line arguments: name.terms.json name.df.json
argv <- commandArgs(trailingOnly = TRUE)
print(argv)

terms_filename <- argv[1]
df_filename <- argv[2]
print(terms_filename)
print(df_filename)

# terms <- stream_in(textConnection(readLines(terms_filename)))
# data <- stream_in(textConnection(readLines(df_filename)))

data <- read.json(df_filename, multiline = TRUE)
terms <- read.json(terms_filename, multiline = TRUE)

data_r <- collect(data)
head(data_r$tf)
# summary(terms)
# head(terms)
# head(data)
# summary(data)

