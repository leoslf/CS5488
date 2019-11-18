#!/usr/bin/env Rscript

library("magrittr")
library("jsonlite")
# library("rjson")

# Command line arguments: name.terms.json name.df.json
argv <- commandArgs(trailingOnly = TRUE)
print(argv)

terms_filename <- argv[1]
df_filename <- argv[2]
print(terms_filename)
print(df_filename)

terms <- stream_in(textConnection(readLines(terms_filename)))
data <- stream_in(textConnection(readLines(df_filename)))

# print(terms)
# 
# print(data)

summary(terms)
