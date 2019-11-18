#!/usr/bin/env bash
sudo apt-get install r-base
R --no-save << EOT
install.packages('jsonlite', repos='http://cran.rstudio.com/')
EOT

