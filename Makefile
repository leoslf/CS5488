CMD_HDFS_MERGEPARTS = hdfs dfs -getmerge 
SPARK_SHELL = spark-shell
R = Rscript

HDFS_PROJECT_DIR = project
OUTPUT_DIR = output
OUTPUT_EXTENSION = json
CHARTS_DIR = charts

MAIN_SCRIPT = main.scala
GENERATE_CHART_SCRIPT = generate_chart.R

OUTPUTS_BASE = overall $(shell for i in `seq 5`; do echo "rating_$$i"; done)
OUTPUTS_BASENAME = $(patsubst %, %.terms, $(OUTPUTS_BASE)) $(patsubst %, %.df, $(OUTPUTS_BASE))
OUTPUTS_FILENAME = $(addprefix $(OUTPUT_DIR)/, $(patsubst %, %.$(OUTPUT_EXTENSION), $(OUTPUTS_BASENAME)))
CHARTS_FILENAME = $(addprefix $(CHARTS_DIR)/, $(patsubst %, %.png, $(OUTPUTS_BASE)))

all: setup main outputs charts

setup:
	# @./r_setup.sh
	# @start-dfs.sh
	# @start-yarn.sh
	# @mr-jobhistory-daemon.sh start historyserver
	# @jps

main:
	@rm -f $(OUTPUT_DIR)/*
	$(SPARK_SHELL) -i $(MAIN_SCRIPT)

outputs: $(OUTPUTS_FILENAME) 

$(OUTPUT_DIR)/%.$(OUTPUT_EXTENSION):
	@$(CMD_HDFS_MERGEPARTS) $(HDFS_PROJECT_DIR)/$(notdir $@) $@

charts: $(CHARTS_FILENAME)

$(CHARTS_DIR)/%.png: $(OUTPUT_DIR)/%.terms.$(OUTPUT_EXTENSION) $(OUTPUT_DIR)/%.df.$(OUTPUT_EXTENSION)
	@$(R) $(GENERATE_CHART_SCRIPT) $^

	
