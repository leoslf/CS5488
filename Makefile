CMD_HDFS_MERGEPARTS = hdfs dfs -getmerge 
SPARK_SHELL = spark-shell
R = Rscript

HDFS_PROJECT_DIR = project
OUTPUT_DIR = output
# OUTPUT_EXTENSION = json
OUTPUT_EXTENSION = csv
CHARTS_DIR = charts

MAIN_SCRIPT = main.scala
GENERATE_CHART_SCRIPT = generate_chart.R

OUTPUTS_BASE = overall $(shell for i in `seq 5`; do echo "rating_$$i"; done)
OUTPUTS_BASENAME = $(patsubst %, %.terms, $(OUTPUTS_BASE)) $(patsubst %, %.tf, $(OUTPUTS_BASE)) $(patsubst %, %.tfidf, $(OUTPUTS_BASE))  # $(patsubst %, %.documents, $(OUTPUTS_BASE))
OUTPUTS_BASENAME_FOR_JSON = $(patsubst %, %.documents, $(OUTPUTS_BASE)) $(patsubst %, %.lda, $(OUTPUTS_BASE))
OUTPUTS_FILENAME = $(addprefix $(OUTPUT_DIR)/, $(patsubst %, %.$(OUTPUT_EXTENSION), $(OUTPUTS_BASENAME))) $(addprefix $(OUTPUT_DIR)/, $(patsubst %, %.json, $(OUTPUTS_BASENAME_FOR_JSON)))
# OUTPUTS_FILENAME = $(addprefix $(OUTPUT_DIR)/, $(patsubst %, %.$(OUTPUT_EXTENSION), $(patsubst %, %.terms, $(OUTPUTS_BASE))) $(patsubst %, %.json, $(patsubst %, %.documents, $(OUTPUTS_BASE))))
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

$(OUTPUT_DIR)/%.json:
	@$(CMD_HDFS_MERGEPARTS) $(HDFS_PROJECT_DIR)/$(notdir $@) $@

charts: $(CHARTS_FILENAME)

$(CHARTS_DIR)/%.png: $(OUTPUT_DIR)/%.terms.$(OUTPUT_EXTENSION) $(OUTPUT_DIR)/%.tf.$(OUTPUT_EXTENSION) $(OUTPUT_DIR)/%.tfidf.$(OUTPUT_EXTENSION)
	# $(OUTPUT_DIR)/%.documents.json
	# $(OUTPUT_EXTENSION)
	@$(R) $(GENERATE_CHART_SCRIPT) $^

# $(CHARTS_DIR)/%.png: 
# 	@$(R) $(GENERATE_CHART_SCRIPT) $(HDFS_PROJECT_DIR)/$(basename $(notdir $@)).terms.$(OUTPUT_EXTENSION) # $(HDFS_PROJECT_DIR)/$(basename $(notdir $@)).df.$(OUTPUT_EXTENSION)

	
