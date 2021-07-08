#!/bin/bash
MAVEN_PATH="/home/gleb/apache-maven-3.6.3/bin/"
CORENLP_PIPELINE_PATH="/home/gleb/ccp-nlp-pipelines"
BATCH_SIZE=$1
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
if ! hash python; then
    echo "python is not installed"
    exit 1
fi
ver=$(python -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
if [ "$ver" -lt "39" ]; then
    echo "This script requires python 3.9 or greater. Please install or create suitable environment."
    exit 1
fi
RAW_FILE="$SCRIPT_DIR/reports.csv"
if [ $(find "$SCRIPT_DIR" -wholename "$RAW_FILE") ]; then
  echo "$RAW_FILE is found in $SCRIPT_DIR"
else
  echo "$RAW_FILE not found. Please provide raw file for further processing."
  exit
fi
CLEAN_FILE="$SCRIPT_DIR/clean_reports.csv"
COORDINATES_FILE="$SCRIPT_DIR/coordinates.csv"
UNPROCESSED_TSV_FILE="$SCRIPT_DIR/unprocessed.csv"
HPO_TSV_FILE="$SCRIPT_DIR/hpo.csv"
FINISHED_TSV_FILE="$SCRIPT_DIR/hpo_ordo.csv"
if [ -d "$SCRIPT_DIR/maven" ]
then
    echo "All necessary directories exist."
else
    mkdir $SCRIPT_DIR/maven
    mkdir $SCRIPT_DIR/maven/temp
    echo "All necessary directories been created."
fi
MAVEN_DIR="$SCRIPT_DIR/maven"
pip install -r "$SCRIPT_DIR/requirements.txt"
python $SCRIPT_DIR/clean_data.py --raw_file $RAW_FILE --clean_file $CLEAN_FILE
python $SCRIPT_DIR/split_for_maven.py --batch_size "$BATCH_SIZE" --clean_file "$CLEAN_FILE" --for_maven_files "$MAVEN_DIR/text_sample_" --coordinates_tsv "$COORDINATES_FILE" --unprocessed_tsv "$UNPROCESSED_TSV_FILE"
$MAVEN_PATH/mvn -f $CORENLP_PIPELINE_PATH/nlp-pipelines-evaluation/pom.xml exec:java -Dexec.mainClass="edu.ucdenver.ccp.nlp.pipelines.conceptmapper.EntityFinder" -Dexec.args="$MAVEN_DIR $MAVEN_DIR OBO $SCRIPT_DIR/OBO_Files/hp.owl $MAVEN_DIR/temp True"
python $SCRIPT_DIR/reverse_match_splitted.py --batch_size "$BATCH_SIZE" --func_name hpo --unprocessed_tsv "$UNPROCESSED_TSV_FILE" --coordinates_tsv "$COORDINATES_FILE" --hpo_tsv "$HPO_TSV_FILE" --finished_tsv "$FINISHED_TSV_FILE" --temp_directory "$MAVEN_DIR/"
$MAVEN_PATH/mvn -f $CORENLP_PIPELINE_PATH/nlp-pipelines-evaluation/pom.xml exec:java -Dexec.mainClass="edu.ucdenver.ccp.nlp.pipelines.conceptmapper.EntityFinder" -Dexec.args="$MAVEN_DIR $MAVEN_DIR OBO $SCRIPT_DIR/OBO_Files/owlapi.xrdf $MAVEN_DIR/temp True"
python $SCRIPT_DIR/reverse_match_splitted.py --batch_size "$BATCH_SIZE" --func_name ordo --unprocessed_tsv "$UNPROCESSED_TSV_FILE" --coordinates_tsv "$COORDINATES_FILE" --hpo_tsv "$HPO_TSV_FILE" --finished_tsv "$FINISHED_TSV_FILE" --temp_directory "$MAVEN_DIR/"
python $SCRIPT_DIR/count_from_parsed_parallel.py --finished_tsv "$FINISHED_TSV_FILE" --ordo_table "$SCRIPT_DIR/ordo_table.csv" --hpo_table "$SCRIPT_DIR/hpo_table.csv"
