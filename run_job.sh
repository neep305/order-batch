CURRENT_DATE=`date '+%Y/%m/%d'`
LESSON=$(basename $PWD)
RANDOM=$(date +%s%N | cut -b10-19)
mvn clean package -Dmaven.test.skip=true;
java -jar ./target/batch-0.0.1-SNAPSHOT.jar "run.date(date)=$CURRENT_DATE" "lesson=$LESSON$RANDOM";
read;
