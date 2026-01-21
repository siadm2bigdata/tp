## FIRST PART : KAFKA

docker compose up -d

docker compose ps


docker compose exec kafka kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092

docker exec -it kafka-kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092

python consumer.py

python producer.py


## SECOND PART : SPARK

docker build -t spark-tp .

docker run --rm -it -v $(pwd)/data:/data -v $(pwd)/spark:/app spark-tp spark-submit /app/job_spark.py

