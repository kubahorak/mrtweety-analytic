FROM gettyimages/spark:1.6.2-hadoop-2.6

# Install nginx
RUN apt-get update \
  && apt-get install -y nginx

EXPOSE 80

# Copy website
RUN rm -rf /var/www/html/*
ADD web /var/www/html

# Run Spark app
COPY spark/build/libs/spark-all.jar /
COPY spark/src/main/resources/log4j.properties /usr/spark-1.6.2/conf/

ENV RESULT_FILENAME=/var/www/html/analytic.json

CMD service nginx start && bin/spark-submit /spark-all.jar
