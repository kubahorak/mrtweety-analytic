FROM gettyimages/spark:2.1.0-hadoop-2.7
ENV SCALA_VERSION 2.11
ENV KAFKA_VERSION 0.10.1.1
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"
RUN apt-get update &&         apt-get install -y zookeeperd wget &&         rm -rf /var/lib/apt/lists/* &&         apt-get clean &&         wget -q http://apache.mirrors.spacedump.net/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz             -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz &&         tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt &&         rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz
RUN apt-get update       && apt-get install -y nginx
EXPOSE 80
RUN rm -rf /var/www/html/*
ADD web /var/www/html
ENV RESULT_FILENAME /var/www/html/analytic.json
COPY producer/build/distributions/producer.tar /
RUN tar xf /producer.tar -C /
COPY spark/build/libs/spark-all.jar /
COPY spark/src/main/resources/log4j.properties /usr/spark-2.1.0/conf/
CMD ["service nginx start         && service zookeeper start         && ($KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &)         && (/producer/bin/producer &)         && (bin/spark-submit /spark-all.jar &)         && tailf /dev/null"]
