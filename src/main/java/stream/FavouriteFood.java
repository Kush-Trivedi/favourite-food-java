package stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteFood {
    public static void main(String[] args) {
        Properties propertiesConfig = new Properties();
        propertiesConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"favourite-food-java");
        propertiesConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        propertiesConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        propertiesConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        propertiesConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> kStreamTextLine = streamsBuilder.stream("favourite-food-input");
        KStream<String,String> usersAndFoods = kStreamTextLine
                .filter((key,value) -> value.contains(","))
                .selectKey((key,value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user,food) -> Arrays.asList("Indian","Mexican","Chinese").contains(food));
        usersAndFoods.to("user-keys-foods");

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KTable<String,String> usersAndFoodsTable = streamsBuilder.table("user-keys-foods");
        KTable<String,Long> favouriteFood = usersAndFoodsTable
                .groupBy((user,food) -> new KeyValue<>(food,food))
                .count(Materialized.as("CountsByFood"));
        favouriteFood.toStream().to("favourite-food-output", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),propertiesConfig);
        //kafkaStreams.cleanUp();
        kafkaStreams.start();
        kafkaStreams.localThreadsMetadata().forEach(System.out::println);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}

