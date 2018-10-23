package kafka.practice.constants;

        import org.apache.kafka.common.serialization.StringDeserializer;
        import org.apache.kafka.common.serialization.StringSerializer;

public class Constant {

    public static final String TOPIC_VALUE = "topic-23-oct-18";

    public static final String BOOTSTRAP_SERVERS_VALUE = "localhost:9092";
    public static final String KEY_SERIALIZER_CLASS_VALUE = StringSerializer.class.getName();
    public static final String VALUE_SERIALIZER_CLASS_VALUE = StringSerializer.class.getName();
    public static final String KEY_DESERIALIZER_CLASS_VALUE = StringDeserializer.class.getName();
    public static final String VALUE_DESERIALIZER_CLASS_VALUE = StringDeserializer.class.getName();
}
