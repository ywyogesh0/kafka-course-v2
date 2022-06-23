package kafka.practice.constants;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Constant {

    public static final String TOPIC_VALUE = "NPA.PFFC.PAYMENTS.FPS.REPORT.CIJOURNAL.REPSERVICES.PROCESSING.STATUS";

    public static final String BOOTSTRAP_SERVERS_VALUE = "kafka.sbs-bld.oncp.dev:9092";
    public static final String KEY_SERIALIZER_CLASS_VALUE = StringSerializer.class.getName();
    public static final String VALUE_SERIALIZER_CLASS_VALUE = StringSerializer.class.getName();
    public static final String KEY_DESERIALIZER_CLASS_VALUE = StringDeserializer.class.getName();
    public static final String VALUE_DESERIALIZER_CLASS_VALUE = StringDeserializer.class.getName();
    public static final String AUTO_OFFSET_RESET_VALUE = "earliest";
    public static final String GROUP_ID_VALUE = "connect-npa-consumer-1";
    public static final String PRODUCER_CLIENT_ID = "c3-producer-1";
    public static final String CONSUMER_CLIENT_ID = "connect-npa-consumer-1";
    public static final String ACKS_VALUE = "all";
    public static final String AUTO_CREATE_TOPICS_ENABLE = "auto.create.topics.enable";
    public static final String AUTO_CREATE_TOPICS_ENABLE_VALUE = "false";
    public static final String REQUEST_TIMEOUT_MS_VALUE = "300000"; // 5 minutes
    public static final String SASL_MECHANISM_KEY = "sasl.mechanism";
    public static final String SASL_MECHANISM_VALUE = "PLAIN";
    public static final String SECURITY_PROTOCOL_KEY = "security.protocol";
    public static final String SECURITY_PROTOCOL_VALUE = "SASL_PLAINTEXT";
    public static final String SASL_JAAS_CONFIG_KEY = "sasl.jaas.config";
    public static final String SASL_JAAS_CONFIG_VALUE = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"npauser\" password=\"npauser-secret\";";
}
