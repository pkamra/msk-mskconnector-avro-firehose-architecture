package com.amazon.kinesis.kafka;

import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;



public class AvroDataUtility{

    private static Integer DEFAULT_SCHEMA_SIZE = 100;
    private static AvroData avroDataHelper = new AvroData(DEFAULT_SCHEMA_SIZE);

//    public AvroDataUtility(Integer schemaCacheSize) {
//        this.avroDataHelper = new AvroData(schemaCacheSize != null ? schemaCacheSize : DEFAULT_SCHEMA_SIZE);
//    }

    /**
     * Parses Kafka Avro Values
     *
     * @param schema
     *            - Schema of passed message as per
     *            https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Schema.html
     * @param value
     *            - Value of the message
     * @return Parsed bytebuffer as per schema type
     */
    public static ByteBuffer parseValue(Schema schema, Object value) {

        DatumWriter<GenericRecord> datumWriter;
        datumWriter = new GenericDatumWriter<>();
        System.out.println("AvroDataUtility#parseValue() enter");
        GenericRecord avroInstance = (GenericRecord) avroDataHelper.fromConnectData(schema, value);

        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        ) {
            dataFileWriter.setCodec(CodecFactory.nullCodec());

            dataFileWriter.create(avroInstance.getSchema(), baos);

            dataFileWriter.append(avroInstance);
            dataFileWriter.flush();
            System.out.println("AvroDataUtility#parseValue() successful");
            return ByteBuffer.wrap(baos.toByteArray());

        } catch (IOException ioe) {
            System.out.println("Error serializing Avro"+ ioe);
            throw new DataException("Error serializing Avro", ioe);
        }
    }

}