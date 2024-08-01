package com.example.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The following class makes use of the instructions found on the Apache Avro Documentation.
 * @See: <a href="https://avro.apache.org/docs/1.11.1/getting-started-java/">Apache Avro: Getting Started with Java</a>
 */
public class GenericRecordBasicExamples {


    public static GenericRecord genericRecordCreation() throws Exception {
        InputStream inputStream = GenericRecordBasicExamples.class.getClassLoader().getResourceAsStream("avro/sensordata.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);

        GenericRecord sensorData = new GenericData.Record(schema);

        sensorData.put("sensorId", "sensor_1");
        sensorData.put("timestamp", System.currentTimeMillis());
        sensorData.put("isActive", true);
        sensorData.put("temperature", 21.3f);
        sensorData.put("humidity", 90.2);
        sensorData.put("firmwareVersion", ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        sensorData.put("statusMessage", "Operating normally");
        sensorData.put("errorCount", 0);
        return sensorData;
    }


    public static GenericRecord genericRecordBuilderCreation() throws Exception {
        InputStream inputStream = GenericRecordBasicExamples.class.getClassLoader().getResourceAsStream("avro/sensordata.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);

        GenericRecordBuilder sensorDataBuilder = new GenericRecordBuilder(schema);

        sensorDataBuilder.set("sensorId", "sensor_2");
        sensorDataBuilder.set("timestamp", System.currentTimeMillis());
        sensorDataBuilder.set("isActive", true);
        sensorDataBuilder.set("temperature", 26.2f);
        sensorDataBuilder.set("humidity", 75.2);
        sensorDataBuilder.set("firmwareVersion", ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        sensorDataBuilder.set("statusMessage", "Operating normally");
        sensorDataBuilder.set("errorCount", 0);

        GenericData.Record sensorData = sensorDataBuilder.build();
        return sensorData;
    }


    public static GenericRecord genericRecordJSONDecoder() throws Exception {
        InputStream inputStream = GenericRecordBasicExamples.class.getClassLoader().getResourceAsStream("avro/sensordata.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);

        String jsonString = "{\"sensorId\": \"sensor_3\", \"timestamp\": 1690899543000, \"isActive\": true, \"temperature\": 25.1, \"humidity\": 73.2, \"firmwareVersion\": \"AQIDBA==\", \"statusMessage\": \"Operating normally\", \"errorCount\": 0}";

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord record = reader.read(null, decoder);

        return record;
    }


    public static void genericRecordJSONEncoder(GenericRecord sensorData) throws Exception {
        Encoder encoder = EncoderFactory.get().jsonEncoder(sensorData.getSchema(), System.out);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        System.out.println(outputStream.toString("UTF-8"));
    }



    public static void genericRecordSerializing(GenericRecord sensorData) throws Exception {
        InputStream inputStream = GenericRecordBasicExamples.class.getClassLoader().getResourceAsStream("avro/sensordata.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);
        File avroFile = getTargetDirectoryPath().resolve("generic-sensordata.avro").toFile();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, avroFile);
        dataFileWriter.append(sensorData);
        dataFileWriter.close();
    }


    public static void genericRecordDeserializing() throws Exception {
        InputStream inputStream = GenericRecordBasicExamples.class.getClassLoader().getResourceAsStream("avro/sensordata.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);
        File avroFile = getTargetDirectoryPath().resolve("generic-sensordata.avro").toFile();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
        GenericRecord sensorData = null;
        while (dataFileReader.hasNext()) {
            sensorData = dataFileReader.next(sensorData);
            System.out.println(sensorData);
        }
    }


    public static Path getTargetDirectoryPath() throws Exception{
        Path classFilePath = Paths.get(GenericRecordBasicExamples.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path moduleRootPath = classFilePath.getParent().getParent();

        return moduleRootPath.resolve("target/");
    }


    public static void main(String[] args) throws Exception {
        GenericRecord genericRecord = GenericRecordBasicExamples.genericRecordCreation();
        GenericRecordBasicExamples.genericRecordSerializing(genericRecord);
        GenericRecordBasicExamples.genericRecordDeserializing();

        genericRecord = GenericRecordBasicExamples.genericRecordBuilderCreation();
        GenericRecordBasicExamples.genericRecordSerializing(genericRecord);
        GenericRecordBasicExamples.genericRecordDeserializing();

        genericRecord = GenericRecordBasicExamples.genericRecordJSONDecoder();
        GenericRecordBasicExamples.genericRecordJSONEncoder(genericRecord);
        GenericRecordBasicExamples.genericRecordSerializing(genericRecord);
        GenericRecordBasicExamples.genericRecordDeserializing();
    }


}
