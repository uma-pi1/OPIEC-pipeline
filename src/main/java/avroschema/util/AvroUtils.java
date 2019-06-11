package avroschema.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author Mateusz Kobos
 * @author madryk
 * @author Kiril Gashteovski
 */
public class AvroUtils {
    /**
     * URL for (most of) this class:
     *      https://github.com/CeON/spark-utils/blob/master/src/main/java/pl/edu/icm/sparkutils/avro/AvroUtils.java
     */
    public final static String primitiveTypePrefix =
            "org.apache.avro.Schema.Type.";

    /**
     * For a given name of a class generated from Avro schema return a JSON schema.
     *
     * Apart from a name of a class you can also give a name of one of enums defined in {@link org.apache.avro.Schema.Type};
     * in such case an appropriate primitive type will be returned.
     *
     * @param typeName fully qualified name of a class generated from Avro schema, e.g.
     * {@code eu.dnetlib.iis.core.schemas.standardexamples.Person}, or a fully qualified name of enum defined by
     * {@link org.apache.avro.Schema.Type}, e.g. {@link org.apache.avro.Schema.Type.STRING}.
     *
     * @return JSON string
     *
     */
    public static Schema toSchema(String typeName) {
        Schema schema = null;
        if(typeName.startsWith(primitiveTypePrefix)){
            String shortName = typeName.substring(primitiveTypePrefix.length(), typeName.length());
            schema = getPrimitiveTypeSchema(shortName);
        } else {
            schema = getAvroClassSchema(typeName);
        }
        return schema;
    }

    /**
     * Returns cloned avro record.
     */
    @SuppressWarnings("unchecked")
    public static <T extends GenericRecord> T cloneAvroRecord(T record) {

        try {
            Method newRecordBuilderMethod = record.getClass().getMethod("newBuilder", record.getClass());
            Object recordBuilderObject = newRecordBuilderMethod.invoke(null, record);


            Method buildMethod = recordBuilderObject.getClass().getMethod("build");
            Object clonedRecord = buildMethod.invoke(recordBuilderObject);

            return (T) clonedRecord;


        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException |
                InvocationTargetException e) {
            throw new RuntimeException(e);
        }

    }

    private static Schema getPrimitiveTypeSchema(String shortName){
        Schema.Type type = Schema.Type.valueOf(shortName);
        Schema schema = Schema.create(type);
        return schema;
    }

    private static Schema getAvroClassSchema(String className){
        try {
            Class<?> avroClass = Class.forName(className);
            Field f = avroClass.getDeclaredField("SCHEMA$");
            Schema schema = (Schema) f.get(null);
            return schema;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Class \""+className+"\" does not exist");
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static Job getJobOutputKeyAvroSchema(Schema avroSchema) {
        Job job;

        try {
            job = new Job();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        AvroJob.setOutputKeySchema(job, avroSchema);
        return job;
    }

    public static Job getJobInputKeyAvroSchema(Schema avroSchema){
        Job job;

        try {
            job = new Job();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        AvroJob.setInputKeySchema(job, avroSchema);
        return job;
    }
}
