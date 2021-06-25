package com.github.windstill.kafka.connect.transform;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.connect.data.Schema.Type.STRUCT;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Created by Li Yuan at 2021/6/8
 */
public abstract class ToJson<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(ToJson.class);
    public static final String OVERVIEW_DOC = "Transform field(s) from struct json string.";
    public static final String FIELDS_CONF = "fields";
    public static final String FIELDS_DOC = "The fields(s) that will be transform.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONF, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, FIELDS_DOC);

    private static final String PURPOSE = "cast struct to json";
    private List<String> transFields;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        transFields = config.getList(FIELDS_CONF);
        schemaUpdateCache = new SynchronizedCache<Schema, Schema>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        }

        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap(value);

        for (String field:transFields) {
            JSON jsonField = parseJson(value.get(field));
            updatedValue.put(field, jsonField.toJSONString());
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (transFields.contains(field.name())) {
                JSON jsonField = parseJson(value.get(field));
                updatedValue.put(field.name(), jsonField.toJSONString());
            } else {
                updatedValue.put(field.name(), value.get(field));
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field: schema.fields()) {
            if (transFields.contains(field.name())) {
                builder.field(field.name(), Schema.STRING_SCHEMA);
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }

    private JSON parseJson(Object value) {
        JSONObject json = new JSONObject();
        if (value == null)
            return json;
        Schema.Type type = ConnectSchema.schemaType(value.getClass());
        if (type != STRUCT)
            throw new ConfigException("ToJson transformation only support type: STRUCT.");
        Struct struct = (Struct) value;
        for (Field field:struct.schema().fields()) {
            if (field.schema().type() == STRUCT) {
                json.put(field.name(), parseJson(struct.get(field)));
            } else {
                json.put(field.name(), struct.get(field));
            }
        }

        return json;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ToJson<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ToJson<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
