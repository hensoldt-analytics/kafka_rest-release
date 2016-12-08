/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.hortonworks.kafkarest.unit;

import com.fasterxml.jackson.databind.JsonNode;

import com.hortonworks.kafkarest.Errors;
import com.hortonworks.kafkarest.MetadataObserver;
import com.hortonworks.kafkarest.RecordMetadataOrException;
import com.hortonworks.kafkarest.entities.AvroTopicProduceRecord;
import com.hortonworks.kafkarest.entities.EmbeddedFormat;
import com.hortonworks.kafkarest.entities.PartitionOffset;
import com.hortonworks.kafkarest.entities.TopicProduceRequest;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import com.hortonworks.kafkarest.Context;
import com.hortonworks.kafkarest.KafkaRestApplication;
import com.hortonworks.kafkarest.KafkaRestConfig;
import com.hortonworks.kafkarest.ProducerPool;
import com.hortonworks.kafkarest.TestUtils;
import com.hortonworks.kafkarest.entities.ProduceRecord;
import com.hortonworks.kafkarest.entities.ProduceResponse;
import com.hortonworks.kafkarest.entities.SchemaHolder;
import com.hortonworks.kafkarest.resources.TopicsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;

import static com.hortonworks.kafkarest.TestUtils.assertErrorResponse;
import static com.hortonworks.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

// This test is much lighter than the Binary one since they would otherwise be mostly duplicated
// -- this just sanity checks the Jersey processing of these requests.
public class TopicsResourceAvroProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {
  private static final SchemaIdVersion KEY_SCHEMA_ID = new SchemaIdVersion(1L, 1);
  private static final SchemaIdVersion VALUE_SCHEMA_ID = new SchemaIdVersion(2L, 2);

  private MetadataObserver mdObserver;
  private ProducerPool producerPool;
  private Context ctx;

  private static final String topicName = "topic1";
  private static final String keySchemaStr = "{\"name\":\"int\",\"type\": \"int\"}";
  private static final String valueSchemaStr = "{\"type\": \"record\", "
                                               + "\"name\":\"test\","
                                               + "\"fields\":[{"
                                               + "  \"name\":\"field\", "
                                               + "  \"type\": \"int\""
                                               + "}]}";
  private static final Schema keySchema = new Schema.Parser().parse(keySchemaStr);
  private static final Schema valueSchema = new Schema.Parser().parse(valueSchemaStr);

  private final static JsonNode[] testKeys = {
      TestUtils.jsonTree("1"),
      TestUtils.jsonTree("2"),
  };

  private final static JsonNode[] testValues = {
      TestUtils.jsonTree("{\"field\": 1}"),
      TestUtils.jsonTree("{\"field\": 2}"),
  };

  private List<AvroTopicProduceRecord> produceRecordsWithPartitionsAndKeys;

  private static final TopicPartition tp0 = new TopicPartition(topicName, 0);
  private static final TopicPartition tp1 = new TopicPartition(topicName, 1);
  private List<RecordMetadataOrException> produceResults = Arrays.asList(
      new RecordMetadataOrException(new RecordMetadata(tp0, 0, 0, 0, 0, 1, 1), null),
      new RecordMetadataOrException(new RecordMetadata(tp0, 0, 1, 0, 0, 1, 1), null),
      new RecordMetadataOrException(new RecordMetadata(tp1, 0, 0, 0, 0, 1, 1), null)
  );
  private static final List<PartitionOffset> offsetResults = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(1, 0L, null, null)
  );

  public TopicsResourceAvroProduceTest() throws RestConfigException {
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new Context(config, mdObserver, producerPool, null, null);

    addResource(new TopicsResource(ctx));

    produceRecordsWithPartitionsAndKeys = Arrays.asList(
        new AvroTopicProduceRecord(testKeys[0], testValues[0], 0),
        new AvroTopicProduceRecord(testKeys[1], testValues[1], 0)
    );
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(mdObserver, producerPool);
  }

  private <K, V> Response produceToTopic(String topic, String acceptHeader, String requestMediatype,
                                         EmbeddedFormat recordFormat,
                                         TopicProduceRequest request,
                                         final List<RecordMetadataOrException> results) {
    final Capture<ProducerPool.ProduceRequestCallback>
        produceCallback =
        new Capture<ProducerPool.ProduceRequestCallback>();
    producerPool.produce(EasyMock.eq(topic),
                         EasyMock.eq((Integer) null),
                         EasyMock.eq(recordFormat),
                         EasyMock.<SchemaHolder>anyObject(),
                         EasyMock.<Collection<? extends ProduceRecord<K, V>>>anyObject(),
                         EasyMock.capture(produceCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        if (results == null) {
          throw new Exception();
        } else {
          produceCallback.getValue().onCompletion(KEY_SCHEMA_ID, VALUE_SCHEMA_ID, results);
        }
        return null;
      }
    });
    EasyMock.replay(mdObserver, producerPool);

    Response response = request("/topics/" + topic, acceptHeader)
        .post(Entity.entity(request, requestMediatype));

    EasyMock.verify(mdObserver, producerPool);

    return response;
  }

  @Test
  public void testProduceToTopicWithPartitionAndKey() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(produceRecordsWithPartitionsAndKeys);
        request.setKeySchema(keySchemaStr);
        request.setValueSchema(valueSchemaStr);

        Response
            rawResponse =
            produceToTopic(topicName, mediatype.header, requestMediatype,
                           EmbeddedFormat.AVRO,
                           request, produceResults);
        assertOKResponse(rawResponse, mediatype.expected);
        ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

        assertEquals(offsetResults, response.getOffsets());
        assertEquals(KEY_SCHEMA_ID, response.getKeySchemaId());
        assertEquals(VALUE_SCHEMA_ID, response.getValueSchemaId());

        EasyMock.reset(mdObserver, producerPool);
      }
    }

    // Test using schema IDs
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(produceRecordsWithPartitionsAndKeys);
        request.setKeySchemaId(KEY_SCHEMA_ID);
        request.setValueSchemaId(VALUE_SCHEMA_ID);

        Response rawResponse =
            produceToTopic(topicName, mediatype.header, requestMediatype,
                           EmbeddedFormat.AVRO,
                           request, produceResults);
        assertOKResponse(rawResponse, mediatype.expected);

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  private void produceToTopicExpectFailure(String topicName, String acceptHeader,
                                           String requestMediatype, TopicProduceRequest request,
                                           String responseMediaType, int errorCode) {
    Response rawResponse = request("/topics/" + topicName, acceptHeader)
        .post(Entity.entity(request, requestMediatype));

    assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
                        rawResponse, errorCode, null, responseMediaType);
  }

  @Test
  public void testMissingKeySchema() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(produceRecordsWithPartitionsAndKeys);
        request.setValueSchema(valueSchemaStr);

        produceToTopicExpectFailure(topicName, mediatype.header, requestMediatype,
                                    request, mediatype.expected,
                                    Errors.KEY_SCHEMA_MISSING_ERROR_CODE);
      }
    }
  }

  @Test
  public void testMissingValueSchema() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(produceRecordsWithPartitionsAndKeys);
        request.setKeySchema(keySchemaStr);

        produceToTopicExpectFailure(topicName, mediatype.header, requestMediatype,
                                    request, mediatype.expected,
                                    Errors.VALUE_SCHEMA_MISSING_ERROR_CODE);
      }
    }
  }
}
