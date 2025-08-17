/*
 * Copyright 2024-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.azure.eventgrid;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.HttpStatusType;

import org.junit.jupiter.api.Test;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class EventGridHandlerTests {

    @Autowired
    private EventGridHandler handler;

    @Test
    void webhookValidation_allowsOrigin_headerCaseInsensitive() {
        Map<String, String> headers = new HashMap<>();
        headers.put("webhook-request-origin", "https://eventgrid.azure.net");

        TestHttpRequest request = new TestHttpRequest(HttpMethod.OPTIONS, URI.create("http://localhost/api/eventgrid"), headers, Optional.empty());

        HttpResponseMessage response = handler.eventGridWebhook(request, new TestExecutionContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK);
        assertThat(response.getHeader("WebHook-Allowed-Origin")).isEqualTo("https://eventgrid.azure.net");
    }

    @Test
    void cloudEvents_structured_mode_isProcessed() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/cloudevents+json");

        String body = "{" +
                "\"specversion\":\"1.0\"," +
                "\"type\":\"com.example.test\"," +
                "\"source\":\"https://example.com/source\"," +
                "\"id\":\"abc-123\"," +
                "\"time\":\"" + OffsetDateTime.now().toString() + "\"," +
                "\"datacontenttype\":\"application/json\"," +
                "\"data\":{\"hello\":\"world\"}" +
                "}";

        TestHttpRequest request = new TestHttpRequest(HttpMethod.POST, URI.create("http://localhost/api/eventgrid"), headers, Optional.of(body));

        HttpResponseMessage response = handler.eventGridWebhook(request, new TestExecutionContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) response.getBody();
        assertThat(payload.get("status")).isEqualTo("success");
        assertThat((String) payload.get("message")).contains("Successfully processed CloudEvent");
    }

    @Test
    void cloudEvents_binary_mode_isProcessed() {
        Map<String, String> headers = new HashMap<>();
        headers.put("ce-specversion", "1.0");
        headers.put("ce-id", "bin-123");
        headers.put("ce-type", "com.example.binary");
        headers.put("ce-source", "https://example.com/bin");
        headers.put("Content-Type", "application/json");

        String body = "{\"k\":\"v\"}";

        TestHttpRequest request = new TestHttpRequest(HttpMethod.POST, URI.create("http://localhost/api/eventgrid"), headers, Optional.of(body));

        HttpResponseMessage response = handler.eventGridWebhook(request, new TestExecutionContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) response.getBody();
        assertThat(payload.get("status")).isEqualTo("success");
        assertThat((String) payload.get("message")).contains("Successfully processed CloudEvent");
    }

    @Test
    void eventGrid_array_format_isConverted_andProcessed() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");

        String body = "[{" +
                "\"id\":\"eg-001\"," +
                "\"eventType\":\"Microsoft.Storage.BlobCreated\"," +
                "\"subject\":\"/blobServices/default/containers/test/blobs/a.txt\"," +
                "\"eventTime\":\"" + OffsetDateTime.now().toString() + "\"," +
                "\"topic\":\"/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/sa\"," +
                "\"data\":{\"api\":\"PutBlob\"}," +
                "\"dataVersion\":\"1.0\"," +
                "\"metadataVersion\":\"1\"" +
                "}]";

        TestHttpRequest request = new TestHttpRequest(HttpMethod.POST, URI.create("http://localhost/api/eventgrid"), headers, Optional.of(body));

        HttpResponseMessage response = handler.eventGridWebhook(request, new TestExecutionContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) response.getBody();
        assertThat(payload.get("status")).isEqualTo("success");
        assertThat((Integer) payload.get("processedEvents")).isEqualTo(1);
    }

    @Test
    void eventGrid_subscription_validation_returnsValidationResponse() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");

        String body = "[{" +
                "\"id\":\"val-001\"," +
                "\"eventType\":\"Microsoft.EventGrid.SubscriptionValidationEvent\"," +
                "\"data\":{\"validationCode\":\"abc123\"}" +
                "}]";

        TestHttpRequest request = new TestHttpRequest(HttpMethod.POST, URI.create("http://localhost/api/eventgrid"), headers, Optional.of(body));

        HttpResponseMessage response = handler.eventGridWebhook(request, new TestExecutionContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) response.getBody();
        assertThat(payload.get("validationResponse")).isEqualTo("abc123");
    }

    // --- Test support classes ---

    static class TestHttpRequest implements HttpRequestMessage<Optional<String>> {
        private final HttpMethod method;
        private final URI uri;
        private final Map<String, String> headers;
        private final Map<String, String> query;
        private final Optional<String> body;

        TestHttpRequest(HttpMethod method, URI uri, Map<String, String> headers, Optional<String> body) {
            this.method = method;
            this.uri = uri;
            this.headers = headers;
            this.query = new HashMap<>();
            this.body = body;
        }

        @Override
        public URI getUri() { return uri; }

        @Override
        public HttpMethod getHttpMethod() { return method; }

        @Override
        public Map<String, String> getHeaders() { return headers; }

        @Override
        public Map<String, String> getQueryParameters() { return query; }

        @Override
        public Optional<String> getBody() { return body; }

        @Override
        public HttpResponseMessage.Builder createResponseBuilder(HttpStatus status) {
            return new TestHttpResponseBuilder(status);
        }

        @Override
        public HttpResponseMessage.Builder createResponseBuilder(HttpStatusType status) {
            return new TestHttpResponseBuilder(status);
        }
    }

    static class TestHttpResponse implements HttpResponseMessage {
        private final HttpStatusType status;
        private final Map<String, String> headers = new HashMap<>();
        private final Object body;

        TestHttpResponse(HttpStatusType status, Map<String, String> headers, Object body) {
            this.status = status;
            if (headers != null) {
                this.headers.putAll(headers);
            }
            this.body = body;
        }

        @Override
        public HttpStatusType getStatus() { return status; }

        @Override
        public String getHeader(String key) { return headers.get(key); }

        @Override
        public Object getBody() { return body; }
    }

    static class TestHttpResponseBuilder implements HttpResponseMessage.Builder {
        private HttpStatusType status;
        private final Map<String, String> headers = new HashMap<>();
        private Object body;

        TestHttpResponseBuilder(HttpStatusType status) {
            this.status = status;
        }

        TestHttpResponseBuilder(HttpStatus status) {
            this.status = status;
        }

        @Override
        public HttpResponseMessage.Builder status(HttpStatusType status) {
            this.status = status;
            return this;
        }

        public HttpResponseMessage.Builder status(HttpStatus status) {
            this.status = status;
            return this;
        }

        @Override
        public HttpResponseMessage.Builder header(String key, String value) {
            headers.put(key, value);
            return this;
        }

        @Override
        public HttpResponseMessage.Builder body(Object body) {
            this.body = body;
            return this;
        }

        @Override
        public HttpResponseMessage build() {
            return new TestHttpResponse(status, headers, body);
        }
    }

    static class TestExecutionContext implements ExecutionContext {
        private static final Logger log = Logger.getLogger(TestExecutionContext.class.getName());

        @Override
        public Logger getLogger() { return log; }

        @Override
        public String getInvocationId() { return "test-invocation"; }

        @Override
        public String getFunctionName() { return "eventGridWebhook"; }
    }
}
