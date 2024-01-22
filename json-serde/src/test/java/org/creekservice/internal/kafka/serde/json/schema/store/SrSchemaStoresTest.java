/*
 * Copyright 2024 Creek Contributors (https://github.com/creek-service)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.creekservice.internal.kafka.serde.json.schema.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.creekservice.internal.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SrSchemaStoresTest {

    @Mock private SrSchemaStores.ClientFactory clientFactory;
    @Mock private SrSchemaStores.SchemaStoreFactory storeFactory;
    @Mock private JsonSchemaStoreClient client;
    @Mock private SrSchemaStore store;
    private SrSchemaStores stores;

    @BeforeEach
    void setUp() {
        stores = new SrSchemaStores(clientFactory, storeFactory);

        when(clientFactory.create(any())).thenReturn(client);
        when(storeFactory.create(any())).thenReturn(store);
    }

    @Test
    void shouldCreateStoreOnFirstAccess() {
        // When:
        final SchemaStore result = stores.get("bob");

        // Then:
        verify(clientFactory).create("bob");
        verify(storeFactory).create(client);
        assertThat(result, is(sameInstance(store)));
    }

    @Test
    void shouldReuseSameStore() {
        // Given:
        final SchemaStore original = stores.get("bob");
        clearInvocations(clientFactory, storeFactory);

        // When:
        final SchemaStore result = stores.get("bob");

        // Then:
        verify(clientFactory, never()).create(any());
        verify(storeFactory, never()).create(any());
        assertThat(result, is(sameInstance(original)));
    }

    @Test
    void shouldCreateDifferentClientPerInstance() {
        // Given:
        stores.get("bob");
        clearInvocations(clientFactory, storeFactory);

        // When:
        final SchemaStore result = stores.get("jane");

        // Then:
        verify(clientFactory).create("jane");
        verify(storeFactory).create(client);
        assertThat(result, is(sameInstance(store)));
    }
}
