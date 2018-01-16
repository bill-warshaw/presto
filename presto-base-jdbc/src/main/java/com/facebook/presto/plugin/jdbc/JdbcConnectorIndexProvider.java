/*
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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class JdbcConnectorIndexProvider
        implements ConnectorIndexProvider
{
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcConnectorIndexProvider(JdbcClient jdbcClient)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public ConnectorIndex getIndex(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorIndexHandle indexHandle, List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema)
    {
        JdbcConnectorIndexHandle jdbcConnectorIndexHandle = (JdbcConnectorIndexHandle) indexHandle;
        List<JdbcColumnHandle> jdbcLookupSchema = castColHandleList(lookupSchema);
        List<JdbcColumnHandle> jdbcOutputSchema = castColHandleList(outputSchema);
        return new JdbcConnectorIndex(jdbcClient, jdbcConnectorIndexHandle, jdbcLookupSchema, jdbcOutputSchema);
    }

    private List<JdbcColumnHandle> castColHandleList(List<ColumnHandle> columnHandles)
    {
        return columnHandles.stream().map(ch -> (JdbcColumnHandle) ch).collect(toList());
    }
}
