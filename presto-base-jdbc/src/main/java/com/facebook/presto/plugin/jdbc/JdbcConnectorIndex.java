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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class JdbcConnectorIndex
        implements ConnectorIndex
{
    private static final Logger log = Logger.get(JdbcConnectorIndex.class);

    private final JdbcClient jdbcClient;
    private final JdbcConnectorIndexHandle indexHandle;
    private final List<JdbcColumnHandle> lookupSchema;
    private final List<JdbcColumnHandle> outputSchema;

    public JdbcConnectorIndex(JdbcClient jdbcClient, JdbcConnectorIndexHandle indexHandle, List<JdbcColumnHandle> lookupSchema, List<JdbcColumnHandle> outputSchema)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.indexHandle = requireNonNull(indexHandle, "indexHandle is null");
        this.lookupSchema = requireNonNull(lookupSchema, "lookupSchema is null");
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
    }

    @Override
    public ConnectorPageSource lookup(RecordSet recordSet)
    {
        // convert the input record set from the column ordering in the query to match the column ordering of the index
        // lookup the values in the index
        // convert the output record set of the index into the column ordering expected by the query

        // turn recordSet into tuple domain from fixed values
        RecordCursor cursor = recordSet.cursor();

        log.warn("lookup schema - %s", lookupSchema);
        log.warn("output schema - %s", outputSchema);

        // todo handle other types
        List<List<Object>> values = new ArrayList<>(lookupSchema.size());
        lookupSchema.forEach(ch -> values.add(new ArrayList<>()));
        while (cursor.advanceNextPosition()) {
            // todo need to reformat order of incoming recordSet?
            for (int i = 0; i < lookupSchema.size(); i++) {
                values.get(i).add(cursor.getLong(i));
            }
        }

        Map<ColumnHandle, Domain> domains = new HashMap<>();
        for (int i = 0; i < lookupSchema.size(); i++) {
            domains.put(lookupSchema.get(i), Domain.multipleValues(IntegerType.INTEGER, values.get(i)));
        }

        TupleDomain<ColumnHandle> pkValues = TupleDomain.withColumnDomains(domains);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(indexHandle.getTableHandle(), pkValues);
        JdbcSplit jdbcSplit = jdbcClient.getSplit(jdbcTableLayoutHandle);
        JdbcRecordSet jdbcRecordSet = new JdbcRecordSet(jdbcClient, jdbcSplit, outputSchema);

        return new RecordPageSource(jdbcRecordSet);
    }
}
