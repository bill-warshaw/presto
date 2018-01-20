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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.DynamicFilterDescription;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class JdbcSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(JdbcSplitSource.class);

    private final JdbcSplit split;
    private boolean consumed;
    private final List<CompletableFuture<DynamicFilterDescription>> dynamicFilters;
    private final ExecutorService executorService;

    public JdbcSplitSource(JdbcSplit split, List<CompletableFuture<DynamicFilterDescription>> dynamicFilters)
    {
        this.split = split;
        this.dynamicFilters = dynamicFilters;
        this.consumed = false;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        consumed = true;
        String connectorId = split.getConnectorId();
        String catalogName = split.getCatalogName();
        String schemaName = split.getSchemaName();
        String tableName = split.getTableName();

        return CompletableFuture.supplyAsync(() -> {
                    List<TupleDomain<ColumnHandle>> dynamicDomains = dynamicFilters.stream().map(f -> {
                        try {
                            log.warn("computing a dynamic filter for table %s", split.getTableName());
                            TupleDomain<ColumnHandle> df = f.get(10, TimeUnit.SECONDS).getTupleDomain();
                            log.warn("computed dynamic filter for %s - %s", tableName, tupToStr(df));
                            return df;
                        }
                        catch (InterruptedException | ExecutionException | TimeoutException e) {
                            return TupleDomain.<ColumnHandle>all();
                        }
                    }).collect(toList());

                    TupleDomain<ColumnHandle> originalTupleDomain = split.getTupleDomain();
                    for (TupleDomain<ColumnHandle> d : dynamicDomains) {
                        originalTupleDomain = originalTupleDomain.intersect(d);
                    }
                    JdbcSplit newSplit = new JdbcSplit(
                            connectorId,
                            catalogName,
                            schemaName,
                            tableName,
                            originalTupleDomain);
                    return (Lists.newArrayList(newSplit));
                },
                executorService);
    }

    @Override
    public void close()
    {
        executorService.shutdownNow();
    }

    @Override
    public boolean isFinished()
    {
        return consumed;
    }

    private String tupToStr(TupleDomain<ColumnHandle> td)
    {
        StringBuilder buffer = new StringBuilder()
                .append("TupleDomain:");
        if (td.isAll()) {
            buffer.append("ALL");
        }
        else if (td.isNone()) {
            buffer.append("NONE");
        }
        else {
            buffer.append(td.getDomains().get().entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().toString())));
        }
        return buffer.toString();
    }
}
