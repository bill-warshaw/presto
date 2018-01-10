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
package com.facebook.presto.operator;

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.DynamicFilter;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.util.RowExpressionConverter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.log.Logger;

import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Signatures.logicalExpressionSignature;
import static java.util.Objects.requireNonNull;

public abstract class AbstractDynamicFilterOperatorCollector
{
    private static final Logger log = Logger.get(AbstractDynamicFilterOperatorCollector.class);
    private final ConcurrentHashMap<DynamicFilter, DynamicProcessorState> dfState = new ConcurrentHashMap<>();
    private final AtomicReference<TupleDomain> dfTupleDomain = new AtomicReference<>(TupleDomain.all());
    private final Optional<RowExpression> staticFilter;
    private final RowExpressionConverter converter;
    private final List<RowExpression> projections;
    private final ExpressionCompiler exprCompiler;
    private final Set<DynamicFilter> dynamicFilters;
    private final DynamicFilterClient client;
    private final QueryId queryId;
    enum DynamicProcessorState {
        NOTFOUND,
        QUERYING,
        DONE,
        FAILED
    }

    protected DynamicProcessorState globalState = DynamicProcessorState.NOTFOUND;

    protected AbstractDynamicFilterOperatorCollector(List<RowExpression> projections,
            ExpressionCompiler expressionCompiler,
            Set<DynamicFilter> dynamicFilters,
            Optional<RowExpression> staticFilter,
            DynamicFilterClient client,
            RowExpressionConverter converter,
            QueryId queryId)
    {
        this.projections = requireNonNull(projections, "projections is null");
        this.exprCompiler = requireNonNull(expressionCompiler, "expressionCompiler is null");
        this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
        this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
        this.client = requireNonNull(client, "client is null");
        this.converter = requireNonNull(converter, "converter is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        dynamicFilters.stream().forEach(d -> dfState.put(d, DynamicProcessorState.NOTFOUND));
    }

    private void fireDFCollection(final DynamicFilter df)
    {
        // check if collection is already in progress
        if (dfState.get(df) == DynamicProcessorState.QUERYING) {
            return;
        }
        final ListenableFuture result = client.getSummary(queryId.toString(), df.getTupleDomainSourceId());
        dfState.put(df, DynamicProcessorState.QUERYING);
        Futures.addCallback(result, new FutureCallback<FullJsonResponseHandler.JsonResponse<DynamicFilterSummary>>() {
            public void onSuccess(FullJsonResponseHandler.JsonResponse<DynamicFilterSummary> jsonResponse)
            {
                try {
                    if (jsonResponse.getStatusCode() == Response.Status.OK.getStatusCode()) {
                        final DynamicFilterSummary summary = jsonResponse.getValue();
                        final TupleDomain<Symbol> tupleDomain = translateSummaryIntoTupleDomain(summary, ImmutableSet.of(df));
                        dfTupleDomain.accumulateAndGet(tupleDomain, TupleDomain::intersect);
                        dfState.put(df, DynamicProcessorState.DONE);
                        log.info("DF " + df + " is collected.");
                    }
                    else if (jsonResponse.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                        log.info("DF " + df + " is not found yet.");
                        dfState.put(df, DynamicProcessorState.NOTFOUND);
                    }
                    else {
                        log.info("DF " + df + " fail to be collected.");
                        dfState.put(df, DynamicProcessorState.FAILED);
                    }
                }
                catch (Exception exp) {
                    log.error("DF " + df + " fail to be collected. Error: " + exp.getMessage(), exp);
                    dfState.put(df, DynamicProcessorState.FAILED);
                }
            }

            public void onFailure(Throwable thrown)
            {
                log.error("DF " + df + " fail to be collected. "
                        + "Throwable Error: " + thrown.getMessage(), thrown);
                dfState.put(df, DynamicProcessorState.FAILED);
            }
        });
    }

    public void collectDynamicSummary()
    {
        if (globalState != DynamicProcessorState.DONE || globalState != DynamicProcessorState.FAILED) {
            collectDynamicSummaryInternal();
        }
    }

    private void collectDynamicSummaryInternal()
    {
        boolean inProgress = false;
        for (ConcurrentHashMap.Entry<DynamicFilter, DynamicProcessorState> e : dfState.entrySet()) {
            DynamicProcessorState state = e.getValue();
            if (state != DynamicProcessorState.DONE
                    && state != DynamicProcessorState.FAILED) {
                inProgress = true;
                fireDFCollection(e.getKey());
            }
        }
        if (!inProgress) {
            afterDFcollection();
        }
    }

    private void afterDFcollection()
    {
        log.info("Initializing Dynamic Processors.");
        if (globalState == DynamicProcessorState.DONE || globalState == DynamicProcessorState.FAILED) {
            return;
        }
        TupleDomain<Symbol> tupleDomain = dfTupleDomain.get();
        if (!tupleDomain.isAll()) {
            try {
                final Expression expr = DomainTranslator.toPredicate(tupleDomain);
                final Optional<RowExpression> rowExpression = converter.expressionToRowExpression(Optional.of(expr));
                final RowExpression combinedExpression = call(logicalExpressionSignature(LogicalBinaryExpression.Type.AND),
                    BOOLEAN,
                    rowExpression.get(),
                    staticFilter.get());
                initDynamicProcessors(combinedExpression, projections, exprCompiler);
                globalState = DynamicProcessorState.DONE;
                log.info("DF Processor initialized");
            }
            catch (Exception e) {
                log.error("Error initializing DF procesor", e);
                globalState = DynamicProcessorState.FAILED;
            }
        }
        else {
            log.info("DF Processing: Tuple Domain is all");
            globalState = DynamicProcessorState.FAILED;
        }
    }

    protected abstract void initDynamicProcessors(RowExpression combinedExpression, List<RowExpression> projections,
            ExpressionCompiler exprCompiler);

    private TupleDomain translateSummaryIntoTupleDomain(DynamicFilterSummary summary, Set<DynamicFilter> dynamicFilters)
    {
        if (!summary.getTupleDomain().getDomains().isPresent()) {
            return TupleDomain.all();
        }

        Map<String, Symbol> sourceExpressionSymbols = extractSourceExpressionSymbols(dynamicFilters);
        ImmutableMap.Builder<Symbol, Domain> domainBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Domain> entry : summary.getTupleDomain().getDomains().get().entrySet()) {
            Symbol actualSymbol = sourceExpressionSymbols.get(entry.getKey());
            if (actualSymbol == null) {
                continue;
            }
            domainBuilder.put(actualSymbol, entry.getValue());
        }

        final ImmutableMap<Symbol, Domain> domainMap = domainBuilder.build();
        return domainMap.isEmpty() ? TupleDomain.all() : TupleDomain.withColumnDomains(domainMap);
    }

    private Map<String, Symbol> extractSourceExpressionSymbols(Set<DynamicFilter> dynamicFilters)
    {
        ImmutableMap.Builder<String, Symbol> resultBuilder = ImmutableMap.builder();
        for (DynamicFilter dynamicFilter : dynamicFilters) {
            Expression expression = dynamicFilter.getSourceExpression();
            if (!(expression instanceof SymbolReference)) {
                continue;
            }
            resultBuilder.put(dynamicFilter.getTupleDomainName(), Symbol.from(expression));
        }
        return resultBuilder.build();
    }

    public boolean isDone()
    {
        return globalState == DynamicProcessorState.DONE;
    }
}
