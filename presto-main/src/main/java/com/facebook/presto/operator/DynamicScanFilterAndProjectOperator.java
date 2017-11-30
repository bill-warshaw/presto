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

import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.MergingPageOutput;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.DynamicFilter;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.util.RowExpressionConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.units.DataSize;

import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Signatures.logicalExpressionSignature;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicScanFilterAndProjectOperator
        extends ScanFilterAndProjectOperator
{
    private final List<RowExpression> projections;
    private final ExpressionCompiler exprCompiler;
    private final DynamicFilter dynamicFilter;
    private final Optional<RowExpression> staticFilter;
    private final DynamicFilterClient client;
    private final RowExpressionConverter converter;
    private final QueryId queryId;
    private CursorProcessor dynamicCursorProcessor;
    private PageProcessor dynamicPageProcessor;

    enum DynamicProcessorState {
        NOTFOUND,
        QUERYING,
        DONE,
        FAILED
    }

    private DynamicProcessorState state = DynamicProcessorState.NOTFOUND;

    protected DynamicScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            Iterable<ColumnHandle> columns,
            Iterable<Type> types,
            MergingPageOutput mergingOutput,
            List<RowExpression> projections,
            ExpressionCompiler expressionCompiler,
            DynamicFilter dynamicFilter,
            Optional<RowExpression> staticFilter,
            DynamicFilterClient client,
            RowExpressionConverter converter,
            QueryId queryId)
    {
        super(operatorContext, sourceId, pageSourceProvider, cursorProcessor,
            pageProcessor, columns, types, mergingOutput);
        this.projections = requireNonNull(projections, "projections is null");
        this.exprCompiler = requireNonNull(expressionCompiler, "expressionCompiler is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilters is null");
        this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
        this.client = requireNonNull(client, "client is null");
        this.converter = requireNonNull(converter, "converter is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
    }

    private void collectDynamicSummary()
    {
        final ListenableFuture result = this.client.getSummary(queryId.toString(), dynamicFilter.getTupleDomainSourceId());
        Futures.addCallback(result, new FutureCallback<FullJsonResponseHandler.JsonResponse<DynamicFilterSummary>>() {
            public void onSuccess(FullJsonResponseHandler.JsonResponse<DynamicFilterSummary> jsonResponse)
            {
                try {
                    if (jsonResponse.getStatusCode() == Response.Status.OK.getStatusCode()) {
                        final DynamicFilterSummary summary = jsonResponse.getValue();
                        final TupleDomain<Symbol> tupleDomain = translateSummaryIntoTupleDomain(summary, ImmutableSet.of(dynamicFilter));
                        final Expression expr = DomainTranslator.toPredicate(tupleDomain);

                        final Optional<RowExpression> rowExpression = converter.expressionToRowExpression(Optional.of(expr));
                        final RowExpression combinedExpression = call(logicalExpressionSignature(LogicalBinaryExpression.Type.AND),
                                BOOLEAN,
                                rowExpression.get(),
                                staticFilter.get());
                        dynamicPageProcessor = exprCompiler.compilePageProcessor(Optional.of(combinedExpression), projections).get();
                        dynamicCursorProcessor = exprCompiler.compileCursorProcessor(Optional.of(combinedExpression), projections, getSourceId()).get();
                        state = DynamicProcessorState.DONE;
                    }
                    else if (jsonResponse.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                        state = DynamicProcessorState.NOTFOUND;
                    }
                    else {
                        state = DynamicProcessorState.FAILED;
                    }
                }
                catch (Exception e) {
                    state = DynamicProcessorState.FAILED;
                }
            }

            public void onFailure(Throwable thrown)
            {
                state = DynamicProcessorState.FAILED;
            }
        });
    }

    @Override
    protected PageProcessor getPageProcessor()
    {
        if (state == DynamicProcessorState.DONE) {
            return this.dynamicPageProcessor;
        }
        else {
            return super.getPageProcessor();
        }
    }

    @Override
    protected CursorProcessor getCursorProcessor()
    {
        if (state == DynamicProcessorState.DONE) {
            return this.dynamicCursorProcessor;
        }
        else {
            return super.getCursorProcessor();
        }
    }

    @Override
    public Page processColumnSource(CursorProcessor cursorProcessor)
    {
        if (state == DynamicProcessorState.NOTFOUND || state == DynamicProcessorState.QUERYING) {
            collectDynamicSummary();
        }
        return super.processColumnSource(getCursorProcessor());
    }

    @Override
    public Page processPageSource(PageProcessor pageProcessor)
    {
        if (state == DynamicProcessorState.NOTFOUND || state == DynamicProcessorState.QUERYING) {
            collectDynamicSummary();
        }
        return super.processPageSource(getPageProcessor());
    }

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

    public static class DynamicScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<CursorProcessor> cursorProcessor;
        private final Supplier<PageProcessor> pageProcessor;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final List<ColumnHandle> columns;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;
        private List<RowExpression> projections;
        private ExpressionCompiler exprCompiler;
        private Optional<RowExpression> staticFilter;
        private DynamicFilter dynamicFilter;
        private DynamicFilterClientSupplier dynamicFilterClientSupplier;
        private QueryId queryId;
        private RowExpressionConverter converter;

        public DynamicScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanNodeId sourceId,
                PageSourceProvider pageSourceProvider,
                Supplier<CursorProcessor> cursorProcessor,
                Supplier<PageProcessor> pageProcessor,
                Iterable<ColumnHandle> columns,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount,
                List<RowExpression> projections,
                ExpressionCompiler expressionCompiler,
                Optional<RowExpression> staticFilter,
                DynamicFilter dynamicFilter,
                DynamicFilterClientSupplier dynamicFilterClientSupplier,
                QueryId queryId,
                RowExpressionConverter converter)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = requireNonNull(types, "types is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.projections = requireNonNull(projections, "projections is null");
            this.exprCompiler = requireNonNull(expressionCompiler, "expressionCompiler is null");
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilters is null");
            this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
            this.dynamicFilterClientSupplier = requireNonNull(dynamicFilterClientSupplier, "client supplier is null");
            this.converter = requireNonNull(converter, "converter is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, DynamicScanFilterAndProjectOperator.class.getSimpleName());
            DynamicFilterClient client = dynamicFilterClientSupplier.createClient(null, null, -1, -1, converter.getTypeManager());
            return new DynamicScanFilterAndProjectOperator(
                    operatorContext,
                    sourceId,
                    pageSourceProvider,
                    cursorProcessor.get(),
                    pageProcessor.get(),
                    columns,
                    types,
                    new MergingPageOutput(types, minOutputPageSize.toBytes(), minOutputPageRowCount),
                    projections, exprCompiler,
                    dynamicFilter, staticFilter,
                    client,
                    converter,
                    queryId);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
