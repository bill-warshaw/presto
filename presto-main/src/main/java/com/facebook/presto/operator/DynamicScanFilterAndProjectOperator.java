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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.DynamicFilter;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.util.RowExpressionConverter;

import com.google.common.collect.ImmutableList;

import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicScanFilterAndProjectOperator
        extends ScanFilterAndProjectOperator
{
    private final DynamicFilterOperatorCollector dfCollector;

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
            Set<DynamicFilter> dynamicFilters,
            Optional<RowExpression> staticFilter,
            DynamicFilterClient client,
            RowExpressionConverter converter,
            QueryId queryId)
    {
        super(operatorContext, sourceId, pageSourceProvider, cursorProcessor,
            pageProcessor, columns, types, mergingOutput);
        dfCollector = new DynamicFilterOperatorCollector(projections, expressionCompiler, dynamicFilters, staticFilter, client, converter, queryId, getSourceId());
        dfCollector.collectDynamicSummaryAsync();
    }

    @Override
    protected PageProcessor getPageProcessor()
    {
        if (dfCollector.isDone()) {
            return dfCollector.getDynamicPageProcessor();
        }
        else {
            return super.getPageProcessor();
        }
    }

    @Override
    protected CursorProcessor getCursorProcessor()
    {
        if (dfCollector.isDone()) {
            return dfCollector.getDynamicCursorProcessor();
        }
        else {
            return super.getCursorProcessor();
        }
    }

    @Override
    public Page processColumnSource(CursorProcessor cursorProcessor)
    {
        return super.processColumnSource(getCursorProcessor());
    }

    @Override
    public Page processPageSource(PageProcessor pageProcessor)
    {
        return super.processPageSource(getPageProcessor());
    }

    @Override
    public void close()
    {
        super.close();
        dfCollector.cleanUp();
    }

    private static class DynamicFilterOperatorCollector
            extends AbstractDynamicFilterOperatorCollector
    {
        private CursorProcessor dynamicCursorProcessor;
        private PageProcessor dynamicPageProcessor;
        private final PlanNodeId sourceId;

        public DynamicFilterOperatorCollector(List<RowExpression> projections,
                ExpressionCompiler expressionCompiler,
                Set<DynamicFilter> dynamicFilters,
                Optional<RowExpression> staticFilter,
                DynamicFilterClient client,
                RowExpressionConverter converter,
                QueryId queryId,
                PlanNodeId sourceId)
        {
            super(projections, expressionCompiler, dynamicFilters, staticFilter, client, converter, queryId);
            this.sourceId = sourceId;
        }

        @Override
        protected void initDynamicProcessors(RowExpression combinedExpression, List<RowExpression> projections,
                ExpressionCompiler exprCompiler)
        {
            dynamicPageProcessor = exprCompiler.compilePageProcessor(Optional.of(combinedExpression), projections).get();
            dynamicCursorProcessor = exprCompiler.compileCursorProcessor(Optional.of(combinedExpression), projections, sourceId).get();
        }

        public CursorProcessor getDynamicCursorProcessor()
        {
            return dynamicCursorProcessor;
        }

        public PageProcessor getDynamicPageProcessor()
        {
            return dynamicPageProcessor;
        }
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
        private Set<DynamicFilter> dynamicFilters;
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
                Set<DynamicFilter> dynamicFilters,
                DynamicFilterClientSupplier dynamicFilterClientSupplier,
                QueryId queryId,
                RowExpressionConverter converter)
        {
            this.operatorId = requireNonNull(operatorId, "operatorId is null");
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
            this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
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
                    dynamicFilters, staticFilter,
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
