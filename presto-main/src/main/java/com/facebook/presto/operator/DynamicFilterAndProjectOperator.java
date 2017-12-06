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

import com.facebook.presto.operator.project.MergingPageOutput;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.type.Type;
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

public class DynamicFilterAndProjectOperator
        extends FilterAndProjectOperator
{
    private final DynamicFilterOperatorCollector dfCollector;

    enum DynamicProcessorState {
        NOTFOUND,
        QUERYING,
        DONE,
        FAILED
    }

    public DynamicFilterAndProjectOperator(OperatorContext operatorContext,
            Iterable<? extends Type> types,
            PageProcessor processor,
            MergingPageOutput mergingOutput,
            List<RowExpression> projections,
            ExpressionCompiler expressionCompiler,
            Set<DynamicFilter> dynamicFilters,
            Optional<RowExpression> staticFilter,
            DynamicFilterClient client,
            RowExpressionConverter converter,
            QueryId queryId)
    {
        super(operatorContext, types, processor, mergingOutput);
        dfCollector = new DynamicFilterOperatorCollector(projections, expressionCompiler, dynamicFilters, staticFilter, client, converter, queryId);
        dfCollector.collectDynamicSummaryAsync();
    }

    @Override
    public void addInput(Page page)
    {
        if (!dfCollector.isDone()) {
            super.addInput(page);
        }
        else {
            super.processPage(page, dfCollector.getDynamicPageProcessor());
        }
    }

    @Override
    public void close() throws Exception
    {
        try {
            super.close();
        }
        finally {
            dfCollector.cleanUp();
        }
    }

    private static class DynamicFilterOperatorCollector
            extends AbstractDynamicFilterOperatorCollector
    {
        private PageProcessor dynamicPageProcessor;

        public DynamicFilterOperatorCollector(List<RowExpression> projections,
                ExpressionCompiler expressionCompiler,
                Set<DynamicFilter> dynamicFilters,
                Optional<RowExpression> staticFilter,
                DynamicFilterClient client,
                RowExpressionConverter converter,
                QueryId queryId)
        {
            super(projections, expressionCompiler, dynamicFilters, staticFilter, client, converter, queryId);
        }

        @Override
        protected void initDynamicProcessors(RowExpression combinedExpression, List<RowExpression> projections,
                ExpressionCompiler exprCompiler)
        {
            dynamicPageProcessor = exprCompiler.compilePageProcessor(Optional.of(combinedExpression), projections).get();
        }

        public PageProcessor getDynamicPageProcessor()
        {
            return dynamicPageProcessor;
        }
    }

    public static class DynamicFilterAndProjectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<PageProcessor> processor;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private final List<RowExpression> projections;
        private final ExpressionCompiler exprCompiler;
        private final Set<DynamicFilter> dynamicFilter;
        private final Optional<RowExpression> staticFilter;
        private final DynamicFilterClientSupplier dynamicFilterClientSupplier;
        private final QueryId queryId;
        private final RowExpressionConverter converter;
        private boolean closed;

        public DynamicFilterAndProjectOperatorFactory(int operatorId,
                PlanNodeId planNodeId,
                Supplier<PageProcessor> processor,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount,
                List<RowExpression> projections,
                ExpressionCompiler exprCompiler,
                Optional<RowExpression> staticFilter,
                Set<DynamicFilter> dynamicFilter,
                DynamicFilterClientSupplier dynamicFilterClientSupplier,
                QueryId queryId,
                RowExpressionConverter converter)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = requireNonNull(processor, "processor is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.minOutputPageSize = minOutputPageSize;
            this.projections = requireNonNull(projections, "projections is null");
            this.exprCompiler = requireNonNull(exprCompiler, "exprCompiler is null");
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilters is null");
            this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
            this.dynamicFilterClientSupplier = requireNonNull(dynamicFilterClientSupplier, "dynamicFilterClientSupplier is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.converter = requireNonNull(converter, "converter is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId,
                    planNodeId, FilterAndProjectOperator.class.getSimpleName());
            DynamicFilterClient client = dynamicFilterClientSupplier.createClient(null, null, -1, -1, converter.getTypeManager());
            MergingPageOutput pageOutput = new MergingPageOutput(types, minOutputPageSize.toBytes(), minOutputPageRowCount);
            return new DynamicFilterAndProjectOperator(operatorContext, types, processor.get(), pageOutput,
                    projections, exprCompiler, dynamicFilter, staticFilter, client, converter, queryId);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DynamicFilterAndProjectOperatorFactory(operatorId, planNodeId,
                    processor, types, minOutputPageSize, minOutputPageRowCount, projections,
                    exprCompiler, staticFilter, dynamicFilter,
                    dynamicFilterClientSupplier, queryId, converter);
        }
    }
}
