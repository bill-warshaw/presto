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

import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.DynamicFilter;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.util.RowExpressionConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;

import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Signatures.logicalExpressionSignature;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicFilterAndProjectOperator
        extends FilterAndProjectOperator
{
    private final List<RowExpression> projections;
    private final ExpressionCompiler exprCompiler;
    private final Set<DynamicFilter> dynamicFilters;
    private final Optional<RowExpression> staticFilter;
    private final DynamicFilterClient client;
    private final RowExpressionConverter converter;

    private Optional<PageProcessor> dynamicPageProcessor = Optional.empty();

    public DynamicFilterAndProjectOperator(OperatorContext operatorContext,
            Iterable<? extends Type> types,
            PageProcessor processor,
            List<RowExpression> projections,
            ExpressionCompiler expressionCompiler,
            Set<DynamicFilter> dynamicFilters,
            Optional<RowExpression> staticFilter,
            DynamicFilterClient client,
            RowExpressionConverter converter)
    {
        super(operatorContext, types, processor);
        this.projections = requireNonNull(projections, "projections is null");
        this.exprCompiler = requireNonNull(expressionCompiler, "expressionCompiler is null");
        this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
        this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
        this.client = requireNonNull(client, "client is null");
        this.converter = requireNonNull(converter, "converter is null");
    }

    @Override
    public void addInput(Page page)
    {
        if (dynamicPageProcessor == null || !dynamicPageProcessor.isPresent()) {
            super.addInput(page);
            if (dynamicPageProcessor != null) {
                collectDynamicSummary();
            }
        }
        else {
            super.processPage(page, dynamicPageProcessor.get());
        }
    }

    private void collectDynamicSummary()
    {
        try {
            final ListenableFuture<?> result = this.client.getSummary();
            final JsonResponse<DynamicFilterSummary> jsonResponse = (JsonResponse<DynamicFilterSummary>) result.get();
            if (jsonResponse.getStatusCode() == Response.Status.OK.getStatusCode()) {
                final DynamicFilterSummary summary = jsonResponse.getValue();
                final TupleDomain<String> tupleDomain = summary.getTupleDomain();
                final TupleDomain<Symbol> transformedTupleDomain = tupleDomain.transform(str -> new Symbol(str));
                final Expression expr = DomainTranslator.toPredicate(transformedTupleDomain);
                final Optional<RowExpression> rowExpression = converter.expressionToRowExpression(Optional.of(expr));
                final RowExpression combinedExpression = call(logicalExpressionSignature(LogicalBinaryExpression.Type.AND),
                    BOOLEAN,
                    rowExpression.get(),
                    staticFilter.get());
                dynamicPageProcessor = Optional.of(exprCompiler.compilePageProcessor(Optional.of(combinedExpression), projections).get());
            }
            else if (jsonResponse.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                dynamicPageProcessor = Optional.empty();
            }
            else {
                dynamicPageProcessor = null;
            }
        }
        catch (Exception e) {
            dynamicPageProcessor = null;
        }
    }

    public static class DynamicFilterAndProjectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<PageProcessor> processor;
        private final List<Type> types;
        private final List<RowExpression> projections;
        private final ExpressionCompiler exprCompiler;
        private final Set<DynamicFilter> dynamicFilters;
        private final Optional<RowExpression> staticFilter;
        private final DynamicFilterClientSupplier dynamicFilterClientSupplier;
        private final String sourceId;
        private final RowExpressionConverter converter;
        private boolean closed;

        public DynamicFilterAndProjectOperatorFactory(int operatorId, PlanNodeId planNodeId,
                Supplier<PageProcessor> processor, List<Type> types,
                List<RowExpression> projections, ExpressionCompiler exprCompiler,
                Optional<RowExpression> staticFilter, Set<DynamicFilter> dynamicFilters,
                DynamicFilterClientSupplier dynamicFilterClientSupplier, String sourceId,
                RowExpressionConverter converter)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = requireNonNull(processor, "processor is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.projections = requireNonNull(projections, "projections is null");
            this.exprCompiler = requireNonNull(exprCompiler, "exprCompiler is null");
            this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
            this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
            this.dynamicFilterClientSupplier = requireNonNull(dynamicFilterClientSupplier, "dynamicFilterClientSupplier is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
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
            DynamicFilterClient client = dynamicFilterClientSupplier.createClient(
                    driverContext.getPipelineContext().getTaskId(), sourceId, -1, -1);
            return new DynamicFilterAndProjectOperator(operatorContext, types, processor.get(),
                    projections, exprCompiler, dynamicFilters, staticFilter, client, converter);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DynamicFilterAndProjectOperatorFactory(operatorId, planNodeId,
                    processor, types, projections, exprCompiler, staticFilter, dynamicFilters,
                    dynamicFilterClientSupplier, sourceId, converter);
        }
    }
}
