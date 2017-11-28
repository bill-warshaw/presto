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
package com.facebook.presto.util;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;

public class RowExpressionConverter
{
    private final Map<Symbol, Integer> sourceLayout;
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final FunctionRegistry funcRegistry;
    private final TypeManager typeManager;
    private final Session session;

    public RowExpressionConverter(Map<Symbol, Integer> sourceLayout, Map<NodeRef<Expression>,
            Type> expressionTypes, FunctionRegistry funcRegistry, TypeManager typeManager, Session session)
    {
        this.sourceLayout = sourceLayout;
        this.expressionTypes = expressionTypes;
        this.funcRegistry = funcRegistry;
        this.typeManager = typeManager;
        this.session = session;
    }

    public Optional<RowExpression> expressionToRowExpression(Optional<Expression> staticFilter)
    {
        SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
        Optional<Expression> rewrittenFilter = staticFilter.map(symbolToInputRewriter::rewrite);
        return rewrittenFilter.map(filter -> toRowExpression(filter, expressionTypes));
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    /*private Map<NodeRef<Expression>, Type> getClonedTypes(Map<NodeRef<Expression>, Type> types, Expression exp)
    {
        ImmutableMap.Builder<NodeRef<Expression>, Type> builder = ImmutableMap.builder();
        types.entrySet().stream().forEach(e -> builder.put(e.getKey(), e.getValue()));
        Map<NodeRef<Expression>, Type> nodeRefTypeMap = new HashMap<>();
        new Visitor(types, nodeRefTypeMap).process(exp);
        builder.putAll(nodeRefTypeMap);
        return builder.build();
    }

    public class Visitor extends AstVisitor<Void, Void>
    {
        Map<NodeRef<Expression>, Type> types;
        Map<NodeRef<Expression>, Type> builder;
        Visitor(Map<NodeRef<Expression>, Type> types, Map<NodeRef<Expression>, Type> builder)
        {
            this.types = types;
            this.builder = builder;
        }

        protected Void visitFieldReference(FieldReference node, Void context)
        {
            if (types.get(NodeRef.of(node)) == null) {
                types.entrySet().stream().filter(new Predicate<Map.Entry<NodeRef<Expression>, Type>>() {
                    @Override
                    public boolean test(Map.Entry<NodeRef<Expression>, Type> entry)
                    {
                        if (entry.getKey().getNode() instanceof FieldReference) {
                            return entry.getKey().getNode().equals(node);
                        }
                        else {
                            return false;
                        }
                    }
                }).forEach(e -> builder.put(NodeRef.of(node), e.getValue()));
            }
            return null;
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void context)
        {
            process(node.getLeft(), context);
            process(node.getRight(), context);
            return null;
        }

        @Override
        protected Void visitNode(Node node, Void context)
        {
            node.getChildren().stream().forEach(n -> process(n, context));
            return null;
        }
    }*/

    private RowExpression toRowExpression(Expression expression, Map<NodeRef<Expression>, Type> types)
    {
        return SqlToRowExpressionTranslator.translateExpression(expression, SCALAR, types,
            funcRegistry, typeManager, session, true);
    }
}
