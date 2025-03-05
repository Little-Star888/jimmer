package org.babyfish.jimmer.sql.fetcher.impl;

import org.babyfish.jimmer.meta.EmbeddedLevel;
import org.babyfish.jimmer.meta.ImmutableProp;
import org.babyfish.jimmer.meta.TargetLevel;
import org.babyfish.jimmer.sql.ast.table.Table;
import org.babyfish.jimmer.sql.fetcher.*;
import org.jetbrains.annotations.NotNull;

class FieldConfigImpl<E, T extends Table<E>> implements RecursiveReferenceFieldConfig<E, T>, RecursiveListFieldConfig<E, T> {

    private ImmutableProp prop;

    private FetcherImpl<?> childFetcher;

    private FieldFilter<T> filter;

    private int batchSize;

    private int limit = Integer.MAX_VALUE;

    private int offset = 0;

    private ReferenceFetchType fetchType;

    private RecursionStrategy<E> recursionStrategy;

    private Field recursionField;

    FieldConfigImpl(ImmutableProp prop, FetcherImpl<?> childFetcher) {
        if (childFetcher != null && !prop.isAssociation(TargetLevel.ENTITY) && !prop.isEmbedded(EmbeddedLevel.BOTH)) {
            throw new IllegalArgumentException(
                    "Child fetcher cannot be specified because'" +
                            prop +
                            "' is neither entity association nor embeddable"
            );
        }
        this.fetchType = ReferenceFetchType.AUTO;
        this.prop = prop;
        this.childFetcher = childFetcher;
    }

    @Override
    public FieldConfigImpl<E, T> filter(FieldFilter<T> filter) {
        if (filter != null && prop.isReference(TargetLevel.PERSISTENT) && !prop.isNullable()) {
            throw new IllegalArgumentException(
                    "Cannot set filter for non-null one-to-one/many-to-one property \"" + prop + "\""
            );
        }
        this.filter = filter;
        return this;
    }

    @Override
    public FieldConfigImpl<E, T> batch(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("batchSize cannot be less than 0");
        }
        batchSize = size;
        return this;
    }

    @Override
    public FieldConfigImpl<E, T> limit(int limit, int offset) {
        if (!prop.isReferenceList(TargetLevel.PERSISTENT)) {
            throw new IllegalArgumentException(
                    "Cannot set limit because current property \"" +
                            prop +
                            "\" is not list property"
            );
        }
        if (limit < 0) {
            throw new IllegalArgumentException("'limit' can not be less than 0");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("'offset' can not be less than 0");
        }
        if (limit > Integer.MAX_VALUE - offset) {
            throw new IllegalArgumentException("'limit' > Integer.MAX_VALUE - offset");
        }
        this.limit = limit;
        this.offset = offset;
        return this;
    }

    @Override
    public FieldConfigImpl<E, T> fetchType(ReferenceFetchType fetchType) {
        this.fetchType = fetchType != null ? fetchType : ReferenceFetchType.AUTO;
        return this;
    }

    @Override
    public FieldConfigImpl<E, T> depth(int depth) {
        return recursive(DefaultRecursionStrategy.of(depth));
    }

    @SuppressWarnings("unchecked")
    @Override
    public FieldConfigImpl<E, T> recursive(RecursionStrategy<E> strategy) {
        if (strategy != null) {
            Class<?> declaringType;
            if (strategy instanceof ManyToManyViewRecursionStrategy<?>) {
                ManyToManyViewRecursionStrategy<?> viewStrategy = (ManyToManyViewRecursionStrategy<?>) strategy;
                declaringType = viewStrategy.getField().getProp().getDeclaringType().getJavaClass();
                strategy = (RecursionStrategy<E>) viewStrategy.getField().getRecursionStrategy();
                recursionField = viewStrategy.getField();
            } else {
                declaringType = prop.getDeclaringType().getJavaClass();
            }
            if (!declaringType.isAssignableFrom(prop.getTargetType().getJavaClass())) {
                throw new IllegalArgumentException(
                        "Cannot set recursive strategy because current property \"" +
                                prop +
                                "\" is not recursive property"
                );
            }
        }
        this.recursionStrategy = strategy;
        return this;
    }

    ImmutableProp getProp() {
        return prop;
    }

    FetcherImpl<?> getChildFetcher() {
        return childFetcher;
    }

    FieldFilter<T> getFilter() {
        return filter;
    }

    int getBatchSize() {
        return batchSize;
    }

    int getLimit() {
        return limit;
    }

    int getOffset() {
        return offset;
    }

    @NotNull
    public ReferenceFetchType getFetchType() {
        return fetchType;
    }

    final RecursionStrategy<E> getRecursionStrategy() {
        return recursionStrategy;
    }

    final Field getRecursionField() {
        return recursionField;
    }

    void setRecursiveTarget(FetcherImpl<?> fetcher) {
        if (this.childFetcher != null) {
            throw new IllegalStateException("childFetcher has already been set");
        }
        if (!prop.isAssociation(TargetLevel.ENTITY) ||
                !prop.getDeclaringType().isEntity() ||
                !prop.getDeclaringType().isAssignableFrom(prop.getTargetType())
        ) {
            throw new IllegalStateException("current property is not recursive");
        }
        this.childFetcher = fetcher;
    }
}
