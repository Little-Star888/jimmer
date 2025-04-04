package org.babyfish.jimmer.sql.ast.impl.mutation;

import org.babyfish.jimmer.meta.*;
import org.babyfish.jimmer.runtime.DraftSpi;
import org.babyfish.jimmer.runtime.ImmutableSpi;
import org.babyfish.jimmer.sql.ast.impl.value.PropertyGetter;
import org.babyfish.jimmer.sql.ast.mutation.QueryReason;
import org.babyfish.jimmer.sql.dialect.Dialect;
import org.babyfish.jimmer.sql.fetcher.Fetcher;
import org.babyfish.jimmer.sql.fetcher.IdOnlyFetchType;
import org.babyfish.jimmer.sql.fetcher.impl.FetcherImpl;
import org.babyfish.jimmer.sql.meta.IdGenerator;
import org.babyfish.jimmer.sql.meta.impl.IdentityIdGenerator;

import java.sql.BatchUpdateException;
import java.util.*;

class EntityInvestigator {

    private final int[] rowCounts;

    private final SaveContext ctx;

    private final Shape shape;

    private final Collection<? extends DraftSpi> entities;

    private final boolean updatable;

    private final ImmutableProp idProp;

    private final KeyMatcher keyMatcher;

    private final Map<ImmutableType, Fetcher<ImmutableSpi>> idFetcherMap =
            new HashMap<>();

    private final List<ImmutableProp> missedProps;
    
    private final boolean isIdMissed;

    private final KeyMatcher.Group primaryGroup;

    private final String uncheckedGroupName;

    EntityInvestigator(
            int[] rowCounts,
            SaveContext ctx,
            Shape shape,
            Collection<? extends DraftSpi> entities,
            boolean updatable
    ) {
        this.rowCounts = rowCounts;
        this.ctx = ctx;
        this.shape = shape;
        this.entities = entities;
        this.updatable = updatable;
        this.idProp = ctx.path.getType().getIdProp();
        this.keyMatcher = ctx.options.getKeyMatcher(ctx.path.getType());
        this.missedProps = keyMatcher.missedProps(shape.getGetterMap().keySet());
        this.isIdMissed = shape.getIdGetters().isEmpty() &&
                shape.getType().getIdGenerator(ctx.options.getSqlClient()) instanceof IdentityIdGenerator;
        this.primaryGroup = isIdMissed ?
                keyMatcher.match(shape.getGetterMap().keySet()) :
                null;
        this.uncheckedGroupName = isIdMissed && updatable ?
                primaryGroup != null ?
                        primaryGroup.getName() :
                        null
                : null;
    }

    public Exception investigate() {
        Dialect dialect = ctx.options.getSqlClient().getDialect();
        if (dialect.isBatchDumb() || dialect.isBatchUpdateExceptionUnreliable()) {
            fillMissedProps(entities);
            Exception translated = translateAll();
            if (translated != null) {
                return translated;
            }
        } else {
            if (rowCounts.length >= 10) {
                int failedCount = 0;
                for (int rowCount : rowCounts) {
                    if (rowCount < 0 && ++failedCount >= 10) {
                        return translateAll();
                    }
                }
            }
            int index = 0;
            for (DraftSpi entity : entities) {
                if (index >= rowCounts.length || rowCounts[index++] < 0) {
                    fillMissedProps(Collections.singletonList(entity));
                    Exception translated = translateOne(entity);
                    if (translated != null) {
                        return translated;
                    }
                }
            }
        }
        return null;
    }

    private List<ImmutableProp> fillMissedProps(Collection<? extends DraftSpi> drafts) {
        if (missedProps.isEmpty()) {
            return Collections.emptyList();
        }
        if (!isIdMissed) {
            PropId idPropId = idProp.getId();
            Map<Object, ImmutableSpi> rowMap = Rows.findMapByIds(
                    ctx,
                    QueryReason.INVESTIGATE_CONSTRAINT_VIOLATION_ERROR,
                    idFetcher(null, missedProps),
                    drafts
            );
            for (DraftSpi draft : drafts) {
                if (draft.__isLoaded(idPropId)) {
                    ImmutableSpi row = rowMap.get(draft.__get(idPropId));
                    if (row != null) {
                        for (ImmutableProp missedProp : missedProps) {
                            PropId missedPropId = missedProp.getId();
                            draft.__set(missedPropId, row.__get(missedPropId));
                        }
                    }
                }
            }
        } else {
            KeyMatcher.Group group = keyMatcher.match(shape.getGetterMap().keySet());
            if (group == null) {
                return Collections.emptyList();
            }
            Map<Object, ImmutableSpi> rowMap = Rows.findMapByKeys(
                    ctx,
                    QueryReason.INVESTIGATE_CONSTRAINT_VIOLATION_ERROR,
                    keyFetcher(group.getProps(), missedProps),
                    drafts,
                    keyMatcher.getGroup(group.getName())
            ).values().iterator().next();
            for (DraftSpi draft : drafts) {
                Object key = Keys.keyOf(draft, group.getProps());
                ImmutableSpi row = rowMap.get(key);
                if (row != null) {
                    for (ImmutableProp missedProp : missedProps) {
                        PropId missedPropId = missedProp.getId();
                        draft.__set(missedPropId, row.__get(missedPropId));
                    }
                }
            }
        }
        return missedProps;
    }

    private Exception translateOne(DraftSpi entity) {
        PropId idPropId = idProp.getId();
        if (!updatable && !isIdMissed) {
            List<ImmutableSpi> rows = Rows.findByIds(
                    ctx,
                    QueryReason.INVESTIGATE_CONSTRAINT_VIOLATION_ERROR,
                    idFetcher(null),
                    Collections.singletonList(entity)
            );
            if (!rows.isEmpty()) {
                return ctx.createConflictId(idProp, entity.__get(idPropId));
            }
        }
        for (Map.Entry<String, Set<ImmutableProp>> e : keyMatcher.toMap().entrySet()) {
            String groupName = e.getKey();
            if (groupName.equals(uncheckedGroupName)) {
                continue;
            }
            Set<ImmutableProp> keyProps = e.getValue();
            if (!keyProps.isEmpty() &&
                    containsAny(shape.getGetterMap().keySet(), keyProps) &&
                    (!updatable || !isIdMissed || primaryGroup != null)) {
                Map<KeyMatcher.Group, List<ImmutableSpi>> rowsMap = Rows.findByKeys(
                        ctx,
                        QueryReason.INVESTIGATE_CONSTRAINT_VIOLATION_ERROR,
                        idFetcher(null, primaryGroup != null ? primaryGroup.getProps() : null),
                        Collections.singletonList(entity),
                        keyMatcher.getGroup(groupName)
                );
                if (!rowsMap.isEmpty()) {
                    List<ImmutableSpi> rows = rowsMap.values().iterator().next();
                    if (!rows.isEmpty()) {
                        ImmutableSpi row = rows.iterator().next();
                        if (!isSameIdentifier(entity, row, idPropId, primaryGroup)) {
                            return ctx.createConflictKey(keyProps, Keys.keyOf(entity, keyProps));
                        }
                    }
                }
            }
        }
        for (ImmutableProp prop : entity.__type().getProps().values()) {
            PropId propId = prop.getId();
            if (entity.__isLoaded(propId) &&
                    prop.isColumnDefinition() &&
                    prop.isTargetForeignKeyReal(ctx.options.getSqlClient().getMetadataStrategy()) &&
                    prop.isReference(TargetLevel.PERSISTENT) &&
                    !prop.isRemote() &&
                    !ctx.options.isAutoCheckingProp(prop)
            ) {
                Object associatedObject = entity.__get(propId);
                if (associatedObject == null) {
                    continue;
                }
                PropId associatedIdPropId = prop.getTargetType().getIdProp().getId();
                if (!((ImmutableSpi) associatedObject).__isLoaded(associatedIdPropId)) {
                    continue;
                }
                Object associatedId = ((ImmutableSpi)associatedObject).__get(associatedIdPropId);
                List<ImmutableSpi> rows = Rows.findRows(
                        ctx.prop(prop),
                        QueryReason.INVESTIGATE_CONSTRAINT_VIOLATION_ERROR,
                        idFetcher(prop.getTargetType()),
                        (q, t) -> {
                            q.where(t.getId().eq(associatedId));
                        }
                );
                if (rows.isEmpty()) {
                    return ctx.prop(prop).createIllegalTargetId(Collections.singleton(associatedId));
                }
            }
        }
        return null;
    }

    private Exception translateAll() {
        if (!updatable && !isIdMissed) {
            PropId idPropId = idProp.getId();
            Map<Object, ImmutableSpi> rowMap = Rows.findMapByIds(
                    ctx,
                    QueryReason.INVESTIGATE_CONSTRAINT_VIOLATION_ERROR,
                    idFetcher(null),
                    entities
            );
            for (ImmutableSpi entity : entities) {
                Object id = entity.__get(idPropId);
                if (rowMap.containsKey(id)) {
                    return ctx.createConflictId(idProp, id);
                }
                rowMap.put(id, entity);
            }
        }
        for (Map.Entry<String, Set<ImmutableProp>> e : keyMatcher.toMap().entrySet()) {
            String groupName = e.getKey();
            if (groupName.equals(uncheckedGroupName)) {
                continue;
            }
            Set<ImmutableProp> keyProps = e.getValue();
            if (!keyProps.isEmpty() &&
                    containsAny(shape.getGetterMap().keySet(), keyProps) &&
                    (!updatable || !isIdMissed || primaryGroup != null)) {
                Map<Object, ImmutableSpi> rowMap = Rows.findMapByKeys(
                        ctx,
                        QueryReason.INVESTIGATE_CONSTRAINT_VIOLATION_ERROR,
                        keyFetcher(keyProps, primaryGroup != null ? primaryGroup.getProps() : null),
                        entities,
                        keyMatcher.getGroup(groupName)
                ).values().iterator().next();
                if (rowMap == null || rowMap.isEmpty()) {
                    continue;
                }
                PropId idPropId = idProp.getId();
                for (ImmutableSpi entity : entities) {
                    Object key = Keys.keyOf(entity, keyProps);
                    ImmutableSpi row = rowMap.get(key);
                    if (row != null) {
                        if (!isSameIdentifier(entity, row, idPropId, primaryGroup)) {
                            return ctx.createConflictKey(keyProps, Keys.keyOf(entity, keyProps));
                        }
                    } else {
                        rowMap.put(key, entity);
                    }
                }
            }
        }
        Map<ImmutableProp, Set<Object>> targetIdMultiMap = new LinkedHashMap<>();
        for (ImmutableSpi entity : entities) {
            for (ImmutableProp prop : entity.__type().getProps().values()) {
                PropId propId = prop.getId();
                if (entity.__isLoaded(propId) &&
                        prop.isColumnDefinition() &&
                        prop.isTargetForeignKeyReal(ctx.options.getSqlClient().getMetadataStrategy()) &&
                        prop.isReference(TargetLevel.PERSISTENT) &&
                        !prop.isRemote() &&
                        !ctx.options.isAutoCheckingProp(prop)
                ) {
                    Object associatedObject = entity.__get(propId);
                    if (associatedObject == null) {
                        continue;
                    }
                    PropId targetIdPropId = prop.getTargetType().getIdProp().getId();
                    if (!((ImmutableSpi)associatedObject).__isLoaded(targetIdPropId)) {
                        continue;
                    }
                    Object associatedId = ((ImmutableSpi)associatedObject).__get(targetIdPropId);
                    targetIdMultiMap
                            .computeIfAbsent(prop, it -> new LinkedHashSet<>())
                            .add(associatedId);
                }
            }
        }
        for (Map.Entry<ImmutableProp, Set<Object>> e : targetIdMultiMap.entrySet()) {
            ImmutableProp prop = e.getKey();
            Set<Object> associatedIds = e.getValue();
            List<ImmutableSpi> rows = Rows.findRows(
                    ctx.prop(prop),
                    QueryReason.INVESTIGATE_CONSTRAINT_VIOLATION_ERROR,
                    idFetcher(prop.getTargetType()),
                    (q, t) -> {
                        q.where(t.getId().in(associatedIds));
                    }
            );
            PropId targetIdPropId = prop.getTargetType().getIdProp().getId();
            Set<Object> existingTargetIds = new HashSet<>((rows.size() * 4 + 2) / 3);
            for (ImmutableSpi row : rows) {
                existingTargetIds.add(row.__get(targetIdPropId));
            }
            for (Object associatedId : associatedIds) {
                if (!existingTargetIds.contains(associatedId)) {
                    return ctx.prop(prop).createIllegalTargetId(Collections.singleton(associatedId));
                }
            }
        }
        return null;
    }

    private boolean isSameIdentifier(ImmutableSpi entity, ImmutableSpi row, PropId idPropId, KeyMatcher.Group primaryGroup) {
        if (!updatable) {
            return false;
        } else if (primaryGroup != null) {
            return Objects.equals(
                    Keys.keyOf(entity, primaryGroup.getProps()),
                    Keys.keyOf(row, primaryGroup.getProps())
            );
        } else {
            return entity.__get(idPropId).equals(row.__get(idPropId));
        }
    }

    @SuppressWarnings("unchecked")
    private Fetcher<ImmutableSpi> idFetcher(ImmutableType type) {
        return idFetcherMap.computeIfAbsent(type, t -> {
            if (t == null) {
                t = ctx.path.getType();
            }
            return new FetcherImpl<>((Class<ImmutableSpi>)t.getJavaClass());
        });
    }

    private Fetcher<ImmutableSpi> idFetcher(
            ImmutableType type,
            Iterable<ImmutableProp> missedProps
    ) {
        Fetcher<ImmutableSpi> fetcher = idFetcher(type);
        if (missedProps != null) {
            for (ImmutableProp missedProp : missedProps) {
                if (missedProp.isReference(TargetLevel.ENTITY)) {
                    fetcher = fetcher.add(missedProp.getName(), IdOnlyFetchType.RAW);
                } else {
                    fetcher = fetcher.add(missedProp.getName());
                }
            }
        }
        return fetcher;
    }

    @SuppressWarnings("unchecked")
    private Fetcher<ImmutableSpi> keyFetcher(Set<ImmutableProp> keyProps) {
        Fetcher<ImmutableSpi> fetcher = new FetcherImpl<>((Class<ImmutableSpi>)ctx.path.getType().getJavaClass());
        for (ImmutableProp keyProp : keyProps) {
            if (keyProp.isReference(TargetLevel.ENTITY)) {
                fetcher = fetcher.add(keyProp.getName(), IdOnlyFetchType.RAW);
            } else {
                fetcher = fetcher.add(keyProp.getName());
            }
        }
        return fetcher;
    }

    private Fetcher<ImmutableSpi> keyFetcher(
            Set<ImmutableProp> keyProps,
            Iterable<ImmutableProp> missedProps
    ) {
        Fetcher<ImmutableSpi> fetcher = keyFetcher(keyProps);
        if (missedProps != null) {
            for (ImmutableProp missedProp : missedProps) {
                if (missedProp.isReference(TargetLevel.ENTITY)) {
                    fetcher = fetcher.add(missedProp.getName(), IdOnlyFetchType.RAW);
                } else {
                    fetcher = fetcher.add(missedProp.getName());
                }
            }
        }
        return fetcher;
    }

    private static boolean containsAny(Collection<?> a, Collection<?> b) {
        for (Object be : b) {
            if (a.contains(be)) {
                return true;
            }
        }
        return false;
    }
}
