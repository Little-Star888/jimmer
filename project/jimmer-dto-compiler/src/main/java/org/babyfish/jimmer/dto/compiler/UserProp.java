package org.babyfish.jimmer.dto.compiler;

import org.antlr.v4.runtime.Token;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;

public class UserProp implements AbstractProp, AbstractPropBuilder {

    private final String alias;

    private final int line;

    private final int col;

    private final TypeRef typeRef;

    @Nullable
    private final String defaultValueText;

    private final List<Anno> annotations;

    @Nullable
    private final String doc;

    public UserProp(
            Token alias,
            TypeRef typeRef,
            @Nullable String defaultValueText,
            List<Anno> annotations,
            @Nullable String doc
    ) {
        this.alias = alias.getText();
        this.line = alias.getLine();
        this.col = alias.getCharPositionInLine();
        this.typeRef = typeRef;
        this.defaultValueText = defaultValueText;
        this.annotations = annotations;
        this.doc = doc;
    }

    @Override
    public String getName() {
        return alias;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public boolean isNullable() {
        return typeRef.isNullable();
    }

    @Override
    public int getAliasLine() {
        return line;
    }

    @Override
    public int getAliasColumn() {
        return col;
    }

    public TypeRef getTypeRef() {
        return typeRef;
    }

    @Nullable
    public String getDefaultValueText() {
        return defaultValueText;
    }

    public List<Anno> getAnnotations() {
        return annotations;
    }

    @Nullable
    public String getDoc() {
        return doc;
    }

    @Override
    public AliasPattern getAliasPattern() {
        return null;
    }

    public UserProp build(DtoType<?, ?> type) {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserProp userProp = (UserProp) o;
        return line == userProp.line && Objects.equals(alias, userProp.alias) && Objects.equals(typeRef, userProp.typeRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, line, typeRef);
    }

    @Override
    public String toString() {
        if (annotations.isEmpty()) {
            return alias + ": " + typeRef;
        }
        StringBuilder builder = new StringBuilder();
        for (Anno anno : annotations) {
            builder.append(anno);
        }
        builder.append(' ').append("alias").append(": ").append(typeRef);
        return builder.toString();
    }
}
