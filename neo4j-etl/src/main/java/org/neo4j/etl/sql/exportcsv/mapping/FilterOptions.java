package org.neo4j.etl.sql.exportcsv.mapping;

import org.neo4j.etl.sql.metadata.TableName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FilterOptions {
    public static FilterOptions DEFAULT = new FilterOptions(
            TinyIntAs.BYTE,
            RelationshipNameFrom.TABLE_NAME,
            ExclusionMode.NONE,
            Collections.EMPTY_LIST,
            false);
    private final TinyIntAs tinyIntAs;
    private final RelationshipNameFrom relationshipNameFrom;
    private final ExclusionMode exclusionMode;
    private final List<String> tablesToExclude;
    private final boolean excludeIncompleteJoinTables;

    public FilterOptions(String tinyIntAs,
                         String relationshipNameFrom,
                         String exclusionMode,
                         List<String> tablesToExclude,
                         boolean excludeIncompleteJoinTables) {
        this(
                TinyIntAs.parse(tinyIntAs),
                RelationshipNameFrom.parse(relationshipNameFrom),
                ExclusionMode.parse(exclusionMode),
                tablesToExclude,
                excludeIncompleteJoinTables);
    }

    private FilterOptions(TinyIntAs tinyIntAs,
                          RelationshipNameFrom relationshipNameFrom,
                          ExclusionMode exclusionMode,
                          List<String> tablesToExclude,
                          boolean excludeIncompleteJoinTables) {
        this.tinyIntAs = tinyIntAs;
        this.relationshipNameFrom = relationshipNameFrom;

        this.exclusionMode = exclusionMode;
        this.excludeIncompleteJoinTables = excludeIncompleteJoinTables;

        this.tablesToExclude = exclusionMode.equals(ExclusionMode.NONE) ? Collections.EMPTY_LIST : tablesToExclude;
    }

    public TinyIntAs tinyIntAs() {
        return tinyIntAs;
    }

    public RelationshipNameFrom relationshipNameFrom() {
        return relationshipNameFrom;
    }

    public ExclusionMode exclusionMode() {
        return exclusionMode;
    }

    public List<String> tablesToExclude() {
        return tablesToExclude;
    }

    public boolean excludeIncompleteJoinTables() {
        return excludeIncompleteJoinTables;
    }

    public void invertTables(Collection<TableName> tableNames) {
        List<String> invertedTablesToExclude = new ArrayList<String>();

        for (TableName tableName : tableNames) {
            if (!tablesToExclude.contains(tableName.simpleName())) {
                invertedTablesToExclude.add(tableName.simpleName());
            }
        }

        tablesToExclude.clear();

        for (String tableName : invertedTablesToExclude) {
            tablesToExclude.add(tableName);
        }
    }

    // TODO: to be implemented
    public List<String> getColumnTypesToExclude() {
        return null;
    }
}
