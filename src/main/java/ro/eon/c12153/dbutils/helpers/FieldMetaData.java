package ro.any.c12153.dbutils.helpers;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class for BaseDBConn class.Keeps information about database table column: field name, is field reserved, is field name in table, JDBC type of field. * 
 * @author C12153
 * @param <T>
 */
public class FieldMetaData<T> {
    
    private String uploadName;
    private String sqlName;
    private T defaultValue;
    private int sqlTip;
    private boolean sqlFunction;
    private boolean readonly;
    private boolean inTable;
    private boolean reserved;

    public FieldMetaData(){
        this.sqlFunction = false;
        this.readonly = false;
        this.inTable = false;
        this.reserved = false;
    }

    public FieldMetaData(String uploadName, String sqlName, T defaultValue, int sqlTip, boolean sqlFunction, boolean readonly, boolean inTable, boolean reserved) {
        this.uploadName = uploadName;
        this.sqlName = sqlName;
        this.defaultValue = defaultValue;
        this.sqlTip = sqlTip;
        this.sqlFunction = sqlFunction;
        this.readonly = readonly;
        this.inTable = inTable;
        this.reserved = reserved;
    }
    
    public static String getSqlInsertStatementColumnText(final Stream<FieldMetaData<?>> fields){
        return fields
                .filter(x -> x.isInTable() && !x.isReadonly())
                .map(x -> x.getSqlName())
                .collect(Collectors.joining(","));
    }
    
    public static String getSqlInsertStatementParameterText(final Stream<FieldMetaData<?>> fields){
        
        Function<FieldMetaData<?>, String> map = field -> {
            String rezultat = "?";
            if (field.isSqlFunction())
                rezultat = String.class.isInstance(field.getDefaultValue()) ? (String) field.getDefaultValue() : "null";
            return rezultat;
        };
        
        return fields
                .filter(x -> x.isInTable() && !x.isReadonly())
                .map(map::apply)
                .collect(Collectors.joining(","));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + Objects.hashCode(this.getSqlName());
        return hash;
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object o) {
        if (o == null) return false;
        if (o == this) return true;
        if (this.getClass().isInstance(o)) return o.hashCode() == this.hashCode();
        return false;
    }

    public String getUploadName() {
        return uploadName;
    }

    public void setUploadName(String uploadName) {
        this.uploadName = uploadName;
    }

    public String getSqlName() {
        return sqlName;
    }

    public void setSqlName(String sqlName) {
        this.sqlName = sqlName;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public int getSqlTip() {
        return sqlTip;
    }

    public void setSqlTip(int sqlTip) {
        this.sqlTip = sqlTip;
    }

    public boolean isSqlFunction() {
        return sqlFunction;
    }

    public void setSqlFunction(boolean sqlFunction) {
        this.sqlFunction = sqlFunction;
    }

    public boolean isReadonly() {
        return readonly;
    }

    public void setReadonly(boolean readonly) {
        this.readonly = readonly;
    }

    public boolean isInTable() {
        return inTable;
    }

    public void setInTable(boolean inTable) {
        this.inTable = inTable;
    }

    public boolean isReserved() {
        return reserved;
    }

    public void setReserved(boolean reserved) {
        this.reserved = reserved;
    }
}
