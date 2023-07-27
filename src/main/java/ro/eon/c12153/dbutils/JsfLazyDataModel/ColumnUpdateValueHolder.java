package ro.any.c12153.dbutils.JsfLazyDataModel;

import java.io.Serializable;

/**
 *
 * @author C12153
 * @param <T>
 */
public class ColumnUpdateValueHolder<T> implements Serializable{
    private static final long serialVersionUID = 1L;
    
    private final String cod;
    private final String nume;
    private T value;
    private final boolean obligatoriu;
    private boolean goleste;
    private final int sqlType;

    public ColumnUpdateValueHolder(String cod, String nume, boolean obligatoriu, int sqlType) {
        this.cod = cod;
        this.nume = nume;
        this.obligatoriu = obligatoriu;
        this.sqlType = sqlType;
    }

    public String getCod() {
        return cod;
    }

    public String getNume() {
        return nume;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public boolean isObligatoriu() {
        return obligatoriu;
    }

    public boolean isGoleste() {
        return goleste;
    }

    public void setGoleste(boolean goleste) {
        this.goleste = goleste;
    }

    public int getSqlType() {
        return sqlType;
    }
}
