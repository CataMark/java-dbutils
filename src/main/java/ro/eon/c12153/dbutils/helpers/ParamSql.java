package ro.any.c12153.dbutils.helpers;

/**
 * Helper class for BaseDBConn class.
 * 
 * This class is used to pass parameters for methods of BaseDBConn class,
 * which in the end will be passed to the SQL query parameters in that method,
 * in a standardized way, also including JDBC Type of the database table column.
 * @author C12153
 */
public class ParamSql {
    
    private Object valoare; //valoare obiect
    private int tip; //tip date sql
    
    /**
     * empty default constructor
     */
    public ParamSql() {
    }

    /**
     * Constructor with initialization.
     * @param valoare - Object: value of the SQL query parameter, should by passed only native java data types
     * @param tip - int: JDBC type of the database table column
     */
    public ParamSql(Object valoare, int tip) {
        this.valoare = valoare;
        this.tip = tip;
    }
    
    /**
     * Getter for the value of the SQL query parameter,
     * should by passed only native java data types.
     * @return Object
     */
    public Object getValoare() {
        return valoare;
    }
    
    /**
     * Setter for the value of the SQL query parameter,
     * should by passed only native java data types.
     * @param valoare - Object
     */
    public void setValoare(Object valoare) {
        this.valoare = valoare;
    }
    
    /**
     * Getter for the JDBC Type of the database table column
     * @return JDBC Type (int)
     */
    public int getTip() {
        return tip;
    }
    
    /**
     * Setter for the JDBC Type of the database table column
     * @param tip - JDBC Type (int)
     */
    public void setTip(int tip) {
        this.tip = tip;
    }
}
