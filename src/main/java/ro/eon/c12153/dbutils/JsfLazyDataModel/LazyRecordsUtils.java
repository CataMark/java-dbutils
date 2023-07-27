package ro.any.c12153.dbutils.JsfLazyDataModel;

import java.util.Map;
import java.util.stream.Collectors;

import ro.any.c12153.dbutils.Constante;

/**
 *
 * @author C12153
 */
public class LazyRecordsUtils {
    
    public static boolean noSqlInject(String input) {
        String[] dictionar = new String[]{"drop", "trunc", "grant", "revoke", "alter", "create", "select", "insert", "update", "delete"};
        for (String termen : dictionar) {
            if (input.toLowerCase().contains(termen)) {
                return false;
            }
        }
        return true;
    }
    
    public static String getSortSql(Map<String, String> sort){
        return sort.entrySet().stream()
                    .map(x -> x.getKey() + " " + x.getValue())
                    .collect(Collectors.joining(",")) + ",";
    }
    
    private static String getFilterFieldSql(String key, String value){
        String rezultat;
        if (key.equals("mod_timp")){
            rezultat = "convert(varchar(25), mod_timp, 120)";
        } else {
            rezultat = key;
        }
        rezultat += (value.equals(Constante.SQL_FILTER_NULL_KEY) ? (" " + Constante.MSSQL_FILTER_NULL) : (" like '" + value + "'"));
        return rezultat;
    }
    
    public static String getFilterSql(Map<String, String> filter){
        String rezultat = filter.entrySet().stream()
                    .filter(x -> x.getValue() != null && !x.getValue().isEmpty() && noSqlInject(x.getValue()))
                    .map(x -> getFilterFieldSql(x.getKey(), x.getValue()))
                    .collect(Collectors.joining(" and "));
        return (rezultat == null || rezultat.isEmpty() ? "" : " and " + rezultat );
    }
}
