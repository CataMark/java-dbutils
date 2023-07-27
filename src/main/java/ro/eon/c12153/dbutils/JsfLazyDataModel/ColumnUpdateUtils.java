package ro.any.c12153.dbutils.JsfLazyDataModel;

import java.util.Optional;

import ro.any.c12153.dbutils.helpers.ParamSql;

/**
 *
 * @author C12153
 */
public class ColumnUpdateUtils {
    
    public static Optional<String> sqlFieldUpdate(ColumnUpdateValueHolder<?> camp){
        String rezultat = null;
        
        if (camp.isObligatoriu()){
            if (camp.getValue() != null &&
                    (camp.getValue() instanceof String ? !((String) camp.getValue()).isEmpty() : true))
                rezultat = camp.getCod() + " = ?";
        } else {
            if (camp.isGoleste()){
                rezultat = camp.getCod() + " = null";
            } else if (camp.getValue() != null &&
                    (camp.getValue() instanceof String ? !((String) camp.getValue()).isEmpty() : true)){
                rezultat = camp.getCod() + " = ?";
            }
        }
        return Optional.ofNullable(rezultat);
    }
    
    public static Optional<ParamSql> sqlFieldParametru(ColumnUpdateValueHolder<?> camp){
        ParamSql rezultat = null;
        
        if (!camp.isGoleste() && camp.getValue() != null &&
                    (camp.getValue() instanceof String ? !((String) camp.getValue()).isEmpty() : true))
            rezultat = new ParamSql(camp.getValue(), camp.getSqlType());
        
        return Optional.ofNullable(rezultat);
    }
}
