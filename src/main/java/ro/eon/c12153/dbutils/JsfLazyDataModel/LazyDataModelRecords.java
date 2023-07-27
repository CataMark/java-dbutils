package ro.any.c12153.dbutils.JsfLazyDataModel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author C12153
 * @param <T>
 */
public class LazyDataModelRecords<T> implements Serializable{
    private static final long serialVersionUID = 1L;
    
    public LazyDataModelRecords(){
        this.records = new ArrayList<>();
    }
    
    private List<T> records;
    private int pozitii;
    private double suma;

    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public List<T> getRecords() {
        return records;
    }

    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    public void setRecords(List<T> records) {
        this.records = records;
    }

    public int getPozitii() {
        return pozitii;
    }

    public void setPozitii(int pozitii) {
        this.pozitii = pozitii;
    }

    public double getSuma() {
        return suma;
    }

    public void setSuma(double suma) {
        this.suma = suma;
    }
}
