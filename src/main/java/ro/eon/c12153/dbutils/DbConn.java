package ro.any.c12153.dbutils;

import com.monitorjbl.xlsx.StreamingReader;
import ro.any.c12153.dbutils.helpers.ParamSql;
import ro.any.c12153.dbutils.helpers.FieldMetaData;
import ro.any.c12153.dbutils.helpers.FieldCheckResult;
import ro.any.c12153.dbutils.helpers.CallbackMethod;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.sql.DataSource;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import static org.apache.poi.ss.usermodel.CellType.FORMULA;
import static org.apache.poi.ss.usermodel.CellType.STRING;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

public class DbConn {
    private static final String ERR_FIELD_META_DATA_SQL_NAME = "No sqlName was set for field meta data!";
    private static final String ERR_FILE_HEAD_COL_NOK = "There are null or non-text values in the supplied table header!";
    private static final String ERR_FILE_HEAD_COL_EMPTY = "The supplied data doesn't contain a table header!";
    private static final String ERR_FILE_COL_WRITABLE_EMPTY = "Supplied table header doesn't contain any writable column name from the destionation table!";
    private static final String ERR_GOT_RESULT_SET = "The supplied query returned a result set! The query was executed!";
    private static final String ERR_NOT_RESULT_SET = "The supplied query didn't return a result set! The query was executed!";
	
    private final DataSource ds;
    private final int batchSize;
    private final String userId;
    private final List<FieldMetaData<?>> defaultReservedFields;

    public DbConn(final DataSource ds, final int batchSize, final String userId, final List<FieldMetaData<?>> defaultReservedFields) {
        this.ds = ds;
        this.batchSize = batchSize;
        this.userId = userId;
        this.defaultReservedFields = defaultReservedFields;
    }
	
    public Connection getConnection() throws Exception{
        return this.ds.getConnection();
    }
	
    private static String[] cleanFieldNames(String[] fieldNames) throws Exception{
        final String PATTERN = "[^a-zA-Z_ 0-9]";

        String[] rezultat = new String[fieldNames.length];            
        for (int i = 0; i < fieldNames.length; i++){
            rezultat[i] = fieldNames[i].trim().replaceAll(PATTERN, "");
        }    
        return rezultat;
    }
	
    @SuppressWarnings({"AssignmentToMethodParameter", "null"})
    private String batchErrorMessage(Exception ex, Integer pozitiiProcesate, long startTime){
        String rezultat;
        if (ex instanceof BatchUpdateException){
            BatchUpdateException exx = (BatchUpdateException) ex;
            pozitiiProcesate += exx.getUpdateCounts().length;

            String randuri_erori = "";
            for (int i=0; i<exx.getUpdateCounts().length; i++){
                if (exx.getUpdateCounts()[i]==Statement.EXECUTE_FAILED){
                    randuri_erori += (i==0? "": ", ") + (pozitiiProcesate - (pozitiiProcesate < this.batchSize ? pozitiiProcesate: this.batchSize) + i + 2);
                }
            }

            rezultat = exx.getMessage() + ". " +
                "Processed rows: " + pozitiiProcesate + ".\n" +
                "Time: " + ((System.currentTimeMillis() - startTime)/1000) + " sec.\n" + 
                "Error number: " + exx.getUpdateCounts().length + ".\n" +
                "Error rows: " + randuri_erori + ".";

        } else {
            rezultat = ex.getMessage() + "\n" +
                "Processed rows: " + pozitiiProcesate + ".\n" +
                "Time: " + ((System.currentTimeMillis() - startTime)/1000) + " sec.";
        }
        return rezultat;
    }
	
    public List<String> getTables() throws Exception{
        List<String> rezultat = new ArrayList<>();
        final String SQL="select (table_schema + '.' + table_name) as table_name from information_schema.tables " +
                "where table_type = 'BASE TABLE' order by table_schema asc, table_name asc;";
        
        try(Connection conn = this.getConnection();
            ResultSet records = conn.createStatement().executeQuery(SQL);) {

            while (records.next()){
                rezultat.add(records.getString(1));
            }
        }
        return rezultat;
    }
    
    private static List<FieldMetaData<?>> getReservedFields(final ResultSetMetaData tableMeta, final Optional<List<FieldMetaData<?>>> defaultReservedFields,
            final Optional<List<FieldMetaData<?>>> specificReservedFields) throws Exception{
        
        List<FieldMetaData<?>> rezultat = new ArrayList<>();
        if (defaultReservedFields.isPresent()) rezultat.addAll(defaultReservedFields.get());
        if (specificReservedFields.isPresent()) rezultat.addAll(specificReservedFields.get());

        for (FieldMetaData<?> field : rezultat){
            if (field.getSqlName() == null || field.getSqlName().isEmpty()) throw new Exception(ERR_FIELD_META_DATA_SQL_NAME);
            for (int i = 1; i <= tableMeta.getColumnCount(); i++){                    
                if (tableMeta.getColumnName(i).equalsIgnoreCase(field.getSqlName())){
                   field.setInTable(true);
                   field.setReadonly(tableMeta.isReadOnly(i));
                   field.setSqlTip(tableMeta.getColumnType(i));
                   break;
                }
            }
            field.setReserved(true);
        }
        return rezultat;
    }
    
    private static List<FieldMetaData<?>> getUploadFields(final ResultSetMetaData tableMeta, final String[] uploadNames,
            final Optional<Map<String, String>> fieldsNameMapping, final List<FieldMetaData<?>> reservedFields) throws Exception{
        
        List<FieldMetaData<?>> rezultat = new ArrayList<>();
            
        for (int i = 0; i < uploadNames.length; i++){
            if (uploadNames[i] == null || uploadNames[i].isEmpty()) throw new Exception(ERR_FILE_HEAD_COL_NOK);
            
            FieldMetaData<String> field = new FieldMetaData<>();            
            Function<String, String> nameMap = fileColumn -> {
                if (fieldsNameMapping.isPresent()) {
                    return Optional.ofNullable(fieldsNameMapping.get().get(fileColumn)).orElse(fileColumn);
                } else {
                    return fileColumn;
                }
            };
            field.setUploadName(uploadNames[i]);
            field.setSqlName(nameMap.apply(uploadNames[i]));
            field.setReserved(reservedFields.contains(field));

            for (int j = 1; j <= tableMeta.getColumnCount(); j++){
                if (tableMeta.getColumnName(j).equalsIgnoreCase(field.getSqlName())){
                    field.setInTable(true);
                    field.setReadonly(tableMeta.isReadOnly(j));
                    field.setSqlTip(tableMeta.getColumnType(j));
                    break;
                }
            }
            rezultat.add(field);
        }            
        if (!rezultat.stream().anyMatch(x -> x.isInTable() && !x.isReadonly() && !x.isReserved()))
            throw new Exception(ERR_FILE_COL_WRITABLE_EMPTY);
        return rezultat;
    }
	
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    public String loadText(final String tableName, final BufferedReader reader, final String delimitator,
            final Optional<String> quotes, final Optional<Map<String, Function<Object, FieldCheckResult>>> fieldCheck,
            final Optional<Map<String, String>> fieldsNameMapping, final Optional<List<FieldMetaData<?>>> specificReservedFields) throws Exception{
        
        //stabilire pattern pentru split text
        //***********************************
        String pattern; //variabila folosita pentru separarea valorilor din text
        String quotesUnescaped = ""; //se va retine caracterul ce delimiteaza valoarea din camp
        if (quotes.isPresent()){
            pattern = delimitator + "(?=(?:[^" + quotes.get() + "]*" + quotes.get() + "[^" + quotes.get() + "]*" + quotes.get() + ")*[^" + quotes.get() +"]*$)";
            quotesUnescaped = String.valueOf(quotes.get().charAt(quotes.get().length() - 1));
        } else {
            pattern = delimitator;
        }
        
        try(Connection conn = this.getConnection();){
            conn.setAutoCommit(false);
            final long startTime = System.currentTimeMillis();

            //obtinere cap de tabel din text
            //***********************************
            String newLine = reader.readLine();
            if (newLine == null || newLine.length() == 0) throw new Exception(ERR_FILE_HEAD_COL_EMPTY);
            String[] primaLinie = cleanFieldNames(newLine.split(pattern, -1));

            //obtinere meta data tabela
            //***********************************
            String meta_sql = "select * from " + tableName + " where 1=0;";
            ResultSetMetaData tableMeta = conn.createStatement().executeQuery(meta_sql).getMetaData();
            List<FieldMetaData<?>> reservedFields = DbConn.getReservedFields(tableMeta, Optional.ofNullable(this.defaultReservedFields), specificReservedFields);
            List<FieldMetaData<?>> uploadFields = DbConn.getUploadFields(tableMeta, primaLinie, fieldsNameMapping, reservedFields);
            
            //stabilire statement sql pentru insert
            //***********************************            
            String stmt_sql = "insert into " + tableName + " (";
            stmt_sql += FieldMetaData.getSqlInsertStatementColumnText(uploadFields.stream().filter(x -> !x.isReserved()));
            if (!reservedFields.isEmpty()) stmt_sql += "," + FieldMetaData.getSqlInsertStatementColumnText(reservedFields.stream());
            stmt_sql += ") values (";
            stmt_sql += FieldMetaData.getSqlInsertStatementParameterText(uploadFields.stream().filter(x -> !x.isReserved()));
            if (!reservedFields.isEmpty()) stmt_sql += "," + FieldMetaData.getSqlInsertStatementParameterText(reservedFields.stream());
            stmt_sql += ");";
            
            //procesare batch
            //*********************************** 
            int pozitiiProcesate = 0;
            try(PreparedStatement stmt = conn.prepareStatement(stmt_sql);){
                           
                int pozitii = 0;
                while(true){
                    newLine = reader.readLine();
                    if (newLine == null || newLine.length() == 0) break;

                    String[] linieValori = newLine.split(pattern, -1);
                    if (linieValori.length != uploadFields.size())
                        throw new Exception("Value count is not equal to columns count for row: " + (pozitii + 1) + "!");

                    int paramIndex = 0;
                    for (int i=0; i < uploadFields.size(); i++){
                        FieldMetaData<?> field = uploadFields.get(i);
                        if (field.isReserved() || field.isReadonly() || field.isSqlFunction() || !field.isInTable()) continue;

                        if (linieValori[i] != null && quotes.isPresent())
                            linieValori[i] = linieValori[i].replace(quotesUnescaped, "");

                        if (fieldCheck.isPresent() && fieldCheck.get().containsKey(field.getSqlName())){
                            FieldCheckResult rezultat = fieldCheck.get().get(field.getSqlName()).apply(linieValori[i]);
                            if (!rezultat.isPassed())
                                throw new Exception("Column's value '" +
                                        (field.getUploadName() == null || field.getUploadName().isEmpty() ? field.getSqlName() : field.getUploadName()) +
                                        " " + rezultat.getNotPassedInfo());
                        }

                        if (linieValori[i] == null || linieValori[i].length() == 0){
                            stmt.setNull(++paramIndex, field.getSqlTip());
                        } else {
                            stmt.setObject(++paramIndex, linieValori[i], field.getSqlTip());
                        }
                    }
                    
                    for (FieldMetaData<?> field : reservedFields){
                        if (field.isReadonly() || field.isSqlFunction() || !field.isInTable()) continue;
                        if (field.getDefaultValue() == null){
                            stmt.setNull(++paramIndex, field.getSqlTip());
                        } else {
                            stmt.setObject(++paramIndex, field.getDefaultValue(), field.getSqlTip());
                        }
                    }

                    stmt.addBatch();
                    if (++pozitii % this.batchSize == 0){
                        pozitiiProcesate += stmt.executeBatch().length;
                        System.out.println(pozitiiProcesate);
                    }
                    
                }

                //procesare rest batch
                pozitiiProcesate += stmt.executeBatch().length;
                conn.commit();
                
                //stabilire rezultat
                //***********************************
                return "Processed rows: " + pozitiiProcesate + ".\n" +
                        "Time: " + ((System.currentTimeMillis() - startTime)/1000) + " sec.";
            } catch (Exception ex){
                conn.rollback();
                throw new Exception(this.batchErrorMessage(ex, pozitiiProcesate, startTime));
            } finally{
                conn.setAutoCommit(true);
            }
        }
    }
    
    public String loadText(final String tableName, final BufferedReader reader, final String delimitator,
            final Optional<String> quotes, final Optional<Map<String, Function<Object, FieldCheckResult>>> fieldCheck,
            final Optional<Map<String, String>> fieldsNameMapping, final Optional<List<FieldMetaData<?>>> specificReservedFields,
            Optional<CallbackMethod> onStart, Optional<CallbackMethod> onComplete) throws Exception{
        
        if (onStart.isPresent()) onStart.get().run();
        String rezultat = loadText(tableName, reader, delimitator, quotes, fieldCheck, fieldsNameMapping, specificReservedFields);
        if (onComplete.isPresent()) onComplete.get().run();
        return rezultat;
    }
    
    private static Map<Integer, String> xlsxReadHeaderRow(final Row row) throws Exception{
        Map<Integer, String> rezultat = new HashMap<>();
        for (Cell cell : row) {
            int index = cell.getColumnIndex();
            switch (cell.getCellType()){
                case BLANK:
                    break;
                case STRING:
                    rezultat.put(index, cell.getStringCellValue());
                    break;
                case FORMULA:
                    switch (cell.getCachedFormulaResultType()){
                        case BLANK:
                            break;
                        case STRING:
                            rezultat.put(index, cell.getRichStringCellValue().getString());
                            break;
                        default:
                            throw new Exception(ERR_FILE_HEAD_COL_NOK.concat(": " + (index++)));
                    }
                    break;
                default:
                    throw new Exception(ERR_FILE_HEAD_COL_NOK.concat(": " + (index++)));
            }
        }
        if (rezultat.isEmpty()) throw new Exception(ERR_FILE_HEAD_COL_NOK);
        return rezultat;
    }
    
    private static Object[] xlsxReadValueRow(final Row row, Set<Integer> colIndexes) throws Exception{
        Map<Integer, Object> rezultat = new HashMap<>();
        colIndexes.forEach(x -> rezultat.put(x, null));
        
        for (Cell cell : row){
            int index = cell.getColumnIndex();
            if (!rezultat.containsKey(index)) throw new Exception("NO_COLUMN_HEADER_AT_INDEX: " + index);
            switch (cell.getCellType()){
                case BLANK:
                    break;
                case STRING:
                    {
                        String _val = cell.getStringCellValue();
                        if (_val.length() > 0) rezultat.put(index, _val);
                    }
                    break;
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)){
                        rezultat.put(index, cell.getDateCellValue());
                    } else {
                        double _val = cell.getNumericCellValue();
                        if ((_val % 1) == 0) {
                            rezultat.put(index, (long) _val);
                        } else {
                            rezultat.put(index, _val);
                        }
                    }
                    break;
                case BOOLEAN:
                    rezultat.put(index, cell.getBooleanCellValue());
                    break;
                case FORMULA:
                    switch (cell.getCachedFormulaResultType()){
                        case BLANK:
                            break;
                        case STRING:
                            {
                                String _val = cell.getRichStringCellValue().getString();
                                if (_val.length() > 0) rezultat.put(index, _val);
                            }
                            break;
                        case NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)){
                                rezultat.put(index, cell.getDateCellValue());
                            } else {
                                double _val = cell.getNumericCellValue();
                                if ((_val % 1) == 0) {
                                    rezultat.put(index, (long) _val);
                                } else {
                                    rezultat.put(index, _val);
                                }
                            }
                            break;
                        case BOOLEAN:
                            rezultat.put(index, cell.getBooleanCellValue());
                            break;
                        default:
                            throw new Exception("Wrong data type at column index " + (index++) + "!");
                    }
                    break;
                default:
                    throw new Exception("Wrong data type at column index " + (index++) + "!");                
            }
        }
        return rezultat.values().toArray();
    }
     
    @SuppressWarnings({"null", "ValueOfIncrementOrDecrementUsed"})
    public String loadExcel(final String tableName, final String filePath, final Optional<String> sheetName,
            Optional<Map<String, Function<Object, FieldCheckResult>>> fieldCheck, final Optional<Map<String, String>> fieldsNameMapping,
            final Optional<List<FieldMetaData<?>>> specificReservedFields) throws Exception{
        
        try(Connection conn = this.getConnection();
            Workbook workbook = StreamingReader.builder()
                    .sstCacheSize(this.batchSize)
                    .rowCacheSize(this.batchSize)
                    .bufferSize(8192)
                    .open(new File(filePath));){
            
            conn.setAutoCommit(false);
            final long startTime = System.currentTimeMillis();
                        
            //obtinere sheet 
            Sheet sheet;
            if (sheetName.isPresent() && !(sheetName.get() == null || sheetName.get().isEmpty())) {
                sheet = workbook.getSheet(sheetName.get());
            } else {
                sheet = workbook.getSheetAt(0);
            }
                     
            List<FieldMetaData<?>> reservedFields = null;
            List<FieldMetaData<?>> uploadFields = null;
            PreparedStatement stmt = null;
            int pozitiiProcesate = 0;
            Set<Integer> colIndexes = null;
            
            try{
                int pozitii = -1;
                for (Row row : sheet){
                    if (pozitii == -1){
                        //obtinere cap de tabel din text
                        //***********************************
                        Map<Integer, String> primaLinie = xlsxReadHeaderRow(row);
                        colIndexes = primaLinie.keySet();
                        //obtinere meta data tabela
                        //***********************************
                        String meta_sql = "select * from " + tableName + " where 1=0;";
                        ResultSetMetaData tableMeta = conn.createStatement().executeQuery(meta_sql).getMetaData();                    
                        reservedFields = DbConn.getReservedFields(tableMeta, Optional.ofNullable(this.defaultReservedFields), specificReservedFields);
                        uploadFields = DbConn.getUploadFields(tableMeta, primaLinie.values().toArray(new String[colIndexes.size()]), fieldsNameMapping, reservedFields);

                        //stabilire statement sql pentru insert
                        //***********************************            
                        String stmt_sql = "insert into " + tableName + " (";
                        stmt_sql += FieldMetaData.getSqlInsertStatementColumnText(uploadFields.stream().filter(x -> !x.isReserved()));
                        if (!reservedFields.isEmpty()) stmt_sql += "," + FieldMetaData.getSqlInsertStatementColumnText(reservedFields.stream());
                        stmt_sql += ") values (";
                        stmt_sql += FieldMetaData.getSqlInsertStatementParameterText(uploadFields.stream().filter(x -> !x.isReserved()));
                        if (!reservedFields.isEmpty()) stmt_sql += "," + FieldMetaData.getSqlInsertStatementParameterText(reservedFields.stream());
                        stmt_sql += ");";

                        //creare prepared statement
                        //*********************************** 
                        stmt = conn.prepareStatement(stmt_sql);
                        
                        pozitii++;
                    } else {
                        Object[] linieValori = xlsxReadValueRow(row, colIndexes);
                        if (linieValori.length != uploadFields.size())
                            throw new Exception("Value count is not equal to columns count for row: " + (pozitii + 1) + "!");

                        int paramIndex = 0;
                        for (int i=0; i < uploadFields.size(); i++){
                            FieldMetaData<?> field = uploadFields.get(i);
                            if (field.isReserved() || field.isReadonly() || field.isSqlFunction() || !field.isInTable()) continue;

                            if (fieldCheck.isPresent() && fieldCheck.get().containsKey(field.getSqlName())){
                                FieldCheckResult rezultat = fieldCheck.get().get(field.getSqlName()).apply(linieValori[i]);
                                if (!rezultat.isPassed())
                                    throw new Exception("Column's value '" +
                                            (field.getUploadName() == null || field.getUploadName().isEmpty() ? field.getSqlName() : field.getUploadName()) +
                                            " " + rezultat.getNotPassedInfo());
                            }

                            if (linieValori[i] == null || (linieValori[i] instanceof String && ((CharSequence) linieValori[i]).length() == 0)){
                                stmt.setNull(++paramIndex, field.getSqlTip());
                            } else {
                                stmt.setObject(++paramIndex, linieValori[i], field.getSqlTip());
                            }
                        }

                        for (FieldMetaData<?> field : reservedFields){
                            if (field.isReadonly() || field.isSqlFunction() || !field.isInTable()) continue;
                            if (field.getDefaultValue() == null){
                                stmt.setNull(++paramIndex, field.getSqlTip());
                            } else {
                                stmt.setObject(++paramIndex, field.getDefaultValue(), field.getSqlTip());
                            }
                        }

                        stmt.addBatch();
                        if (++pozitii % this.batchSize == 0){
                            pozitiiProcesate += stmt.executeBatch().length;
                            System.out.println(pozitiiProcesate);
                        }
                    }
                }              

                //procesare rest batch
                pozitiiProcesate += stmt.executeBatch().length;
                conn.commit();
                stmt.close();
                
                //stabilire rezultat
                //***********************************
                return "Processed rows: " + pozitiiProcesate + ".\n" +
                        "Time: " + ((System.currentTimeMillis() - startTime)/1000) + " sec.";                
            } catch (Exception ex){
                if (stmt != null) stmt.close();
                conn.rollback();
                throw new Exception(this.batchErrorMessage(ex, pozitiiProcesate, startTime));
            } finally{
                conn.setAutoCommit(true);
            }
        }
    }
    
    public String loadExcel(final String tableName, final String filePath, final Optional<String> sheetName,
            Optional<Map<String, Function<Object, FieldCheckResult>>> fieldCheck, final Optional<Map<String, String>> fieldsNameMapping,
            final Optional<List<FieldMetaData<?>>> specificReservedFields,
            Optional<CallbackMethod> onStart, Optional<CallbackMethod> onComplete) throws Exception{
        
        if (onStart.isPresent()) onStart.get().run();
        String rezultat = loadExcel(tableName, filePath, sheetName, fieldCheck, fieldsNameMapping, specificReservedFields);
        if (onComplete.isPresent()) onComplete.get().run();
        return rezultat;
    }
  
    public String executePreparedStatement(String sql, Optional<ParamSql[]> inputParams, Connection conn) throws Exception{        
        conn.setAutoCommit(false);
        try(PreparedStatement stmt = conn.prepareStatement(sql);) {
        	//initializare variabile
            long startTime = System.currentTimeMillis();
        
            //procesare inputParams sql
            if (inputParams.isPresent() && inputParams.get().length>0){
                for(int i=0; i < inputParams.get().length; i++){
                    stmt.setObject(i + 1, inputParams.get()[i].getValoare(), inputParams.get()[i].getTip());
                }
            }
            
            //executare interogare sql
            boolean isResultSet = stmt.execute();
            if (isResultSet) throw new Exception(ERR_GOT_RESULT_SET);
            
            int affectedRecords = stmt.getUpdateCount();
            stmt.getMoreResults(); //verificare daca sunt erori sql
            
            conn.commit();            

          //stabilire rezultat
            String rezultat;
            if (affectedRecords > -1) {
                rezultat = "Pozitii afectate: " + affectedRecords + ".\n";
            } else {
                rezultat = "Interogare efectuata!\n";
            }
            rezultat += "Timp: " + ((System.currentTimeMillis() - startTime)/1000) + " sec.";
            return rezultat;
            
        } catch (Exception ex) {
        	conn.rollback();
        	throw ex;
        } finally {
        	conn.setAutoCommit(true);
        }
    }
	
    public String executePreparedStatement(String sql, Optional<ParamSql[]> inputParams) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.executePreparedStatement(sql, inputParams, conn);
        }
    }
	
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    public String executePreparedStatement(String sql, List<Optional<ParamSql[]>> paramsList, Connection conn) throws Exception{
        //initializare variabile
        long startTime = System.currentTimeMillis();            
        //procesare batch
        int pozitiiProcesate = 0;
        
        conn.setAutoCommit(false);
        try(PreparedStatement stmt = conn.prepareStatement(sql);){               
      
            int pozitii = 0;
            for(Optional<ParamSql[]> inputParams: paramsList){
                //procesare inputParams sql
                if (inputParams.isPresent() && inputParams.get().length>0){
                    for(int i=0; i < inputParams.get().length; i++){
                        stmt.setObject(i + 1, inputParams.get()[i].getValoare(), inputParams.get()[i].getTip());
                    }
                }

                stmt.addBatch();
                if (++pozitii % this.batchSize == 0)
                    pozitiiProcesate += stmt.executeBatch().length;
            }

            //procesare rest batch
            pozitiiProcesate += stmt.executeBatch().length;
            conn.commit();

            //stabilire rezultat
            return "Pozitii procesate: " + pozitiiProcesate + ".\n" +
                    "Timp: " + ((System.currentTimeMillis() - startTime)/1000) + " sec.";
        }catch (Exception ex){
            conn.rollback();
            throw new Exception(this.batchErrorMessage(ex, pozitiiProcesate, startTime));
        } finally {
            conn.setAutoCommit(true);
        }
    }
	
    public String executePreparedStatement(String sql, List<Optional<ParamSql[]>> paramsList) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.executePreparedStatement(sql, paramsList, conn);
        }
    }
	
    public String executeCallableStatement(String sql, Optional<ParamSql[]> inputParams, Connection conn) throws Exception{        
        conn.setAutoCommit(false);
        try(CallableStatement stmt = conn.prepareCall(sql);) {
            
            //initializare variabile
            long startTime = System.currentTimeMillis();
            
            //procesare inputParams interogare sql
            if (inputParams.isPresent() && inputParams.get().length>0){
                for(int i=0; i < inputParams.get().length; i++){
                    stmt.setObject(i + 1, inputParams.get()[i].getValoare(), inputParams.get()[i].getTip());
                }
            }
            
            //executare interogare sql
            boolean isResultSet = stmt.execute();
            if (isResultSet) throw new Exception(ERR_GOT_RESULT_SET);
            
            int affectedRecords = stmt.getUpdateCount();
            stmt.getMoreResults(); //verificare daca exista erori sql
            
            conn.commit();
            
            //stabilire rezultat
            String rezultat;
            if (affectedRecords > -1) {
                rezultat = "Pozitii afectate: " + affectedRecords + ".\n";
            } else {
                rezultat = "Interogare efectuata!\n";
            }
            rezultat += "Timp: " + ((System.currentTimeMillis() - startTime)/1000) + " sec.";
            return rezultat;
        } catch (Exception ex) {
        	conn.rollback();
        	throw ex;
        } finally {
        	conn.setAutoCommit(true);
        }
    }
	
    public String executeCallableStatement(String sql, Optional<ParamSql[]> inputParams) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.executeCallableStatement(sql, inputParams, conn);
        }
    }
	
    public String executeCallableStatement(String sql, Map<String, ParamSql> inputParams, Connection conn) throws Exception{        
        conn.setAutoCommit(false);
        try(CallableStatement stmt = conn.prepareCall(sql);){
            
            //initializare variabile
            long startTime = System.currentTimeMillis();
            
            //procesare inputParams interogare sql
            if (inputParams != null && !inputParams.isEmpty()){
                for (Entry<String, ParamSql> entry: inputParams.entrySet()){
                    stmt.setObject(entry.getKey(), entry.getValue().getValoare(), entry.getValue().getTip());
                }
            }
            
            //executare interogare sql
            boolean isResultSet = stmt.execute();
            if (isResultSet) throw new Exception(ERR_GOT_RESULT_SET);
            
            int affectedRecords = stmt.getUpdateCount();
            stmt.getMoreResults(); //verificare daca exista erori sql
            
            conn.commit();
            
            //stabilire rezultat
            String rezultat;
            if (affectedRecords > -1) {
                rezultat = "Pozitii afectate: " + affectedRecords + ".\n";
            } else {
                rezultat = "Interogare efectuata!\n";
            }
            rezultat += "Timp: " + ((System.currentTimeMillis() - startTime)/1000) + " sec.";
            return rezultat;
        } catch (Exception ex) {
        	conn.rollback();
        	throw ex;
        } finally {
        	conn.setAutoCommit(true);
        }
    }
	
    public String executeCallableStatement(String sql, Map<String, ParamSql> inputParams) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.executeCallableStatement(sql, inputParams, conn);
        }
    }
	
    public List<Map<String, Object>> getFromPreparedStatement(String sql, Optional<ParamSql[]> inputParams, Connection conn) throws Exception{        
        try(PreparedStatement stmt = conn.prepareStatement(sql);) {
            
            //procesare inputParams sql
            if (inputParams.isPresent() && inputParams.get().length>0){
                for(int i=0; i < inputParams.get().length; i++){
                    stmt.setObject(i + 1, inputParams.get()[i].getValoare(), inputParams.get()[i].getTip());
                }
            }
            //executare interogare si verificare daca exista erori sql
            boolean isResultSet = stmt.execute();
            if (!isResultSet){
                stmt.getMoreResults(); //poate arunca eroare din sql
                throw new Exception(ERR_NOT_RESULT_SET);
            }
            
            try(ResultSet rs = stmt.getResultSet();){
                //obtinere inregistrari

                ResultSetMetaData rs_meta = rs.getMetaData();

                //adaugare inregistrari la rezultat
                List<Map<String, Object>> rezultat = new ArrayList<>();
                while (rs.next()){
                    Map<String, Object> inreg = new HashMap<>();
                    for (int i=1; i<=rs_meta.getColumnCount(); i++){
                        inreg.put(rs_meta.getColumnName(i), rs.getObject(i));
                    }
                    rezultat.add(inreg);
                }
                return rezultat;
            }
        }
    }
	
    public List<Map<String, Object>> getFromPreparedStatement(String sql, Optional<ParamSql[]> inputParams) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.getFromPreparedStatement(sql, inputParams, conn);
        }
    }
	
    public List<Map<String, Object>> getFromCallableStatement (String sql, Optional<ParamSql[]> inputParams, Connection conn) throws Exception{
        try(CallableStatement stmt = conn.prepareCall(sql);) {
            
            //procesare inputParams sql
            if (inputParams.isPresent() && inputParams.get().length>0){
                for(int i=0; i < inputParams.get().length; i++){
                    stmt.setObject(i + 1, inputParams.get()[i].getValoare(), inputParams.get()[i].getTip());
                }
            }
            
            //executare interogare si verificare daca exista erori sql
            boolean isResultSet = stmt.execute();
            if (!isResultSet){
                stmt.getMoreResults(); //poate arunca eroare din sql
                throw new Exception(ERR_NOT_RESULT_SET);
            }
            
            //obtinere inregistrari
            try(ResultSet rs = stmt.getResultSet();){
                ResultSetMetaData rs_meta = rs.getMetaData();

                //adaugare inregistrari la rezultat
                List<Map<String, Object>> rezultat = new ArrayList<>();
                while (rs.next()){
                    Map<String, Object> inreg = new HashMap<>();
                    for (int i=1; i<=rs_meta.getColumnCount(); i++){
                        inreg.put(rs_meta.getColumnName(i), rs.getObject(i));
                    }
                    rezultat.add(inreg);
                }
                return rezultat;
            }
        }
    }
	
    public List<Map<String, Object>> getFromCallableStatement (String sql, Optional<ParamSql[]> inputParams) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.getFromCallableStatement(sql, inputParams, conn);
        }
    }
	
    public List<Map<String, Object>> getFromCallableStatement(String sql, Map<String, ParamSql> inputParams, Connection conn) throws Exception{
        try(CallableStatement stmt = conn.prepareCall(sql);) {

            //procesare inputParams sql
            if (inputParams != null && !inputParams.isEmpty()){
                for (Entry<String, ParamSql> entry: inputParams.entrySet()){
                    stmt.setObject(entry.getKey(), entry.getValue().getValoare(), entry.getValue().getTip());
                }
            }
            
            //executare interogare si verificare daca exista erori sql
            boolean isResultSet = stmt.execute();
            if (!isResultSet){
                stmt.getMoreResults(); //poate arunca eroare din sql
                throw new Exception(ERR_NOT_RESULT_SET);
            }
            
            //obtinere inregistrari
            try(ResultSet rs = stmt.getResultSet();){
                ResultSetMetaData rs_meta = rs.getMetaData();

                //adaugare inregistrari la rezultat
                List<Map<String, Object>> rezultat = new ArrayList<>();
                while (rs.next()){
                    Map<String, Object> inreg = new HashMap<>();
                    for (int i=1; i<=rs_meta.getColumnCount(); i++){
                        inreg.put(rs_meta.getColumnName(i), rs.getObject(i));
                    }
                    rezultat.add(inreg);
                }
                return rezultat;
            }            
        }
    }
	
    public List<Map<String, Object>> getFromCallableStatement(String sql, Map<String, ParamSql> inputParams) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.getFromCallableStatement(sql, inputParams, conn);
        }
    }
	
    public Object[] getFromCallableStatement(String sql, Optional<ParamSql[]> inputParams, ParamSql[] outputParams, Connection conn) throws Exception{
        try(CallableStatement stmt = conn.prepareCall(sql);){
            
            int paramIndex = 0;
            //procesare inputParams sql
            if (inputParams.isPresent() && inputParams.get().length>0){
                for(int i=0; i < inputParams.get().length; i++){
                    paramIndex += 1;
                    stmt.setObject(paramIndex, inputParams.get()[i], inputParams.get()[i].getTip());
                }
            }
            
            //inregistrare outputParams
            for (int i = 0; i < outputParams.length; i++){
                paramIndex += 1;
                stmt.registerOutParameter(paramIndex, outputParams[i].getTip());
            }
            
            
            //executare interogare si verificare daca exista erori sql
            boolean isResultSet = stmt.execute();
            if (!isResultSet){
                stmt.getMoreResults(); //poate arunca eroare din sql
                throw new Exception(ERR_GOT_RESULT_SET);
            }
            
            //extragere rezultat
            Object[] rezultat = new Object[outputParams.length - 1];
            for (int i=0; i<rezultat.length; i++){
                rezultat[i] = stmt.getObject(
                        paramIndex - rezultat.length + i + 1
                    );
            }
            return rezultat;
        }
    }
	
    public Object[] getFromCallableStatement(String sql, Optional<ParamSql[]> inputParams, ParamSql[] outputParams) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.getFromCallableStatement(sql, inputParams, outputParams, conn);
        }
    }
	
    public Map<String, Object> getFromCallableStatement (String sql, Map<String, ParamSql> inputParams, Map<String, ParamSql> outputParams, Connection conn) throws Exception{
        try(CallableStatement stmt = conn.prepareCall(sql);){

            //procesare parametri intrare sql
            if (inputParams != null && !inputParams.isEmpty()){
                for (Entry<String, ParamSql> entry: inputParams.entrySet()){
                    stmt.setObject(entry.getKey(), entry.getValue().getValoare(), entry.getValue().getTip());
                }
            }
            
            //inregistrare parametri iesire sql
            for (Entry<String, ParamSql> entry: outputParams.entrySet()){
                stmt.registerOutParameter(entry.getKey(), entry.getValue().getTip());
            }
            
            //executare interogare si verificare daca exista erori sql
            boolean isResultSet = stmt.execute();
            if (!isResultSet){
                stmt.getMoreResults(); //poate arunca eroare din sql
                throw new UnsupportedOperationException(ERR_GOT_RESULT_SET);
            }
            
            //extragere rezultat
            Map<String, Object> rezultat = new HashMap<>();
            for (Entry<String, ParamSql> entry: outputParams.entrySet()){
                rezultat.put(entry.getKey(), stmt.getObject(entry.getKey()));
            }
            return rezultat;
        }
    }
	
    public Map<String, Object> getFromCallableStatement (String sql, Map<String, ParamSql> inputParams, Map<String, ParamSql> outputParams) throws Exception{
        try(Connection conn = this.getConnection();) {
            return this.getFromCallableStatement(sql, inputParams, outputParams, conn);
        }
    }
	
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    public void downloadFromPreparedStmtToCSV (String sql, Optional<ParamSql[]> inputParams, BufferedWriter writer) throws Exception{        
        final String DELIMITATOR=",";
        final char CSV_BOM = '\uFEFF'; //caracter pentru ca Excel sa recunoasca UTF-8

        try(Connection conn = this.getConnection();){
            conn.setAutoCommit(false);
            try(PreparedStatement stmt = conn.prepareStatement(sql);){
                stmt.setFetchSize(this.batchSize * 3);

                //procesare inputParams sql
                if (inputParams.isPresent() && inputParams.get().length>0){
                    for(int i=0; i < inputParams.get().length; i++){
                        stmt.setObject(i + 1, inputParams.get()[i].getValoare(), inputParams.get()[i].getTip());
                    }
                }

                //executare interogare si verificare daca exista erori sql
                boolean isResultSet = stmt.execute();
                if (!isResultSet){
                    stmt.getMoreResults(); //poate arunca eroare din sql
                    throw new Exception(ERR_NOT_RESULT_SET);
                }

                //obtinere inregistrari
                try(ResultSet rs = stmt.getResultSet();){
                    ResultSetMetaData rs_meta = rs.getMetaData();

                    //start scrire csv
                    writer.write(CSV_BOM);

                    //scriere cap tabel
                    String header = "";
                    for (int i=1; i<=rs_meta.getColumnCount(); i++){
                        if (i > 1) header += DELIMITATOR;
                        String numeColoana = rs_meta.getColumnName(i);
                        header += (numeColoana.equals("ID")? numeColoana.toLowerCase() : numeColoana);
                    }
                    writer.write(header);
                    writer.flush();

                    //scriere inregistrari
                    int poz=0;
                    while(rs.next()){
                        String linie = "";
                        for (int i=1; i<=rs_meta.getColumnCount(); i++){
                            if (i > 1) linie += DELIMITATOR;
                            String valoare = rs.getString(i);

                            if (valoare == null){
                                linie += "";
                            } else {
                                boolean needsQuotes = valoare.contains(DELIMITATOR);
                                if (needsQuotes) linie += "\"";
                                linie += valoare;
                                if (needsQuotes) linie += "\"";
                            }
                        }
                        writer.newLine();
                        writer.write(linie);
                        if (++poz % (this.batchSize) == 0) writer.flush();
                    }
                    writer.flush();
                }
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }
	
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    public void downloadFromPreparedStmtToJsonArray(String sql, Optional<ParamSql[]> inputParams, OutputStream writer) throws Exception{        
        try(Connection conn = this.getConnection();){
            conn.setAutoCommit(false);
            try(PreparedStatement stmt = conn.prepareStatement(sql);){
                stmt.setFetchSize(this.batchSize * 3);

                //procesare inputParams sql
                if (inputParams.isPresent() && inputParams.get().length>0){
                    for(int i=0; i < inputParams.get().length; i++){
                        stmt.setObject(i + 1, inputParams.get()[i].getValoare(), inputParams.get()[i].getTip());
                    }
                }

                //executare interogare si verificare daca exista erori sql
                boolean isResultSet = stmt.execute();
                if (!isResultSet){
                    stmt.getMoreResults(); //poate arunca eroare din sql
                    throw new Exception(ERR_NOT_RESULT_SET);
                }

                //obtinere inregistrari
                try(ResultSet rs = stmt.getResultSet();
                    JsonGenerator generator = Json.createGenerator(writer);){
                    ResultSetMetaData rs_meta = rs.getMetaData();

                    //scriere inregistrari
                    int poz = 0;
                    generator.writeStartArray();

                    while(rs.next()){
                        generator.writeStartObject();
                        for (int i=1; i<=rs_meta.getColumnCount(); i++){                    
                            if (rs.getObject(i) == null){
                                generator.writeNull(rs_meta.getColumnName(i));
                            } else {
                                switch (rs_meta.getColumnType(i)){
                                    case Types.DECIMAL:
                                    case Types.NUMERIC:
                                        generator.write(rs_meta.getColumnName(i), rs.getBigDecimal(i));
                                        break;                                
                                    case Types.FLOAT:
                                    case Types.DOUBLE:
                                    case Types.REAL:
                                        generator.write(rs_meta.getColumnName(i), rs.getDouble(i));
                                        break;                                
                                    case Types.BIGINT:
                                        generator.write(rs_meta.getColumnName(i), rs.getLong(i));
                                        break;
                                    case Types.TINYINT:
                                    case Types.SMALLINT:
                                    case Types.INTEGER:
                                        generator.write(rs_meta.getColumnName(i), rs.getInt(i));
                                        break;                                
                                    case Types.BIT:
                                    case Types.BOOLEAN:
                                        generator.write(rs_meta.getColumnName(i), rs.getBoolean(i));
                                        break;
                                    case Types.ARRAY:
                                    case Types.BINARY:
                                    case Types.VARBINARY:
                                    case Types.LONGVARBINARY:
                                    case Types.BLOB:
                                    case Types.CLOB:
                                    case Types.NCLOB:
                                        generator.write(rs_meta.getColumnName(i), "not supported");
                                        break;
                                    default:
                                        generator.write(rs_meta.getColumnName(i), rs.getString(i));
                                        break;
                                }
                            }
                        }
                        generator.writeEnd();
                        if (++poz % this.batchSize == 0) generator.flush();
                    }
                    generator.writeEnd();
                    generator.flush();
                }          
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }
    
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    public void downloadFromPreparedStmtToXLSX(String sql, Optional<ParamSql[]> inputParams, OutputStream writer) throws Exception{
        try(Connection conn = this.getConnection();){
            conn.setAutoCommit(false);
            try(PreparedStatement stmt = conn.prepareStatement(sql);){
                stmt.setFetchSize(this.batchSize * 3);

                //procesare inputParams sql
                if (inputParams.isPresent() && inputParams.get().length>0){
                    for(int i=0; i < inputParams.get().length; i++){
                        stmt.setObject(i + 1, inputParams.get()[i].getValoare(), inputParams.get()[i].getTip());
                    }
                }

                //executare interogare si verificare daca exista erori sql
                boolean isResultSet = stmt.execute();
                if (!isResultSet){
                    stmt.getMoreResults(); //poate arunca eroare din sql
                    throw new Exception(ERR_NOT_RESULT_SET);
                }

                //obtinere inregistrari
                try(ResultSet rs = stmt.getResultSet();
                    SXSSFWorkbook wb = new SXSSFWorkbook(-1);){ // turn off auto-flushing and accumulate all rows in memory
                    
                    //cell formating for date
                    CellStyle dateStyle = wb.createCellStyle();
                    dateStyle.setDataFormat(wb.getCreationHelper().createDataFormat().getFormat(BuiltinFormats.getBuiltinFormat(14)));
                    
                    ResultSetMetaData rs_meta = rs.getMetaData();

                    //start scriere XLSX
                    wb.setCompressTempFiles(true); // temp files will be gzipped
                    SXSSFSheet sh = wb.createSheet();

                    //scriere cap de tabel
                    SXSSFRow headRow = sh.createRow(0);
                    for (int i=1; i<=rs_meta.getColumnCount(); i++){
                        SXSSFCell cell = headRow.createCell(i-1);
                        cell.setCellValue(rs_meta.getColumnName(i));
                    }
                    sh.flushRows();

                    //scriere inregistrari
                    int poz=0;
                    while(rs.next()){
                        SXSSFRow row = sh.createRow(++poz);
                        for (int i=1; i<=rs_meta.getColumnCount(); i++){
                            SXSSFCell cell = row.createCell(i-1);
                            switch(rs_meta.getColumnType(i)){
                                case Types.DECIMAL:
                                case Types.NUMERIC:
                                    {
                                        BigDecimal _val = rs.getBigDecimal(i);
                                        if (_val == null) {
                                            cell.setBlank();
                                        } else {
                                            cell.setCellValue(_val.doubleValue());
                                        }
                                    }
                                    break;
                                case Types.FLOAT:
                                case Types.DOUBLE:
                                case Types.REAL:
                                    cell.setCellValue(rs.getDouble(i));
                                    break;
                                case Types.DATE:
                                    {
                                        cell.setCellValue(rs.getDate(i));
                                        cell.setCellStyle(dateStyle);
                                    }
                                    break;
                                case Types.BIT:
                                case Types.BOOLEAN:
                                    cell.setCellValue(rs.getBoolean(i));
                                    break;
                                default:
                                    cell.setCellValue(rs.getString(i));
                                    break;                        
                            }
                        }
                        if (poz % this.batchSize == 0) sh.flushRows();
                    }
                    sh.flushRows();

                    wb.write(writer);
                    wb.dispose();
                    writer.flush();
                }
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }
}
