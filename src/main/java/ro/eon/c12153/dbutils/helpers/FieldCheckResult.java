package ro.any.c12153.dbutils.helpers;

/**
 *
 * @author C12153
 */
public class FieldCheckResult {
    
    private boolean passed;
    private String notPassedInfo;

    public FieldCheckResult() {
    }

    public FieldCheckResult(boolean passed, String checkInfo) {
        this.passed = passed;
        this.notPassedInfo = checkInfo;
    }

    public boolean isPassed() {
        return passed;
    }

    public void setPassed(boolean passed) {
        this.passed = passed;
    }

    public String getNotPassedInfo() {
        return notPassedInfo;
    }

    public void setNotPassedInfo(String notPassedInfo) {
        this.notPassedInfo = notPassedInfo;
    }
}
