package org.myorg.quickstart.calvid;

import java.sql.Timestamp;

public class VidPojo {

    private String env;
    private String colo;
    private String pool;
    private String machine;
    private String buildLabel;
    private String xslLabel;
    private Timestamp timestamp;
    private String type;
    private Character classStr;
    private String typeName;
    private String status;
    private int duration;
    private int count;

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getColo() {
        return colo;
    }

    public void setColo(String colo) {
        this.colo = colo;
    }

    public String getPool() {
        return pool;
    }

    public void setPool(String pool) {
        this.pool = pool;
    }

    public String getMachine() {
        return machine;
    }

    public void setMachine(String machine) {
        this.machine = machine;
    }

    public String getBuildLabel() {
        return buildLabel;
    }

    public void setBuildLabel(String buildLabel) {
        this.buildLabel = buildLabel;
    }

    public String getXslLabel() {
        return xslLabel;
    }

    public void setXslLabel(String xslLabel) {
        this.xslLabel = xslLabel;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Character getClassStr() {
        return classStr;
    }

    public void setClassStr(Character classStr) {
        this.classStr = classStr;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
