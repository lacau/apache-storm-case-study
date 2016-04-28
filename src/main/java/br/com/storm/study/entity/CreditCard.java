package br.com.storm.study.entity;

import java.io.Serializable;

/**
 * Created by lacau on 28/04/16.
 */
public class CreditCard implements Serializable {

    private static final long serialVersionUID = 5381943031652064821L;

    private String bin;

    private boolean sold;

    private boolean persisted;

    public String getBin() {
        return bin;
    }

    public void setBin(String bin) {
        this.bin = bin;
    }

    public boolean isSold() {
        return sold;
    }

    public void setSold(boolean sold) {
        this.sold = sold;
    }

    public boolean isPersisted() {
        return persisted;
    }

    public void setPersisted(boolean persisted) {
        this.persisted = persisted;
    }
}