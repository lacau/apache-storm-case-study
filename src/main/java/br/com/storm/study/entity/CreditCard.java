package br.com.storm.study.entity;

import java.io.Serializable;

/**
 * Created by lacau on 28/04/16.
 */
public class CreditCard implements Serializable {

    private static final long serialVersionUID = 5381943031652064821L;

    private Long id;

    private String bin;

    private boolean sold;

    private boolean persisted;

    public CreditCard(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CreditCard{");
        sb.append("id=").append(id);
        sb.append(", bin='").append(bin).append('\'');
        sb.append(", sold=").append(sold);
        sb.append(", persisted=").append(persisted);
        sb.append('}');
        return sb.toString();
    }
}