package de.uni_mannheim.minie.annotation.factuality;

/**
 * Class containing polarity and modality
 *
 * @author Kiril Gashteovski
 */
public class Factuality {
    private Polarity polarity;
    private Modality modality;

    public Factuality() {
        this.polarity = new Polarity();
        this.modality = new Modality();
    }

    public Factuality(Polarity p, Modality m) {
        this.polarity = p;
        this.modality = m;
    }

    public void setPolarity(Polarity p) {
        this.polarity = p;
    }

    public void setModality(Modality m) {
        this.modality = m;
    }

    public Polarity getPolarity() {
        return this.polarity;
    }

    public Polarity.Type getPolarityType() {
        return this.polarity.getType();
    }

    public Modality getModality() {
        return this.modality;
    }

    public Modality.Type getModalityType() {
        return this.modality.getModalityType();
    }

    public void clear() {
        this.polarity = new Polarity();
        this.modality = new Modality();
    }
}
