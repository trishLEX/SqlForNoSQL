package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

public class OrderableSelectField extends Column {
    private AscDesc ascDesc;

    public OrderableSelectField(String userInput) {
        super(userInput);
        this.ascDesc = AscDesc.ASC;
    }

    public OrderableSelectField(String userInput, AscDesc ascDesc) {
        super(userInput);
        this.ascDesc = ascDesc;
    }

    public void setAscDesc(AscDesc ascDesc) {
        this.ascDesc = ascDesc;
    }

    public AscDesc getAscDesc() {
        return ascDesc;
    }

    @Override
    public String toString() {
        return userInputName + " " + ascDesc;
    }

    @Override
    public String getQuotedFullQualifiedContent() {
        return super.getQuotedFullQualifiedContent() + " " + ascDesc;
    }
}
