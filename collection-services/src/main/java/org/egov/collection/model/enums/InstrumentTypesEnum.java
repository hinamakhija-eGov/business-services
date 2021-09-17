package org.egov.collection.model.enums;

public enum InstrumentTypesEnum {
    CASH, CHEQUE, DD, ONLINE, CARD,
    OFFLINE_NEFT,
    OFFLINE_RTGS,
    POS;

    public static boolean contains(String test) {
        for (InstrumentTypesEnum val : InstrumentTypesEnum.values()) {
            if (val.name().equalsIgnoreCase(test)) {
                return true;
            }
        }
        return false;
    }
}
