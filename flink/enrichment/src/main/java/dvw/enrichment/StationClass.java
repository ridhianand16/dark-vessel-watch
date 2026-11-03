package dvw.enrichment;

/**
 * Station class of an MMSI, from its leading digits (ITU-R M.585).
 *
 * <p>A port of stream/faust_app/mmsi.py with the same wire names, so the tracker can read
 * {@code station_class} from ais.enriched instead of recomputing it. The table is checked
 * longest prefix first, so 00 wins over 0. An MMSI below 10^8 has leading zeros: 2190261
 * is 002190261, a coast station.
 */
public enum StationClass {
    SHIP("ship"),
    CRAFT_WITH_PARENT("craft_with_parent"),
    COAST("coast"),
    GROUP("group"),
    SAR_AIRCRAFT("sar_aircraft"),
    HANDHELD("handheld"),
    AID_TO_NAVIGATION("aid_to_navigation"),
    SART("sart"),
    MOB("mob"),
    EPIRB("epirb"),
    OTHER("other");

    public static final long MMSI_MAX = 999_999_999L;

    private static final String[] PREFIXES = {"970", "972", "974", "111", "98", "99", "00", "0", "8"};
    private static final StationClass[] CLASSES = {
        SART, MOB, EPIRB, SAR_AIRCRAFT, CRAFT_WITH_PARENT, AID_TO_NAVIGATION, COAST, GROUP, HANDHELD
    };

    public final String wire;

    StationClass(String wire) {
        this.wire = wire;
    }

    public static StationClass of(long mmsi) {
        if (mmsi < 0 || mmsi > MMSI_MAX) {
            return OTHER;
        }
        String digits = String.format("%09d", mmsi);
        for (int i = 0; i < PREFIXES.length; i++) {
            if (digits.startsWith(PREFIXES[i])) {
                return CLASSES[i];
            }
        }
        char first = digits.charAt(0);
        return first >= '2' && first <= '7' ? SHIP : OTHER;
    }
}
