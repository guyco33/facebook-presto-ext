import com.facebook.presto.ext.functions.ExtJsonFunctions;

public class test2 {

    public static void main(String[] args) {
        String str = "{\n" +
                "  \"pricing\": {\n" +
                "    \"customer\": {\n" +
                "      \"price_models\": [56,\n" +
                "        {\n" +
                "          \"components\": [\n" +
                "            {\n" +
                "              \"markups\": null,\n" +
                "              \"apply_surge\": false,\n" +
                "              \"name\": \"driving\"\n" +
                "            },\n" +
                "            {\n" +
                "              \"name\": \"tolls\",\n" +
                "              \"apply_surge\": false,\n" +
                "              \"markups\": null,\n" +
                "              \"use_destination_for_fixed_price\": true,\n" +
                "              \"timeslots\": null\n" +
                "            },\n" +
                "            {\n" +
                "              \"name\": \"surge\",\n" +
                "              \"apply_surge\": false,\n" +
                "              \"applies_to\": null,\n" +
                "              \"markups\": null\n" +
                "            },\n" +
                "            {\n" +
                "              \"customer_tip\": null,\n" +
                "              \"name\": \"tips\",\n" +
                "              \"apply_surge\": false,\n" +
                "              \"enabled\": false,\n" +
                "              \"applies_to\": null,\n" +
                "              \"markups\": null,\n" +
                "              \"supplier_share\": null,\n" +
                "              \"applies_to_extras\": null\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        ExtJsonFunctions.getJsonKeys(str,true).forEach(System.out::println);



    }
}
