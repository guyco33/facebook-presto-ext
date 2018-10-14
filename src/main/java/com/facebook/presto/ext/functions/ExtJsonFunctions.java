package com.facebook.presto.ext.functions;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.sun.org.apache.xpath.internal.operations.Bool;
import com.sun.tools.internal.ws.wsdl.document.jaxws.Exception;
import io.airlift.slice.Slice;

import javax.json.Json;
import javax.json.stream.JsonParser;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;


public class ExtJsonFunctions {

    private ExtJsonFunctions() {}

    @ScalarFunction("get_json_keys")
    @Description("get qualified json keys in all levels")
    @SqlType("array(varchar)")
    @SqlNullable
    public static Block getJsonKeys(
            @SqlType(StandardTypes.VARCHAR) Slice str)
    {
        return stringArrayBlock(getJsonKeys(str.toStringUtf8(),true));
    }

    @ScalarFunction("get_json_keys")
    @Description("get json keys in all level (false for unqualified keys)")
    @SqlType("array(varchar)")
    @SqlNullable
    public static Block getJsonKeys(
            @SqlType(StandardTypes.VARCHAR) Slice str,
            @SqlType(StandardTypes.BOOLEAN) boolean qualified)
    {
        return stringArrayBlock(getJsonKeys(str.toStringUtf8(),qualified));
    }

    @ScalarFunction("get_json_values")
    @Description("get all json values in all levels ")
    @SqlType("array(varchar)")
    @SqlNullable
    public static Block getJsonValues(
            @SqlType(StandardTypes.VARCHAR) Slice str)
    {
        return stringArrayBlock(getJsonValues(str.toStringUtf8()));
    }

    public static List<String> getJsonKeys(String str,boolean qualifiedName) {
        HashSet<String> result = new HashSet<String>();
        try {
            boolean inKey=false;
            JsonParser parser = Json.createParser(new StringReader(str));
            ArrayList<String> key = new ArrayList<String>();
            while (parser.hasNext()) {
                JsonParser.Event event = parser.next();
                switch (event) {
                    case KEY_NAME:
                        inKey = true;
                        if (!qualifiedName) key.clear();
                        key.add(parser.getString());
                        break;
                    case START_ARRAY:
                        inKey = false;
                        if (qualifiedName && !key.isEmpty())
                            key.set(key.size() - 1, key.get(key.size() - 1) + "*");
                        break;
                    case END_ARRAY:
                        if (!key.isEmpty() && key.get(key.size() - 1).endsWith("*"))
                            key.remove(key.size() - 1);
                        break;
                    case END_OBJECT:
                        if (!key.isEmpty() && !key.get(key.size() - 1).endsWith("*"))
                            key.remove(key.size() - 1);
                        break;
                    case VALUE_STRING:
                    case VALUE_NUMBER:
                    case VALUE_TRUE:
                    case VALUE_FALSE:
                    case VALUE_NULL:
                        if (inKey) {
                            result.add(String.join(":", key));
                            key.remove(key.size() - 1);
                            inKey = false;
                        }
                        else {
                            if (!key.isEmpty() && key.get(key.size() - 1).endsWith("*"))
                                result.add(String.join(":", key));
                        }
                        break;
                }
            }
        }
        catch (java.lang.Exception e) {
//            result.add("guyco");
            throw  e;
        }
        return result.stream().sorted().collect(Collectors.toList());
    }

    public static List<String> getJsonValues(String str) {
        JsonParser parser = Json.createParser(new StringReader(str));
        HashSet<String> result = new HashSet<String>();
        while (parser.hasNext()) {
            JsonParser.Event event = parser.next();
            switch (event) {
                case VALUE_STRING: case VALUE_NUMBER:
                    result.add(parser.getString());
                    break;
            }
        }
        return result.stream().sorted().collect(Collectors.toList());
    }

    public static Block stringArrayBlock(List<String> list) {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, list.size());
        for (String str: list)
            VARCHAR.writeSlice(blockBuilder, utf8Slice(str));
        return blockBuilder.build();

    }
}
